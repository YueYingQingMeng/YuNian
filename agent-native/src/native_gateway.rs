// native_gateway.rs — 原生 HTTP 网关（流式输出完全下沉 rs）
//
// 职责（替代 Kotlin AgentModelGateway + AiService 的 HTTP 层）：
// - SQLite 直读：api_configs（active config）/ companions（人设画像）
// - System Prompt 由 PromptOrchestrator 统一组装（L0-L5 分层注入），本模块只做传递
// - HTTP 请求：OpenAI 兼容 chat/completions（非流式 + SSE 流式）+ Anthropic /v1/messages
// - SSE 解析：data: 行 → delta 增量 → 经 StreamSink 实时回调 Kotlin
//
// 决策边界：
// - 决策 100% 在 Rust；Kotlin 仅传 AgentGlobalConfig（db path / 设备 id / 设置 / 表情包）
//   并接收 StreamSink 增量事件 + AgentEvent 落地。
// - 历史消息仍由 Kotlin 传入（history_json，已含 AiDialogueHistoryPolicy 清洗）；
//   system prompt 由 AgentRuntime 每轮经 PromptOrchestrator 组装后写入 request.system_prompt，
//   本模块发送前统一替换 messages 中的 persona system（废除旧 has_persona_system 冻结逻辑）。
// - 工具副作用（ToolHost）保留 Kotlin 实现（不回沉）。

use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};

use crate::agent::{AgentTurnRequest, StreamSink};
use crate::prompt_orchestrator::{CompanionProfile, PromptOrchestrator};
use crate::retry::{self, AttemptFailure, RetryPolicy};

/// Room schema（AppDatabase @Database version）直读支持范围。
/// ⚠️ Room 迁移版本号变化时必须同步更新，否则 Agent 直读查询会静默失败。
///
/// 本地（予念/yunian）当前 Room 版本：44 基线 + Agent 迁移一次性建 10 张表 → **45**。
/// 注意：本地 v44 与上游 master 的 v44 并非同一 schema（本地缺 master 的 v38–v48 血统），
/// 故此处上限取**本地的实际终态版本 45**，而非 master 的 48。
const MIN_SUPPORTED_SCHEMA: i64 = 41;
const MAX_SUPPORTED_SCHEMA: i64 = 45;

/// 全局运行配置（AgentRuntime::new 时一次传入，不随调用变化）
///
/// - db_path / device_id：App 级固定；
/// - settings_json / stickers / credentials_json：全局可变，通过
///   AgentRuntime::update_settings / update_stickers / update_credentials 热更新。
#[derive(Clone, uniffi::Record)]
pub struct AgentGlobalConfig {
    /// Room 数据库文件路径（context.getDatabasePath("yunian_database")，无 .db 后缀）
    pub db_path: String,
    /// 设备 ID（memory_entries.deviceId 过滤）
    pub device_id: String,
    /// Kotlin 设置 JSON：{role, reasoning_field, yandere, ...}（inner_thought/ntp_time 已废弃移除）
    pub settings_json: String,
    /// 可用表情包标签列表（Kotlin StickerManager 产物，Top-30；
    /// builtin_send_sticker 校验用 + 无匹配报告展示）
    pub stickers: Vec<String>,
    /// API 凭证覆盖 JSON（PARTNER 的 X-LianYu-Session / X-LianYu-Client-Id 等）
    pub credentials_json: String,
    /// 提示词编排器（Kotlin 注入；None 时 system prompt 完全由 Kotlin 提供）
    pub orchestrator: Option<Arc<PromptOrchestrator>>,
}

impl AgentGlobalConfig {
    fn settings(&self) -> Value {
        serde_json::from_str(&self.settings_json).unwrap_or(json!({}))
    }

    fn credentials(&self) -> Value {
        serde_json::from_str(&self.credentials_json).unwrap_or(json!({}))
    }
}

/// API 配置行（api_configs 表）
struct ApiConfigRow {
    provider: String,
    api_key: String,
    extra_api_keys: String,
    base_url: String,
    model: String,
    temperature: f64,
    max_tokens: Option<i64>,
    format_hint: String,
}

/// 伴侣行（companions 表）
struct CompanionRow {
    name: String,
    age: Option<i64>,
    personality: String,
    backstory: Option<String>,
    speaking_style: Option<String>,
    raw_prompt: Option<String>,
    system_prompt: Option<String>,
}

impl CompanionRow {
    /// 转为编排器身份画像（L1 数据来源；role 来自 settings_json）
    fn to_profile(&self, role: &str) -> CompanionProfile {
        CompanionProfile {
            name: self.name.clone(),
            age: self.age,
            personality: self.personality.clone(),
            speaking_style: self.speaking_style.clone(),
            backstory: self.backstory.clone(),
            raw_prompt: self.raw_prompt.clone(),
            custom_prompt: self.system_prompt.clone(),
            role: role.to_string(),
        }
    }
}

/// 原生 HTTP 网关（每回合构造一次；SQLite 只读连接懒加载并回合内复用）
pub struct NativeGateway {
    cfg: AgentGlobalConfig,
    /// HTTP 传输（默认 ureq；测试注入 mock）
    transport: Arc<dyn HttpTransport>,
    /// suflow.cloud 设备签名回调（PARTNER 通道；Kotlin 实现，私钥不出 Android Keystore）
    signer: Option<Arc<dyn crate::agent::RequestSignatureProvider>>,
    /// 懒加载的只读连接（回合级复用：一次 runTurn 内多次查询共用一条连接）
    db: Mutex<Option<Connection>>,
    /// 首次打开时读取的 SQLite user_version（Room 版本），0 = 未读取
    schema_version: AtomicI64,
    /// LLM 调用自动重试策略（瞬时故障退避重试；默认见 `RetryPolicy::default`）
    retry_policy: RetryPolicy,
    /// 退避睡眠实现（生产 = `std::thread::sleep`；测试注入零等待，避免真实等待）
    sleeper: Arc<dyn Fn(Duration) + Send + Sync>,
    /// 回合取消标志（`Some` 时重试循环在每次尝试前/退避后可被中断；由 AgentRuntime 注入）
    cancel: Option<Arc<AtomicBool>>,
}

/// HTTP 传输抽象（内部 trait，非 uniffi 面）：便于单测注入 mock。
/// stream=true 时要求增量经 sink 实时回调，返回与 send 同构的完整响应 JSON。
pub(crate) trait HttpTransport: Send + Sync {
    fn post_json(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &str,
        stream: bool,
        sink: Option<&dyn StreamSink>,
    ) -> Result<String, String>;

    /// GET 请求（模型列表 / 余额查询等只读探测）
    fn get_json(&self, url: &str, headers: &[(String, String)]) -> Result<String, String>;
}

/// 默认传输：ureq 阻塞 HTTP（rustls TLS）
pub(crate) struct UreqTransport;

impl HttpTransport for UreqTransport {
    fn post_json(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &str,
        stream: bool,
        sink: Option<&dyn StreamSink>,
    ) -> Result<String, String> {
        if !stream {
            return ureq_post_once(url, headers, body);
        }
        ureq_post_stream(url, headers, body, sink)
    }

    fn get_json(&self, url: &str, headers: &[(String, String)]) -> Result<String, String> {
        ureq_get_once(url, headers)
    }
}

impl NativeGateway {
    pub fn new(cfg: AgentGlobalConfig) -> Self {
        Self::with_signer(cfg, None)
    }

    /// 带设备签名回调构造（PARTNER 通道；None = 不签名，PARTNER 请求 fail-closed）
    pub(crate) fn with_signer(
        cfg: AgentGlobalConfig,
        signer: Option<Arc<dyn crate::agent::RequestSignatureProvider>>,
    ) -> Self {
        NativeGateway {
            cfg,
            transport: Arc::new(UreqTransport),
            signer,
            db: Mutex::new(None),
            schema_version: AtomicI64::new(0),
            retry_policy: RetryPolicy::default(),
            sleeper: Arc::new(|d| std::thread::sleep(d)),
            cancel: None,
        }
    }

    /// 注入自定义传输（测试与 Eval 脚本化传输共用）
    pub(crate) fn with_transport(cfg: AgentGlobalConfig, transport: Arc<dyn HttpTransport>) -> Self {
        Self::with_transport_and_signer(cfg, transport, None)
    }

    /// 注入自定义传输 + 签名回调（Eval mock 与 PARTNER 签名可共存）
    pub(crate) fn with_transport_and_signer(
        cfg: AgentGlobalConfig,
        transport: Arc<dyn HttpTransport>,
        signer: Option<Arc<dyn crate::agent::RequestSignatureProvider>>,
    ) -> Self {
        NativeGateway {
            cfg,
            transport,
            signer,
            db: Mutex::new(None),
            schema_version: AtomicI64::new(0),
            retry_policy: RetryPolicy::default(),
            sleeper: Arc::new(|d| std::thread::sleep(d)),
            cancel: None,
        }
    }

    // ── 重试 / 取消注入（生产走默认；测试注入零等待睡眠与取消标志） ──

    /// 注入重试策略与退避睡眠实现（测试专用；生产用默认）。
    pub(crate) fn with_retry(
        mut self,
        policy: RetryPolicy,
        sleeper: Arc<dyn Fn(Duration) + Send + Sync>,
    ) -> Self {
        self.retry_policy = policy;
        self.sleeper = sleeper;
        self
    }

    /// 注入回合取消标志：重试循环在每次尝试前 / 退避等待后检查该标志，
    /// 一旦置位立即停止重试（避免在 WorkManager 中断 / 用户停止生成后继续重试）。
    pub(crate) fn with_cancel(mut self, cancel: Arc<AtomicBool>) -> Self {
        self.cancel = Some(cancel);
        self
    }

    // ── SQLite 直读 ──
    //
    // 连接策略（P1-3）：NativeGateway 每次 runTurn 构造一次，内部**懒加载并复用**
    // 一条只读连接（回合内多次查询共用），避免每查询一次 open 的固定开销；
    // 只读 + NO_MUTEX，与 Room（WAL 写）同进程并发安全。

    /// 打开（或复用）只读连接；首次打开时读取 schema user_version 并校验支持范围
    fn open_db(&self) -> Result<std::sync::MutexGuard<'_, Option<Connection>>, String> {
        let mut guard = self
            .db
            .lock()
            .map_err(|_| "数据库连接锁失效".to_string())?;
        if guard.is_none() {
            let conn = Connection::open_with_flags(
                &self.cfg.db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|e| format!("打开数据库失败: {e}"))?;
            if let Ok(v) = conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)) {
                self.schema_version.store(v, Ordering::Relaxed);
                if v < MIN_SUPPORTED_SCHEMA || v > MAX_SUPPORTED_SCHEMA {
                    crate::agentlog::warn(
                        "native_gateway",
                        &format!(
                            "Room schema user_version={v} 超出 Agent 直读支持范围 [{MIN_SUPPORTED_SCHEMA}..={MAX_SUPPORTED_SCHEMA}]——Room 迁移后必须同步 native_gateway.rs 的 MIN/MAX_SUPPORTED_SCHEMA"
                        ),
                    );
                }
            }
            *guard = Some(conn);
        }
        Ok(guard)
    }

    /// 在复用的只读连接上执行查询
    fn with_db<T>(&self, f: impl FnOnce(&Connection) -> Result<T, String>) -> Result<T, String> {
        let guard = self.open_db()?;
        let conn = guard
            .as_ref()
            .ok_or_else(|| "数据库连接未初始化".to_string())?;
        f(conn)
    }

    /// 查询失败时附带 schema 版本，便于定位 Room 迁移导致的不兼容
    fn db_error(&self, context: &str, e: &dyn std::fmt::Display) -> String {
        format!("{context}: {e} (schema user_version={})", self.schema_version.load(Ordering::Relaxed))
    }

    /// 读取激活的 API 配置（isEnabled=1，优先最新）
    fn load_api_config(&self) -> Result<ApiConfigRow, String> {
        self.with_db(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT provider, apiKey, extraApiKeys, baseUrl, model, temperature, maxTokens, formatHint \
                     FROM api_configs WHERE isEnabled = 1 ORDER BY id DESC LIMIT 1",
                )
                .map_err(|e| self.db_error("查询 api_configs 失败", &e))?;
            let row = stmt
                .query_row([], |r| {
                    Ok(ApiConfigRow {
                        provider: r.get(0)?,
                        api_key: r.get(1)?,
                        extra_api_keys: r.get(2)?,
                        base_url: r.get(3)?,
                        model: r.get(4)?,
                        temperature: r.get::<_, f64>(5)?,
                        max_tokens: r.get(6)?,
                        format_hint: r.get::<_, Option<String>>(7)?.unwrap_or_default(),
                    })
                })
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => "无可用 API 配置".to_string(),
                    _ => self.db_error("读取 api_configs 失败", &e),
                })?;
            Ok(row)
        })
    }

    /// 读取伴侣人设
    fn load_companion(&self, companion_id: Option<i64>) -> Result<CompanionRow, String> {
        let id = companion_id.ok_or_else(|| "未指定伴侣 ID（群聊场景应提供 system_prompt 覆盖）".to_string())?;
        self.with_db(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT name, age, personality, backstory, speakingStyle, rawPrompt, systemPrompt \
                     FROM companions WHERE id = ?1",
                )
                .map_err(|e| self.db_error("查询 companions 失败", &e))?;
            let row = stmt
                .query_row([id], |r| {
                    Ok(CompanionRow {
                        name: r.get(0)?,
                        age: r.get(1)?,
                        personality: r.get(2)?,
                        backstory: r.get(3)?,
                        speaking_style: r.get(4)?,
                        raw_prompt: r.get(5)?,
                        system_prompt: r.get(6)?,
                    })
                })
                .map_err(|e| self.db_error(&format!("伴侣信息加载失败 (id={id})"), &e))?;
            Ok(row)
        })
    }

    /// 读取伴侣人设画像（编排器 L1 数据来源；role 取自 settings_json）
    pub(crate) fn load_companion_profile(&self, companion_id: Option<i64>) -> Result<CompanionProfile, String> {
        let row = self.load_companion(companion_id)?;
        let role = self
            .cfg
            .settings()
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("GIRLFRIEND")
            .to_string();
        Ok(row.to_profile(&role))
    }

    // ── 凭证解析 ──

    fn all_api_keys(&self, cfg: &ApiConfigRow) -> Vec<String> {
        // 优先使用 Kotlin 解密后经 credentials 传入的明文 key
        // （SQLite 中 apiKey 为 Tink 加密串 enc:v4:...，Rust 无法解密）
        let cred = self.cfg.credentials();
        let mut keys = Vec::new();
        if let Some(main) = cred.get("api_key").and_then(|v| v.as_str()) {
            let main = main.trim();
            if !main.is_empty() {
                keys.push(main.to_string());
            }
        }
        if let Some(extra) = cred.get("extra_api_keys").and_then(|v| v.as_str()) {
            if !extra.is_blank_trim() {
                keys.extend(
                    extra
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty()),
                );
            }
        }
        if !keys.is_empty() {
            return keys;
        }
        let main = cfg.api_key.trim().to_string();
        if !main.is_empty() {
            keys.push(main);
        }
        if !cfg.extra_api_keys.is_blank_trim() {
            keys.extend(
                cfg.extra_api_keys
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
            );
        }
        keys
    }

    /// 解析本轮请求可用的 API Key 列表。
    /// PARTNER 模式通过 credentials（X-LianYu-Session / X-LianYu-Client-Id）认证，
    /// 不依赖 API Key；apiKey 为空时返回占位 key，provider_headers 的 PARTNER 分支不消费它。
    fn resolve_keys(&self, cfg: &ApiConfigRow) -> Result<Vec<String>, String> {
        let keys = self.all_api_keys(cfg);
        if !keys.is_empty() {
            return Ok(keys);
        }
        if cfg.provider == "PARTNER" {
            return Ok(vec![String::new()]);
        }
        Err("没有可用的 API Key".to_string())
    }

    fn provider_headers(&self, cfg: &ApiConfigRow, key: &str) -> Vec<(String, String)> {
        let mut headers = Vec::new();
        headers.push(("Content-Type".to_string(), "application/json".to_string()));
        if cfg.provider == "PARTNER" {
            let cred = self.cfg.credentials();
            let session = cred
                .get("session")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let client_id = cred
                .get("client_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            headers.push(("X-LianYu-Session".to_string(), session));
            headers.push(("X-LianYu-Client-Id".to_string(), client_id));
        } else if cfg.provider == "XIAOMI" {
            headers.push(("api-key".to_string(), key.to_string()));
        } else {
            headers.push(("Authorization".to_string(), format!("Bearer {key}")));
        }
        headers
    }

    /// 是否 Anthropic 协议（formatHint == "anthropic" 或 provider == ANTHROPIC）
    fn is_anthropic(&self, cfg: &ApiConfigRow) -> bool {
        cfg.format_hint.eq_ignore_ascii_case("anthropic")
            || cfg.provider.eq_ignore_ascii_case("ANTHROPIC")
    }

    fn uses_max_completion_tokens(&self, cfg: &ApiConfigRow) -> bool {
        cfg.provider.eq_ignore_ascii_case("XIAOMI")
    }

    // ── 请求构建 ──

    /// 组装 LLM 请求体（OpenAI 兼容）。messages 首条为 system。
    fn build_openai_body(
        &self,
        cfg: &ApiConfigRow,
        messages: &[Value],
        tools: &Value,
        tool_choice: &str,
        request: &AgentTurnRequest,
        stream: bool,
    ) -> Value {
        // 视觉输入：把最后一条 user 消息的 content 换成 [text, image_url] 数组
        let mut effective_msgs: Vec<Value> = messages.to_vec();
        if let Some(img) = &request.image {
            let mut content: Vec<Value> = Vec::new();
            if let Some(text) = effective_msgs
                .iter()
                .rev()
                .find(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
            {
                content.push(json!({"type": "text", "text": text}));
            }
            if let Some(path) = &img.path {
                match read_image_as_base64(path, &img.mime_type) {
                    Ok((mime, b64)) => {
                        content.push(json!({
                            "type": "image_url",
                            "image_url": {"url": format!("data:{};base64,{}", mime, b64)}
                        }));
                    }
                    Err(e) => {
                        content.push(json!({"type": "text", "text": format!("[图片加载失败: {e}]")}));
                    }
                }
            } else if let Some(b64) = &img.base64_data {
                let mime = img.mime_type.as_deref().unwrap_or("image/jpeg");
                content.push(json!({
                    "type": "image_url",
                    "image_url": {"url": format!("data:{};base64,{}", mime, b64)}
                }));
            }
            if let Some(last) = effective_msgs
                .iter_mut()
                .rev()
                .find(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
            {
                last["content"] = json!(content);
            }
        }
        let mut body = json!({
            "model": cfg.model,
            "messages": effective_msgs,
            "stream": stream,
        });
        // temperature：2 位小数归一 + 固定温度模型跳过
        //
        // 1) 归一：Room 把 Kotlin Float 写成 SQLite REAL 时按 Float→Double 加宽
        //    （0.7f → 0.699999988079071），智谱清言等「限制小数点 2 位」的网关会直接 400
        //    拒绝整个请求（用户实测报障）。详见 temperature.rs。
        // 2) 固定温度模型（kimi-k2.6 / k2.6）：服务端不接受自定义 temperature，传入即报错。
        //    此前**只有连接测试**做了该判定（api_probe::api_requires_fixed_temperature），
        //    生成路径漏判 → 测试通过但每次聊天必失败。语义与 Kotlin
        //    AiService.requiresFixedTemperature 完全一致。
        if !crate::api_probe::api_requires_fixed_temperature(cfg.model.clone()) {
            let safe_temp = crate::temperature::normalize_temperature(cfg.temperature);
            body["temperature"] = json!(safe_temp);
        }
        let max_tokens = cfg.max_tokens.unwrap_or(800);
        if max_tokens > 0 {
            let param = if self.uses_max_completion_tokens(cfg) {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
            body[param] = json!(max_tokens);
        }
        // 工具
        if let Some(tools_arr) = tools.as_array() {
            if !tools_arr.is_empty() {
                body["tools"] = tools.clone();
                let tc = if tool_choice.is_empty() { "auto" } else { tool_choice };
                if tc.starts_with('{') {
                    body["tool_choice"] = serde_json::from_str::<Value>(tc).unwrap_or(json!("auto"));
                } else {
                    body["tool_choice"] = json!(tc.trim_matches('"'));
                }
            }
        }
        body
    }

    /// 组装 Anthropic 请求体。messages 不含 system（system 单独字段）。
    ///
    /// 与 OpenAI 兼容路径的能力对齐（此前完全缺失，导致 Anthropic 通道下所有工具不可用）：
    /// - assistant 的 tool_calls → tool_use 内容块（回灌必需，否则下轮 tool_result 无法匹配）；
    /// - tool 角色消息 → tool_result 内容块，连续结果合并进同一条 user 消息；
    /// - tools / tool_choice（OpenAI function 结构）→ Anthropic tools（input_schema）+ tool_choice；
    /// - 运行时注入的 system 片段（世界书 at_depth/bottom、[回合状态]、表情包熔断）折进 system 字段，
    ///   不再因 Anthropic 禁止 messages 内 system 角色而被整体丢弃。
    fn build_anthropic_body(
        &self,
        cfg: &ApiConfigRow,
        messages: &[Value],
        system_prompt: &str,
        request: &AgentTurnRequest,
        tools: &Value,
        tool_choice: &str,
    ) -> Value {
        let mut anthro_messages: Vec<Value> = Vec::new();
        let mut runtime_system: Vec<String> = Vec::new();
        for m in messages {
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");
            let content = m.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string();
            match role {
                // 旧 persona system 每轮由 request.system_prompt 重建 → 丢弃；
                // 带内部保留标记或 [回合状态 的运行时注入 → 折进 system 字段保留语义。
                "system" => {
                    let preserved = m
                        .get("_agent_preserve_system")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                        || content.starts_with("[回合状态");
                    if preserved && !content.is_empty() {
                        runtime_system.push(content);
                    }
                }
                "tool" => {
                    let call_id = m
                        .get("tool_call_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if call_id.is_empty() {
                        // 无 tool_use_id 无法构成合法 tool_result，降级为文本块
                        anthro_messages.push(json!({
                            "role": "user",
                            "content": [{"type": "text", "text": format!("[工具结果] {content}")}],
                        }));
                    } else {
                        let block = json!({
                            "type": "tool_result",
                            "tool_use_id": call_id,
                            "content": content,
                        });
                        // tool_result 必须紧跟对应 tool_use；连续多个结果合并进同一条 user 消息
                        let merged = match anthro_messages.last_mut() {
                            Some(last)
                                if last.get("role").and_then(|v| v.as_str()) == Some("user")
                                    && last.get("content").map(|c| c.is_array()).unwrap_or(false) =>
                            {
                                let only_results = last
                                    .get("content")
                                    .and_then(|c| c.as_array())
                                    .map(|blocks| {
                                        blocks.iter().all(|b| {
                                            b.get("type").and_then(|t| t.as_str())
                                                == Some("tool_result")
                                        })
                                    })
                                    .unwrap_or(false);
                                if only_results {
                                    match last.get_mut("content").and_then(|c| c.as_array_mut()) {
                                        Some(blocks) => {
                                            blocks.push(block.clone());
                                            true
                                        }
                                        None => false,
                                    }
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        };
                        if !merged {
                            anthro_messages.push(json!({"role": "user", "content": [block]}));
                        }
                    }
                }
                _ => {
                    let tool_calls = m
                        .get("tool_calls")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    if role == "assistant" && !tool_calls.is_empty() {
                        let mut blocks: Vec<Value> = Vec::new();
                        if !content.is_empty() {
                            blocks.push(json!({"type": "text", "text": content}));
                        }
                        for tc in &tool_calls {
                            let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                            let name = tc
                                .get("name")
                                .and_then(|v| v.as_str())
                                .or_else(|| {
                                    tc.get("function")
                                        .and_then(|f| f.get("name"))
                                        .and_then(|v| v.as_str())
                                })
                                .unwrap_or("");
                            let args = tc
                                .get("arguments")
                                .and_then(|v| v.as_str())
                                .or_else(|| {
                                    tc.get("function")
                                        .and_then(|f| f.get("arguments"))
                                        .and_then(|v| v.as_str())
                                })
                                .unwrap_or("{}");
                            let input: Value = serde_json::from_str(args).unwrap_or(json!({}));
                            blocks.push(json!({
                                "type": "tool_use",
                                "id": id,
                                "name": name,
                                "input": input,
                            }));
                        }
                        anthro_messages.push(json!({"role": "assistant", "content": blocks}));
                    } else {
                        let role_out = if role == "user" { "user" } else { "assistant" };
                        anthro_messages.push(json!({"role": role_out, "content": content}));
                    }
                }
            }
        }
        // 视觉输入（Anthropic image block）
        if let Some(img) = &request.image {
            if let Some(last) = anthro_messages.last_mut() {
                let mut blocks: Vec<Value> = Vec::new();
                if let Some(text) = last.get("content").and_then(|c| c.as_str()) {
                    if !text.is_empty() {
                        blocks.push(json!({"type": "text", "text": text}));
                    }
                }
                if let Some(path) = &img.path {
                    match read_image_as_base64(path, &img.mime_type) {
                        Ok((mime, b64)) => {
                            blocks.push(json!({
                                "type": "image",
                                "source": {"type": "base64", "media_type": mime, "data": b64}
                            }));
                        }
                        Err(e) => {
                            blocks.push(json!({"type": "text", "text": format!("[图片加载失败: {e}]")}));
                        }
                    }
                } else if let Some(b64) = &img.base64_data {
                    let mime = img.mime_type.as_deref().unwrap_or("image/jpeg");
                    blocks.push(json!({
                        "type": "image",
                        "source": {"type": "base64", "media_type": mime, "data": b64}
                    }));
                }
                last["content"] = json!(blocks);
            }
        }
        let system_text = if runtime_system.is_empty() {
            system_prompt.to_string()
        } else {
            format!("{}\n\n{}", system_prompt.trim_end(), runtime_system.join("\n\n"))
        };
        let mut body = json!({
            "model": cfg.model,
            "system": system_text,
            "messages": anthro_messages,
            "max_tokens": cfg.max_tokens.unwrap_or(800),
            // 同 OpenAI 兼容路径：temperature 必须 2 位小数（见 temperature.rs）
            "temperature": crate::temperature::normalize_temperature(cfg.temperature),
        });
        // 工具：OpenAI function 结构 → Anthropic tools（input_schema）。
        // tool_choice="none"（工作流/单轮生成场景）表示本轮不暴露工具，直接省略 tools。
        let choice_norm = tool_choice.trim().trim_matches('"');
        let wants_tools = tools.as_array().map(|arr| !arr.is_empty()).unwrap_or(false)
            && choice_norm != "none";
        if wants_tools {
            let anthro_tools: Vec<Value> = tools
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .map(|t| {
                            let f = t.get("function").unwrap_or(t);
                            let mut tool = serde_json::Map::new();
                            tool.insert("name".to_string(), f.get("name").cloned().unwrap_or(json!("")));
                            if let Some(desc) = f.get("description").and_then(|d| d.as_str()) {
                                if !desc.trim().is_empty() {
                                    tool.insert("description".to_string(), json!(desc));
                                }
                            }
                            tool.insert(
                                "input_schema".to_string(),
                                f.get("parameters").cloned().unwrap_or(json!({"type": "object"})),
                            );
                            Value::Object(tool)
                        })
                        .collect()
                })
                .unwrap_or_default();
            body["tools"] = json!(anthro_tools);
            body["tool_choice"] = if tool_choice.trim().starts_with('{') {
                serde_json::from_str::<Value>(tool_choice.trim()).unwrap_or(json!({"type": "auto"}))
            } else {
                match choice_norm {
                    "required" | "any" => json!({"type": "any"}),
                    _ => json!({"type": "auto"}),
                }
            };
        }
        body
    }

    /// 解析 system prompt：新注入架构下由 AgentRuntime 每轮经 PromptOrchestrator
    /// 组装后写入 request.system_prompt（单聊 L0-L5 / 群聊覆盖），此处直接透传。
    /// 群聊成员名字映射已并入编排器 L0 环境层。
    fn resolve_system_prompt(&self, request: &AgentTurnRequest) -> String {
        request.system_prompt.clone().unwrap_or_default()
    }

    // ── 非流式发送 ──

    /// 发送一轮请求并返回与旧契约同构的响应 JSON：
    /// {content, tool_calls:[{id,name,arguments}], finish_reason}
    ///
    /// 重试：每个 API Key 的 HTTP 调用经 [`retry::run_with_retry`] 包裹——
    /// 瞬时故障（连接/超时/408/429/5xx）按策略退避重试；客户端错误（4xx）立即失败。
    /// 单个 Key 重试耗尽后沿用既有「故障转移到下一个 Key」语义；最终失败保留原始错误。
    pub fn send(
        &self,
        request: &AgentTurnRequest,
        _companion_id: Option<i64>,
        messages: &[Value],
        tools: &Value,
        tool_choice: &str,
    ) -> Result<String, String> {
        let cfg = self.load_api_config()?;
        if cfg.model.trim().is_empty() {
            return Err("模型名未配置，请在「API设置」中重新测试连接".to_string());
        }
        let keys = self.resolve_keys(&cfg)?;
        let anthropic = self.is_anthropic(&cfg);

        // 每轮重建请求体（与 Key 无关，循环外只构造一次）
        let system_prompt = self.resolve_system_prompt(request);
        let (path, body) = if anthropic {
            let body =
                self.build_anthropic_body(&cfg, messages, &system_prompt, request, tools, tool_choice);
            ("/messages", body)
        } else {
            let mut msgs = messages.to_vec();
            replace_persona_system(&mut msgs, &system_prompt);
            let body = self.build_openai_body(&cfg, &msgs, tools, tool_choice, request, false);
            ("/chat/completions", body)
        };

        let mut last_err: Option<String> = None;
        for key in &keys {
            let attempt = || match self.http_post_json(&cfg, key, path, &body, anthropic) {
                Ok(resp) => {
                    // HTTP 2xx 但响应体含 error 字段（部分厂商）→ 视为该 Key 失败（不重试，换 Key）
                    match extract_error_message(&resp) {
                        Some(msg) => Err(AttemptFailure::fatal(format!("API 返回错误: {msg}"))),
                        None => Ok(resp),
                    }
                }
                Err(e) => {
                    let (retryable, retry_after) = retry::classify_error(&e);
                    Err(AttemptFailure { message: e, retryable, retry_after })
                }
            };
            match retry::run_with_retry(
                self.retry_policy,
                self.cancel.as_deref(),
                "send",
                attempt,
                |d| (self.sleeper)(d),
            ) {
                Ok(resp) => {
                    return Ok(if anthropic {
                        parse_anthropic_response(&resp)
                    } else {
                        parse_openai_response(&resp)
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| "所有 API Key 均请求失败".to_string()))
    }

    // ── 流式发送（SSE） ──

    /// 流式发送：OpenAI 兼容 SSE。增量经 sink 实时回调；返回与 send 同构的完整响应 JSON。
    ///
    /// 重试：与 [`Self::send`] 同策略，但**已向 sink 交付过任何增量后不再重试**
    /// （否则会重复打字/重复气泡）——此时立即失败，也不再切换 API Key。
    pub fn send_stream(
        &self,
        request: &AgentTurnRequest,
        companion_id: Option<i64>,
        messages: &[Value],
        tools: &Value,
        tool_choice: &str,
        sink: &dyn StreamSink,
    ) -> Result<String, String> {
        let cfg = self.load_api_config()?;
        if cfg.model.trim().is_empty() {
            return Err("模型名未配置，请在「API设置」中重新测试连接".to_string());
        }
        if self.is_anthropic(&cfg) {
            // Anthropic 降级为非流式终态（协议差异大，流式后续迭代）
            let result = self.send(request, companion_id, messages, tools, tool_choice)?;
            let parsed: Value = serde_json::from_str(&result).unwrap_or(json!({}));
            let content = parsed.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let finish = parsed.get("finish_reason").and_then(|v| v.as_str()).unwrap_or("stop").to_string();
            if !content.is_empty() {
                sink.on_text_delta(content.clone());
            }
            sink.on_done(content.clone(), finish.clone());
            return Ok(result);
        }
        let keys = self.resolve_keys(&cfg)?;

        let system_prompt = self.resolve_system_prompt(request);
        let mut msgs = messages.to_vec();
        replace_persona_system(&mut msgs, &system_prompt);
        let body = self.build_openai_body(&cfg, &msgs, tools, tool_choice, request, true);

        let mut last_err: Option<String> = None;
        for key in &keys {
            // 交付守卫：一旦本 Key 尝试中已向 sink 交付增量，则不得重试/换 Key
            let delivered = AtomicBool::new(false);
            let guarded = DeliveryGuardedSink { inner: sink, delivered: &delivered };
            let attempt = || match self.http_post_stream(&cfg, key, &body, &guarded) {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    let (mut retryable, retry_after) = retry::classify_error(&e);
                    if delivered.load(Ordering::SeqCst) {
                        retryable = false;
                    }
                    Err(AttemptFailure { message: e, retryable, retry_after })
                }
            };
            match retry::run_with_retry(
                self.retry_policy,
                self.cancel.as_deref(),
                "send_stream",
                attempt,
                |d| (self.sleeper)(d),
            ) {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    // 已交付增量 → 不能换 Key 重放（重复输出）
                    if delivered.load(Ordering::SeqCst) {
                        return Err(e);
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| "所有 API Key 均请求失败".to_string()))
    }

    // ── HTTP 底层（传输委托） ──

    fn base_url(&self, cfg: &ApiConfigRow) -> String {
        cfg.base_url.trim().trim_end_matches('/').to_string()
    }

    fn http_post_json(
        &self,
        cfg: &ApiConfigRow,
        key: &str,
        path: &str,
        body: &Value,
        anthropic: bool,
    ) -> Result<String, String> {
        let url = format!("{}{}", self.base_url(cfg), path);
        let mut headers = self.provider_headers(cfg, key);
        if anthropic {
            headers.push(("x-api-key".to_string(), key.to_string()));
            headers.push(("anthropic-version".to_string(), "2023-06-01".to_string()));
        }
        self.inject_partner_signature(cfg, "POST", path, body, &mut headers)?;
        self.transport
            .post_json(&url, &headers, &body.to_string(), false, None)
    }

    /// 流式 POST：SSE 解析在传输层，增量经 sink 回调，返回完整响应 JSON
    fn http_post_stream(
        &self,
        cfg: &ApiConfigRow,
        key: &str,
        body: &Value,
        sink: &dyn StreamSink,
    ) -> Result<String, String> {
        let url = format!("{}{}", self.base_url(cfg), "/chat/completions");
        let mut headers = self.provider_headers(cfg, key);
        headers.push(("Accept".to_string(), "text/event-stream".to_string()));
        self.inject_partner_signature(cfg, "POST", "/chat/completions", body, &mut headers)?;
        self.transport
            .post_json(&url, &headers, &body.to_string(), true, Some(sink))
    }

    /// PARTNER（suflow.cloud）设备签名注入：回调 Kotlin 签名器附加 7 个 X-LianYu-* 头。
    /// fail-closed：PARTNER 且已注入签名器但未产出 X-LianYu-Sig → 拒绝发送。
    fn inject_partner_signature(
        &self,
        cfg: &ApiConfigRow,
        method: &str,
        path: &str,
        body: &Value,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        if cfg.provider != "PARTNER" {
            return Ok(());
        }
        let Some(signer) = &self.signer else {
            return Err("PARTNER 请求需要设备签名（X-LianYu-Sig-Version），签名回调未注入".to_string());
        };
        let client_id = self
            .cfg
            .credentials()
            .get("client_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let signed = signer.sign_headers(
            method.to_string(),
            path.to_string(),
            body.to_string(),
            client_id,
        );
        let has_sig = signed.iter().any(|h| h.name == "X-LianYu-Sig");
        if !has_sig {
            return Err("设备签名不可用（X-LianYu-Sig 缺失），PARTNER 请求已拒绝".to_string());
        }
        for header in signed {
            headers.push((header.name, header.value));
        }
        Ok(())
    }
}

/// 交付守卫 Sink：包装真实 `StreamSink`，一旦有任一增量/完成回调发生即置位 `delivered`。
///
/// 用途：流式重试安全——只要已经向 UI 交付过字节，就不允许重试（否则重复打字/气泡）。
struct DeliveryGuardedSink<'a> {
    inner: &'a dyn StreamSink,
    delivered: &'a AtomicBool,
}

impl StreamSink for DeliveryGuardedSink<'_> {
    fn on_text_delta(&self, text: String) {
        self.delivered.store(true, Ordering::SeqCst);
        self.inner.on_text_delta(text);
    }

    fn on_reasoning_delta(&self, text: String) {
        self.delivered.store(true, Ordering::SeqCst);
        self.inner.on_reasoning_delta(text);
    }

    fn on_done(&self, full_text: String, finish_reason: String) {
        self.delivered.store(true, Ordering::SeqCst);
        self.inner.on_done(full_text, finish_reason);
    }

    fn on_error(&self, error: String) {
        // 错误回调不算「已交付有效增量」：允许重试（若此前没有任何 delta）
        self.inner.on_error(error);
    }
}

// ── ureq 传输实现 ──

/// 从错误响应提取 Retry-After（秒）并格式化为人类可读后缀。
///
/// 约定：仅整数秒（HTTP-date 形式忽略）。格式 `" (retry-after {secs}s)"`，
/// 由 [`crate::retry::classify_error`] 解析并作为退避等待（受单次上限约束）。
fn retry_after_suffix(resp: &ureq::Response) -> String {
    resp.header("Retry-After")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|secs| format!(" (retry-after {secs}s)"))
        .unwrap_or_default()
}

/// GET 请求：返回响应体文本（模型列表 / 余额查询等只读探测）
fn ureq_get_once(url: &str, headers: &[(String, String)]) -> Result<String, String> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(15))
        .timeout_read(std::time::Duration::from_secs(25))
        .build();
    let mut req = agent.get(url);
    for (k, v) in headers {
        req = req.set(k, v);
    }
    let resp = match req.call() {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let body = r.into_string().unwrap_or_default();
            let detail = extract_error_message(&body)
                .unwrap_or_else(|| body.chars().take(300).collect::<String>());
            return Err(format!("HTTP {code}: {detail}"));
        }
        Err(e) => return Err(format!("HTTP 请求失败: {e}")),
    };
    let status = resp.status();
    let text = resp
        .into_string()
        .map_err(|e| format!("读取响应失败: {e}"))?;
    if status >= 400 {
        let msg = extract_error_message(&text).unwrap_or_else(|| format!("HTTP {status}"));
        return Err(msg);
    }
    Ok(text)
}

/// 非流式 POST：返回响应体文本
fn ureq_post_once(
    url: &str,
    headers: &[(String, String)],
    body: &str,
) -> Result<String, String> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(15))
        .timeout_read(std::time::Duration::from_secs(120))
        .build();
    let mut req = agent.post(url);
    for (k, v) in headers {
        req = req.set(k, v);
    }
    // ureq 2.x 对非 2xx 返回 Err(Status)；捕获后读响应体，把服务端
    // error.message 透传给调用方（否则只报 "status code 400" 无法定位原因）。
    let resp = match req.send_string(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let suffix = retry_after_suffix(&r);
            let body = r.into_string().unwrap_or_default();
            let detail = extract_error_message(&body)
                .unwrap_or_else(|| body.chars().take(300).collect::<String>());
            return Err(format!("HTTP {code}: {detail}{suffix}"));
        }
        Err(e) => return Err(format!("HTTP 请求失败: {e}")),
    };
    let status = resp.status();
    let text = resp
        .into_string()
        .map_err(|e| format!("读取响应失败: {e}"))?;
    if status >= 400 {
        let msg = extract_error_message(&text).unwrap_or_else(|| format!("HTTP {status}"));
        return Err(msg);
    }
    if text.trim_start().starts_with('<') {
        return Err("服务器返回了网页而非API响应，请检查API密钥/地址是否正确".to_string());
    }
    Ok(text)
}

/// 流式 POST：逐行读 SSE 并回调 sink，最后返回完整响应 JSON
fn ureq_post_stream(
    url: &str,
    headers: &[(String, String)],
    body: &str,
    sink: Option<&dyn StreamSink>,
) -> Result<String, String> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(15))
        .timeout_read(std::time::Duration::from_secs(300))
        .build();
    let mut req = agent.post(url);
    for (k, v) in headers {
        req = req.set(k, v);
    }
    let resp = match req.send_string(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let suffix = retry_after_suffix(&r);
            let body = r.into_string().unwrap_or_default();
            let detail = extract_error_message(&body)
                .unwrap_or_else(|| body.chars().take(300).collect::<String>());
            return Err(format!("HTTP {code}: {detail}{suffix}"));
        }
        Err(e) => return Err(format!("HTTP 流式请求失败: {e}")),
    };
    let status = resp.status();
    if status >= 400 {
        let text = resp
            .into_string()
            .map_err(|e| format!("读取错误响应失败: {e}"))?;
        let msg = extract_error_message(&text).unwrap_or_else(|| format!("HTTP {status}"));
        return Err(msg);
    }
    let sink = sink.ok_or_else(|| "流式请求缺少 sink".to_string())?;

    let reader = BufReader::new(resp.into_reader());
    let mut full_text = String::new();
    let mut reasoning_content = String::new();
    let mut finish_reason = "stop".to_string();
    // 流式工具调用增量（OpenAI 兼容：delta.tool_calls[i] 的 name/arguments 分片到达）
    let mut tool_calls: Vec<StreamToolCallAcc> = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| format!("读取 SSE 行失败: {e}"))?;
        if handle_sse_line(
            &line,
            &mut full_text,
            &mut reasoning_content,
            &mut finish_reason,
            Some(sink),
            &mut tool_calls,
        )? {
            break;
        }
    }
    sink.on_done(full_text.clone(), finish_reason.clone());
    let tool_calls_json: Vec<Value> = tool_calls
        .iter()
        .map(|tc| {
            json!({
                "id": tc.id,
                "name": tc.name,
                "arguments": tc.arguments,
            })
        })
        .collect();
    Ok(json!({
        "content": full_text,
        "reasoning_content": reasoning_content,
        "tool_calls": tool_calls_json,
        "finish_reason": finish_reason,
    })
    .to_string())
}

/// 流式工具调用累加器（跨 SSE 分片聚合同一次 tool_call 的 name/arguments）
#[derive(Default)]
struct StreamToolCallAcc {
    id: String,
    name: String,
    arguments: String,
}

/// 处理一行 SSE 数据。
///
/// - `Ok(true)`：应终止流（遇到 `data: [DONE]`）；
/// - `Ok(false)`：继续读取下一行；
/// - `Err(msg)`：chunk 内含 error 字段（已回调 `sink.on_error`）。
///
/// 抽成纯函数以便直接单测流式解析（delta 增量 / reasoning / tool_calls /
/// finish_reason / error chunk / [DONE]），网络读取与解析分离。
fn handle_sse_line(
    line: &str,
    full_text: &mut String,
    reasoning_content: &mut String,
    finish_reason: &mut String,
    sink: Option<&dyn StreamSink>,
    tool_calls: &mut Vec<StreamToolCallAcc>,
) -> Result<bool, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }
    let Some(data) = trimmed.strip_prefix("data:") else {
        return Ok(false);
    };
    let data = data.trim();
    if data == "[DONE]" {
        return Ok(true);
    }
    let chunk: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(_) => return Ok(false),
    };
    if let Some(err) = chunk.get("error") {
        let msg = err
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("流式响应错误")
            .to_string();
        if let Some(sink) = sink {
            sink.on_error(msg.clone());
        }
        return Err(msg);
    }
    let choice = chunk
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first());
    if let Some(choice) = choice {
        if let Some(delta) = choice.get("delta") {
            if let Some(text) = delta.get("content").and_then(|v| v.as_str()) {
                if !text.is_empty() {
                    full_text.push_str(text);
                    if let Some(sink) = sink {
                        sink.on_text_delta(text.to_string());
                    }
                }
            }
            if let Some(reasoning) = delta.get("reasoning_content").and_then(|v| v.as_str()) {
                if !reasoning.is_empty() {
                    reasoning_content.push_str(reasoning);
                    if let Some(sink) = sink {
                        sink.on_reasoning_delta(reasoning.to_string());
                    }
                }
            }
            // 流式工具调用增量（OpenAI 兼容）：delta.tool_calls 数组，按 index 分片。
            // 每片可能含 function.name（首个分片）或 function.arguments 片段（后续分片）。
            if let Some(tcs) = delta.get("tool_calls").and_then(|v| v.as_array()) {
                for tc in tcs {
                    let index = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                    while tool_calls.len() <= index {
                        tool_calls.push(StreamToolCallAcc::default());
                    }
                    let acc = &mut tool_calls[index];
                    if let Some(id) = tc.get("id").and_then(|v| v.as_str()) {
                        if !id.is_empty() {
                            acc.id = id.to_string();
                        }
                    }
                    if let Some(f) = tc.get("function") {
                        if let Some(name) = f.get("name").and_then(|v| v.as_str()) {
                            if !name.is_empty() {
                                acc.name = name.to_string();
                            }
                        }
                        if let Some(args) = f.get("arguments").and_then(|v| v.as_str()) {
                            if !args.is_empty() {
                                acc.arguments.push_str(args);
                            }
                        }
                    }
                }
            }
        }
        if let Some(fr) = choice.get("finish_reason").and_then(|v| v.as_str()) {
            if !fr.is_empty() {
                *finish_reason = fr.to_string();
            }
        }
    }
    Ok(false)
}

// ── 工具函数 ──

trait StrExt {
    fn is_blank_trim(&self) -> bool;
}

impl StrExt for str {
    fn is_blank_trim(&self) -> bool {
        self.trim().is_empty()
    }
}

/// 读取图片文件为 base64 + MIME
fn read_image_as_base64(path: &str, mime_hint: &Option<String>) -> Result<(String, String), String> {
    let data = std::fs::read(path).map_err(|e| format!("读取图片失败: {e}"))?;
    let mime = mime_hint
        .clone()
        .unwrap_or_else(|| guess_mime(path));
    let b64 = base64_encode(&data);
    Ok((mime, b64))
}

fn guess_mime(path: &str) -> String {
    let lower = path.to_lowercase();
    if lower.ends_with(".png") {
        "image/png".to_string()
    } else if lower.ends_with(".webp") {
        "image/webp".to_string()
    } else if lower.ends_with(".gif") {
        "image/gif".to_string()
    } else {
        "image/jpeg".to_string()
    }
}

/// 极简 base64 编码（避免引入额外依赖）
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[(n >> 18) as usize & 63] as char);
        out.push(CHARS[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(CHARS[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// 从错误响应 JSON 提取 error.message
fn extract_error_message(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    let msg = v
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .or_else(|| {
            v.get("error")
                .and_then(|e| e.as_str())
        });
    msg.map(|s| s.to_string())
}

/// 解析 OpenAI 兼容响应 → {content, tool_calls, finish_reason}
fn parse_openai_response(body: &str) -> String {
    let v: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return json!({"content": "", "tool_calls": [], "finish_reason": "error"})
                .to_string();
        }
    };
    if let Some(err) = v.get("error") {
        return json!({
            "content": "",
            "tool_calls": [],
            "finish_reason": "error",
            "error": err.get("message").and_then(|m| m.as_str()).unwrap_or("API返回错误"),
        })
        .to_string();
    }
    let choice = v.get("choices").and_then(|c| c.as_array()).and_then(|a| a.first());
    let message = choice.and_then(|c| c.get("message"));
    let content = message
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();
    let finish_reason = choice
        .and_then(|c| c.get("finish_reason"))
        .and_then(|f| f.as_str())
        .unwrap_or("")
        .to_string();
    let tool_calls: Vec<Value> = message
        .and_then(|m| m.get("tool_calls"))
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .map(|tc| {
                    json!({
                        "id": tc.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                        "name": tc.get("function").and_then(|f| f.get("name")).and_then(|v| v.as_str()).unwrap_or(""),
                        "arguments": tc.get("function").and_then(|f| f.get("arguments")).and_then(|v| v.as_str()).unwrap_or("{}"),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "content": content,
        "reasoning_content": message.and_then(|m| m.get("reasoning_content")).and_then(Value::as_str).unwrap_or(""),
        "tool_calls": tool_calls,
        "finish_reason": finish_reason,
        // token 用量：Agent 路径 HTTP 已下沉 Rust，Kotlin AiService 不再经过，
        // 必须由这里带出，否则 token_usage 表永远为空（真机实测 0 行）。
        "usage": v.get("usage").cloned().unwrap_or(Value::Null),
    })
    .to_string()
}

/// 解析 Anthropic 响应 → 同构 {content, tool_calls, finish_reason}
fn parse_anthropic_response(body: &str) -> String {
    let v: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return json!({"content": "", "tool_calls": [], "finish_reason": "error"})
                .to_string();
        }
    };
    if let Some(err) = v.get("error") {
        return json!({
            "content": "",
            "tool_calls": [],
            "finish_reason": "error",
            "error": err.get("message").and_then(|m| m.as_str()).unwrap_or("API返回错误"),
        })
        .to_string();
    }
    // 文本块 + tool_use 块：此前 tool_use 被整体丢弃，导致 Anthropic 通道下
    // 模型即使请求调用工具也不会产生 tool_calls，Agent 循环直接以空正文结束。
    let mut content = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    if let Some(blocks) = v.get("content").and_then(|c| c.as_array()) {
        for block in blocks {
            match block.get("type").and_then(|t| t.as_str()) {
                Some("text") => {
                    if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                        content.push_str(text);
                    }
                }
                Some("tool_use") => {
                    tool_calls.push(json!({
                        "id": block.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                        "name": block.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                        "arguments": block.get("input").cloned().unwrap_or(json!({})).to_string(),
                    }));
                }
                _ => {}
            }
        }
    } else if let Some(text) = v.get("content").and_then(|c| c.as_str()) {
        content.push_str(text);
    }
    let stop_reason = v
        .get("stop_reason")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    json!({
        "content": content,
        "tool_calls": tool_calls,
        "finish_reason": match stop_reason.as_str() {
            "tool_use" => "tool_calls",
            "max_tokens" => "length",
            _ => "stop",
        },
        // Anthropic 用 input_tokens/output_tokens，原样带出由 Kotlin 归一
        "usage": v.get("usage").cloned().unwrap_or(Value::Null),
    })
    .to_string()
}

/// 每轮重建并替换 persona system：移除旧 persona system 消息（保留 [回合状态 状态消息），
/// 在消息数组头部插入最新的 system prompt。新架构下废除旧 has_persona_system 冻结逻辑。
fn replace_persona_system(msgs: &mut Vec<Value>, system_prompt: &str) {
    if system_prompt.trim().is_empty() {
        return;
    }
    msgs.retain(|m| {
        m.get("role").and_then(|v| v.as_str()) != Some("system")
            || m.get("_agent_preserve_system").and_then(Value::as_bool) == Some(true)
            || m.get("content")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .starts_with("[回合状态")
    });
    // 内部来源标记不能泄露到上游协议。
    for message in msgs.iter_mut() {
        if let Some(object) = message.as_object_mut() {
            object.remove("_agent_preserve_system");
        }
    }
    msgs.insert(0, json!({"role": "system", "content": system_prompt}));
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::testutil::FakeSigner;
    use crate::retry::RetryPolicy;

    /// 复用测试 sink：记录 delta / error / done
    struct CollectingSink {
        deltas: Mutex<Vec<String>>,
        errors: Mutex<Vec<String>>,
        done: Mutex<Option<(String, String)>>,
    }
    impl StreamSink for CollectingSink {
        fn on_text_delta(&self, text: String) {
            self.deltas.lock().unwrap().push(text);
        }
        fn on_reasoning_delta(&self, _text: String) {}
        fn on_done(&self, full: String, reason: String) {
            *self.done.lock().unwrap() = Some((full, reason));
        }
        fn on_error(&self, error: String) {
            self.errors.lock().unwrap().push(error);
        }
    }
    fn collecting_sink() -> CollectingSink {
        CollectingSink {
            deltas: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            done: Mutex::new(None),
        }
    }

    #[test]
    fn base64_encode_roundtrip() {
        let data = b"hello world";
        let enc = base64_encode(data);
        assert_eq!(enc, "aGVsbG8gd29ybGQ=");
    }

    #[test]
    fn parse_openai_response_with_tool_calls() {
        let body = r#"{
            "choices": [{
                "message": {
                    "content": null,
                    "reasoning_content": "先检查工具",
                    "tool_calls": [{
                        "id": "c1",
                        "function": {"name": "emit_segmented", "arguments": "{\"text\":\"hi\"}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        }"#;
        let parsed = parse_openai_response(body);
        let v: Value = serde_json::from_str(&parsed).unwrap();
        assert_eq!(v["finish_reason"], "tool_calls");
        assert_eq!(v["tool_calls"][0]["name"], "emit_segmented");
        assert_eq!(v["reasoning_content"], "先检查工具");
    }

    #[test]
    fn parse_openai_response_plain() {
        let body = r#"{
            "choices": [{
                "message": {"content": "你好呀"},
                "finish_reason": "stop"
            }]
        }"#;
        let parsed = parse_openai_response(body);
        let v: Value = serde_json::from_str(&parsed).unwrap();
        assert_eq!(v["content"], "你好呀");
    }

    #[test]
    fn parse_anthropic_response_plain() {
        let body = r#"{
            "content": [{"type": "text", "text": "你好"}],
            "stop_reason": "end_turn"
        }"#;
        let parsed = parse_anthropic_response(body);
        let v: Value = serde_json::from_str(&parsed).unwrap();
        assert_eq!(v["content"], "你好");
        assert_eq!(v["finish_reason"], "stop");
    }

    #[test]
    fn extract_error_message_from_body() {
        let body = r#"{"error": {"message": "invalid api key"}}"#;
        assert_eq!(
            extract_error_message(body),
            Some("invalid api key".to_string())
        );
    }

    // ── SQLite 直读 + 请求组装集成测试 ──

    #[test]
    fn load_api_config_reads_sqlite() {
        let db = testutil::temp_db();
        let gw = NativeGateway::new(testutil::global_config(&db));
        let cfg = gw.load_api_config().unwrap();
        assert_eq!(cfg.provider, "OPENAI");
        assert_eq!(cfg.model, "gpt-4o-mini");
        assert_eq!(cfg.temperature, 0.7);
        let keys = gw.all_api_keys(&cfg);
        assert_eq!(keys, vec!["sk-test-main", "sk-test-extra1", "sk-test-extra2"]);
    }

    #[test]
    fn load_companion_profile_reads_sqlite() {
        let db = testutil::temp_db();
        let gw = NativeGateway::new(testutil::global_config(&db));
        let p = gw.load_companion_profile(Some(1)).unwrap();
        assert_eq!(p.name, "小恋");
        assert_eq!(p.age, Some(22));
        assert!(p.personality.contains("温柔"));
        assert_eq!(p.role, "GIRLFRIEND");
    }

    #[test]
    fn replace_persona_system_replaces_old_system() {
        // 每轮重建：旧 persona system 被移除，最新 system prompt 插入头部，[回合状态 保留
        let mut msgs = vec![
            json!({"role": "system", "content": "旧的人设"}),
            json!({"role": "user", "content": "在吗"}),
            json!({"role": "system", "content": "[回合状态 1/3] 摘要"}),
        ];
        replace_persona_system(&mut msgs, "新的人设");
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "新的人设");
        assert_eq!(msgs[1]["role"], "user");
        assert!(msgs[2]["content"].as_str().unwrap().starts_with("[回合状态"));
        // 空 system_prompt 不插入也不清理
        let mut msgs2 = vec![json!({"role": "user", "content": "x"})];
        replace_persona_system(&mut msgs2, "");
        assert_eq!(msgs2.len(), 1);
    }

    #[test]
    fn persona_replacement_preserves_runtime_system_injections() {
        let mut messages = vec![
            json!({"role":"system", "content":"old persona"}),
            json!({"role":"system", "content":"worldbook", "_agent_preserve_system":true}),
            json!({"role":"user", "content":"hello"}),
            json!({"role":"system", "content":"stop sticker retries", "_agent_preserve_system":true}),
        ];
        replace_persona_system(&mut messages, "new persona");
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1]["content"], "worldbook");
        assert_eq!(messages[3]["content"], "stop sticker retries");
        assert!(messages.iter().all(|m| m.get("_agent_preserve_system").is_none()));
    }

    #[test]
    fn send_assembles_openai_body_with_system_and_tools() {
        let db = testutil::temp_db();
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"choices":[{"message":{"content":"你好呀"},"finish_reason":"stop"}]}"#,
        ]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![crate::agent::ToolDefinition {
                name: "order_coffee".to_string(),
                description: "点咖啡".to_string(),
                parameters_json: r#"{"type":"object","properties":{}}"#.to_string(),
                category: crate::agent::ToolCategory::Custom,
                toolsets: vec!["domain".to_string()],
                available: true,
            }],
            max_rounds: 3,
            tool_choice: "required".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: Some("你是小恋，一位女朋友，正在与用户进行日常陪伴对话。".to_string()),
            companion_name_map_json: None,
        };
        let messages = serde_json::from_str::<Value>(&request.history_json).unwrap();
        let msgs = messages.as_array().unwrap().clone();
        let tools_value = json!([{"type":"function","function":{"name":"order_coffee","description":"点咖啡","parameters":{"type":"object","properties":{}}}}]);
        let raw = gw.send(&request, Some(1), &msgs, &tools_value, "required").unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "你好呀");

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (url, headers, body) = &requests[0];
        assert!(url.ends_with("/chat/completions"));
        assert!(headers.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-test-main"));
        let body_v: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body_v["model"], "gpt-4o-mini");
        assert_eq!(body_v["stream"], false);
        assert_eq!(body_v["max_tokens"], 800);
        assert_eq!(body_v["temperature"], 0.7);
        let msgs = body_v["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "system");
        assert!(msgs[0]["content"].as_str().unwrap().contains("小恋"));
        assert_eq!(msgs[1]["content"], "在吗");
        assert!(body_v["tools"].is_array());
        assert_eq!(body_v["tool_choice"], "required");
    }

    #[test]
    fn send_fails_over_to_extra_keys() {
        let db = testutil::temp_db();
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"error":{"message":"rate limit"}}"#,
            r#"{"choices":[{"message":{"content":"成功"},"finish_reason":"stop"}]}"#,
        ]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let raw = gw.send(&request, Some(1), &[], &json!([]), "auto").unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "成功");
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].1.iter().any(|(_, v)| v == "Bearer sk-test-main"));
        assert!(requests[1].1.iter().any(|(_, v)| v == "Bearer sk-test-extra1"));
    }

    #[test]
    fn send_stream_delivers_deltas_and_full_result() {
        let db = testutil::temp_db();
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"content":"流式回复内容","finish_reason":"stop"}"#,
        ]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let sink = collecting_sink();
        let raw = gw.send_stream(&request, Some(1), &[], &json!([]), "auto", &sink).unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "流式回复内容");
        assert_eq!(sink.deltas.lock().unwrap().join(""), "流式回复内容");
        let (full, reason) = sink.done.lock().unwrap().clone().unwrap();
        assert_eq!(full, "流式回复内容");
        assert_eq!(reason, "stop");
    }

    // ── SSE 流式解析（handle_sse_line 纯函数） ──

    #[test]
    fn sse_parses_deltas_reasoning_and_done() {
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        let lines = [
            r#"data: {"choices":[{"delta":{"content":"你"}}]}"#,
            r#"data: {"choices":[{"delta":{"content":"好"}}]}"#,
            r#"data: {"choices":[{"delta":{"reasoning_content":"思考中..."}}]}"#,
            r#"data: {"choices":[{"delta":{"content":"呀"},"finish_reason":"stop"}]}"#,
            "data: [DONE]",
            // [DONE] 之后的行不应再被处理
            r#"data: {"choices":[{"delta":{"content":"不应出现"}}]}"#,
        ];
        let mut stop = false;
        for line in lines {
            if stop {
                break;
            }
            if handle_sse_line(line, &mut full, &mut reasoning, &mut reason, Some(&sink), &mut tcs).unwrap() {
                stop = true;
            }
        }
        assert_eq!(full, "你好呀");
        assert_eq!(reasoning, "思考中...");
        assert_eq!(reason, "stop");
        assert_eq!(sink.deltas.lock().unwrap().join(""), "你好呀");
        assert!(tcs.is_empty());
    }

    #[test]
    fn sse_ignores_malformed_and_non_data_lines() {
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        // 空行 / 注释行 / event 行 / 非法 JSON 全部跳过且不报错
        for line in [
            "",
            " ",
            ": keep-alive",
            "event: message",
            "data: 不是json{{",
        ] {
            assert!(!handle_sse_line(line, &mut full, &mut reasoning, &mut reason, Some(&sink), &mut tcs).unwrap());
        }
        assert!(full.is_empty());
        assert!(sink.errors.lock().unwrap().is_empty());
    }

    #[test]
    fn sse_reports_chunk_error_and_calls_sink() {
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        let err = handle_sse_line(
            r#"data: {"error":{"message":"rate limit exceeded"}}"#,
            &mut full,
            &mut reasoning,
            &mut reason,
            Some(&sink),
            &mut tcs,
        )
        .unwrap_err();
        assert!(err.contains("rate limit"));
        assert_eq!(sink.errors.lock().unwrap().join(","), "rate limit exceeded");
        assert!(full.is_empty());
    }

    #[test]
    fn sse_extracts_finish_reason_from_last_chunk() {
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        let lines = [
            r#"data: {"choices":[{"delta":{"content":"hi"}}]}"#,
            r#"data: {"choices":[{"delta":{},"finish_reason":"length"}]}"#,
            "data: [DONE]",
        ];
        for line in lines {
            if handle_sse_line(line, &mut full, &mut reasoning, &mut reason, Some(&sink), &mut tcs).unwrap() {
                break;
            }
        }
        assert_eq!(full, "hi");
        assert_eq!(reason, "length");
    }

    #[test]
    fn sse_aggregates_tool_calls_across_delta_chunks() {
        // OpenAI 兼容流式工具调用：name 在首个分片，arguments 跨多个分片增量到达
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        let lines = [
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"emit_segmented","arguments":"{\"text\":\"从前"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"有一只小刺猬\"}"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ];
        let mut stop = false;
        for line in lines {
            if stop {
                break;
            }
            if handle_sse_line(line, &mut full, &mut reasoning, &mut reason, Some(&sink), &mut tcs).unwrap() {
                stop = true;
            }
        }
        assert_eq!(reason, "tool_calls");
        assert_eq!(tcs.len(), 1);
        assert_eq!(tcs[0].id, "call_abc");
        assert_eq!(tcs[0].name, "emit_segmented");
        // 跨分片 arguments 拼接完整
        assert_eq!(tcs[0].arguments, r#"{"text":"从前有一只小刺猬"}"#);
        assert!(full.is_empty(), "工具调用时不应有正文增量");
    }

    #[test]
    fn sse_aggregates_multiple_tool_calls_by_index() {
        let sink = collecting_sink();
        let mut full = String::new();
        let mut reasoning = String::new();
        let mut reason = "stop".to_string();
        let mut tcs: Vec<StreamToolCallAcc> = Vec::new();
        // 两个并行工具调用：index 0 = emit_bubble，index 1 = send_sticker
        let lines = [
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"emit_bubble","arguments":"{\"text\":\"嗨\"}"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"c2","function":{"name":"send_sticker","arguments":"{\"description\":\"开心\"}"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ];
        let mut stop = false;
        for line in lines {
            if stop {
                break;
            }
            if handle_sse_line(line, &mut full, &mut reasoning, &mut reason, Some(&sink), &mut tcs).unwrap() {
                stop = true;
            }
        }
        assert_eq!(tcs.len(), 2);
        assert_eq!(tcs[0].name, "emit_bubble");
        assert_eq!(tcs[1].name, "send_sticker");
        assert_eq!(reason, "tool_calls");
    }

    // ── provider 认证头 ──

    #[test]
    fn provider_headers_partner_uses_credentials() {
        let gw = NativeGateway::new(AgentGlobalConfig {
            db_path: String::new(),
            device_id: String::new(),
            settings_json: "{}".into(),
            stickers: vec![],
            credentials_json: r#"{"session":"sess-abc","client_id":"cli-123"}"#.into(),
            orchestrator: None,
        });
        let row = ApiConfigRow {
            provider: "PARTNER".into(),
            api_key: String::new(),
            extra_api_keys: String::new(),
            base_url: "https://partner.example".into(),
            model: "m".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let headers = gw.provider_headers(&row, "");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "X-LianYu-Session" && v == "sess-abc"));
        assert!(headers
            .iter()
            .any(|(k, v)| k == "X-LianYu-Client-Id" && v == "cli-123"));
        assert!(!headers.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn send_partner_with_empty_api_key_sends_session_headers() {
        // 回归：PARTNER 配置 apiKey/extraApiKeys 均为空（靠 session 认证）时，
        // send 不得报"没有可用的 API Key"，应正常发出带会话头的请求。
        let db = testutil::temp_db();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE api_configs SET provider='PARTNER', apiKey='', extraApiKeys='', \
             baseUrl='https://suflow.cloud/v1', model='auto', isEnabled=1 WHERE id=1",
            [],
        )
        .unwrap();
        drop(conn);
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"choices":[{"message":{"content":"你好呀"},"finish_reason":"stop"}]}"#,
        ]));
        let mut cfg = testutil::global_config(&db);
        cfg.credentials_json = r#"{"session":"sess-abc","client_id":"cli-123"}"#.into();
        let gw = NativeGateway::with_transport_and_signer(cfg, transport.clone(), Some(Arc::new(FakeSigner)));
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "required".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let msgs = serde_json::from_str::<Value>(&request.history_json).unwrap();
        let raw = gw
            .send(&request, Some(1), msgs.as_array().unwrap(), &json!([]), "required")
            .unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "你好呀");

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].0.ends_with("/chat/completions"));
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "X-LianYu-Session" && v == "sess-abc"));
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "X-LianYu-Client-Id" && v == "cli-123"));
        assert!(!requests[0].1.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn send_stream_partner_with_empty_api_key_sends_session_headers() {
        // 回归：流式路径同样豁免 PARTNER 空 key 检查。
        let db = testutil::temp_db();
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE api_configs SET provider='PARTNER', apiKey='', extraApiKeys='', \
             baseUrl='https://suflow.cloud/v1', model='auto', isEnabled=1 WHERE id=1",
            [],
        )
        .unwrap();
        drop(conn);
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"content":"你好呀","finish_reason":"stop"}"#,
        ]));
        let mut cfg = testutil::global_config(&db);
        cfg.credentials_json = r#"{"session":"sess-abc","client_id":"cli-123"}"#.into();
        let gw = NativeGateway::with_transport_and_signer(cfg, transport.clone(), Some(Arc::new(FakeSigner)));
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let sink = collecting_sink();
        let raw = gw
            .send_stream(&request, Some(1), &[], &json!([]), "auto", &sink)
            .unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "你好呀");
        assert_eq!(sink.deltas.lock().unwrap().join(""), "你好呀");

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "X-LianYu-Session" && v == "sess-abc"));
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "X-LianYu-Client-Id" && v == "cli-123"));
        assert!(!requests[0].1.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn provider_headers_xiaomi_uses_api_key() {
        let gw = NativeGateway::new(testutil::global_config(""));
        let row = ApiConfigRow {
            provider: "XIAOMI".into(),
            api_key: "mk-test".into(),
            extra_api_keys: String::new(),
            base_url: "https://api.xiaomi.com".into(),
            model: "m".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let headers = gw.provider_headers(&row, "mk-test");
        assert!(headers.iter().any(|(k, v)| k == "api-key" && v == "mk-test"));
        assert!(!headers.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn send_uses_credentials_api_key_over_encrypted_db_value() {
        // 回归：SQLite 中 apiKey 为 Tink 加密串（enc:v4:...），Rust 无法解密。
        // Kotlin 解密后经 credentials_json.api_key 传入，必须优先于加密串使用。
        let db = testutil::temp_db();
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "UPDATE api_configs SET provider='DEEPSEEK', \
                 apiKey='enc:v4:tink-env:AAAAfakeencrypted', extraApiKeys='', \
                 baseUrl='https://api.deepseek.com', model='deepseek-v4-flash', isEnabled=1 WHERE id=1",
                [],
            )
            .unwrap();
        }
        drop(Connection::open(&db).unwrap());
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"choices":[{"message":{"content":"解密成功"},"finish_reason":"stop"}]}"#,
        ]));
        let mut cfg = testutil::global_config(&db);
        cfg.credentials_json = r#"{"api_key":"sk-decrypted-real-key"}"#.into();
        let gw = NativeGateway::with_transport(cfg, transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let msgs = serde_json::from_str::<Value>(&request.history_json).unwrap();
        let raw = gw
            .send(&request, Some(1), msgs.as_array().unwrap(), &json!([]), "auto")
            .unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "解密成功");

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].0.contains("api.deepseek.com"));
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk-decrypted-real-key"));
        // 绝不能把加密串当 key 发送
        assert!(!requests[0]
            .1
            .iter()
            .any(|(_, v)| v.contains("enc:v4:")));
    }

    #[test]
    fn send_stream_uses_credentials_api_key() {
        // 回归：流式路径同样使用 credentials 传入的明文 key。
        let db = testutil::temp_db();
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "UPDATE api_configs SET provider='DEEPSEEK', \
                 apiKey='enc:v4:tink-env:AAAAfakeencrypted', extraApiKeys='', \
                 baseUrl='https://api.deepseek.com', model='deepseek-v4-flash', isEnabled=1 WHERE id=1",
                [],
            )
            .unwrap();
        }
        drop(Connection::open(&db).unwrap());
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"content":"流式成功","finish_reason":"stop"}"#,
        ]));
        let mut cfg = testutil::global_config(&db);
        cfg.credentials_json = r#"{"api_key":"sk-decrypted-real-key"}"#.into();
        let gw = NativeGateway::with_transport(cfg, transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let sink = collecting_sink();
        let raw = gw
            .send_stream(&request, Some(1), &[], &json!([]), "auto", &sink)
            .unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "流式成功");
        assert_eq!(sink.deltas.lock().unwrap().join(""), "流式成功");

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk-decrypted-real-key"));
        assert!(!requests[0]
            .1
            .iter()
            .any(|(_, v)| v.contains("enc:v4:")));
    }

    #[test]
    fn provider_headers_default_bearer() {
        let gw = NativeGateway::new(testutil::global_config(""));
        let row = ApiConfigRow {
            provider: "OPENAI".into(),
            api_key: "sk-1".into(),
            extra_api_keys: String::new(),
            base_url: "https://api.openai.com/v1".into(),
            model: "gpt-4o-mini".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let headers = gw.provider_headers(&row, "sk-1");
        assert!(headers
            .iter()
            .any(|(k, v)| k == "Authorization" && v == "Bearer sk-1"));
    }

    // ── Anthropic 流式降级 ──

    #[test]
    fn send_stream_anthropic_falls_back_to_full_response() {
        let db = testutil::temp_db();
        {
            let conn = Connection::open(&db).unwrap();
            conn.execute(
                "UPDATE api_configs SET provider='ANTHROPIC', formatHint='anthropic', \
                 apiKey='sk-ant-test', baseUrl='https://api.anthropic.com', model='claude-3-5-sonnet' \
                 WHERE id=1",
                [],
            )
            .unwrap();
        }
        let transport = Arc::new(testutil::MockTransport::new(vec![
            r#"{"content":[{"type":"text","text":"降级成功"}],"stop_reason":"end_turn"}"#,
        ]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let sink = collecting_sink();
        let raw = gw
            .send_stream(&request, Some(1), &[], &json!([]), "auto", &sink)
            .unwrap();
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "降级成功");
        let (full, reason) = sink.done.lock().unwrap().clone().unwrap();
        assert_eq!(full, "降级成功");
        assert_eq!(reason, "stop");
        // 确认走 anthropic /messages 而非 /chat/completions
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].0.ends_with("/messages"));
        assert!(reqs[0].1.iter().any(|(k, _)| k == "x-api-key"));
    }

    #[test]
    fn send_stream_returns_transport_error_message() {
        let db = testutil::temp_db();
        // 预置响应耗尽 → MockTransport 返回 Err
        let transport = Arc::new(testutil::MockTransport::new(vec![]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let sink = collecting_sink();
        let err = gw
            .send_stream(&request, Some(1), &[], &json!([]), "auto", &sink)
            .unwrap_err();
        assert!(err.contains("预置响应耗尽") || err.contains("请求失败"));
    }

    // ── 请求体组装 ──

    #[test]
    fn build_anthropic_body_drops_system_and_maps_roles() {
        let gw = NativeGateway::new(testutil::global_config(""));
        let row = ApiConfigRow {
            provider: "ANTHROPIC".into(),
            api_key: "sk-ant-test".into(),
            extra_api_keys: String::new(),
            base_url: "https://api.anthropic.com".into(),
            model: "claude-3-5-sonnet".into(),
            temperature: 0.7,
            max_tokens: Some(800),
            format_hint: "anthropic".into(),
        };
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let messages = json!([
            {"role": "system", "content": "你是小恋"},
            {"role": "user", "content": "你好"},
            {"role": "assistant", "content": "嗨"},
        ]);
        let tools = json!([
            {"type": "function", "function": {
                "name": "emit_bubble",
                "description": "发一条气泡",
                "parameters": {"type": "object", "properties": {"text": {"type": "string"}}}
            }}
        ]);
        let body = gw.build_anthropic_body(
            &row,
            messages.as_array().unwrap(),
            "SYSTEM_PROMPT",
            &request,
            &tools,
            "auto",
        );
        assert_eq!(body["model"], "claude-3-5-sonnet");
        assert_eq!(body["system"], "SYSTEM_PROMPT");
        assert_eq!(body["max_tokens"], 800);
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[1]["role"], "assistant");
        assert!(msgs.iter().all(|m| m["role"] != "system"));
        // OpenAI function 结构必须转成 Anthropic input_schema，否则工具对模型完全不可见
        assert_eq!(body["tools"][0]["name"], "emit_bubble");
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(body["tool_choice"]["type"], "auto");
    }

    /// Anthropic 回灌：assistant 的 tool_calls 必须变成 tool_use，tool 结果变成 tool_result，
    /// 否则多轮工具调用在第二轮就会被服务端拒绝（tool_result 无对应 tool_use）。
    #[test]
    fn anthropic_body_replays_tool_calls_and_merges_tool_results() {
        let gw = NativeGateway::new(testutil::global_config(""));
        let row = ApiConfigRow {
            provider: "ANTHROPIC".into(),
            api_key: "sk-ant-test".into(),
            extra_api_keys: String::new(),
            base_url: "https://api.anthropic.com".into(),
            model: "claude-3-5-sonnet".into(),
            temperature: 0.7,
            max_tokens: Some(800),
            format_hint: "anthropic".into(),
        };
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: "[]".to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let messages = json!([
            {"role": "user", "content": "在吗"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "toolu_1", "type": "function",
                 "function": {"name": "emit_bubble", "arguments": "{\"text\":\"在的\"}"}}
            ]},
            {"role": "tool", "tool_call_id": "toolu_1", "content": "ok"},
            {"role": "tool", "tool_call_id": "toolu_1", "content": "第二条"},
            {"role": "system", "content": "[回合状态 1/3] 摘要", "_agent_preserve_system": true},
        ]);
        let body = gw.build_anthropic_body(
            &row,
            messages.as_array().unwrap(),
            "SYSTEM_PROMPT",
            &request,
            &json!([]),
            "none",
        );
        let msgs = body["messages"].as_array().unwrap();
        // user + assistant + 合并后的 user(2 个 tool_result)
        assert_eq!(msgs.len(), 3, "tool_result 应合并进同一条 user 消息: {body}");
        let assistant_blocks = msgs[1]["content"].as_array().unwrap();
        assert_eq!(assistant_blocks[0]["type"], "tool_use");
        assert_eq!(assistant_blocks[0]["id"], "toolu_1");
        assert_eq!(assistant_blocks[0]["name"], "emit_bubble");
        assert_eq!(assistant_blocks[0]["input"]["text"], "在的");
        let results = msgs[2]["content"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["type"], "tool_result");
        assert_eq!(results[0]["tool_use_id"], "toolu_1");
        // 运行时注入的 system 片段不能因 Anthropic 禁止 system 角色而丢失
        assert!(body["system"].as_str().unwrap().contains("[回合状态 1/3]"), "{body}");
        // tool_choice=none 时完全不暴露工具
        assert!(body.get("tools").is_none(), "{body}");
    }

    /// 解析：tool_use 块必须产出可执行的 tool_calls（此前被整体丢弃 → 工具永不执行）。
    #[test]
    fn anthropic_parse_extracts_tool_use_blocks() {
        let body = r#"{
            "content": [
                {"type": "text", "text": "我看看"},
                {"type": "tool_use", "id": "toolu_9", "name": "emit_bubble", "input": {"text": "好呀"}}
            ],
            "stop_reason": "tool_use"
        }"#;
        let parsed = parse_anthropic_response(body);
        let v: Value = serde_json::from_str(&parsed).unwrap();
        assert_eq!(v["content"], "我看看");
        assert_eq!(v["finish_reason"], "tool_calls");
        assert_eq!(v["tool_calls"][0]["id"], "toolu_9");
        assert_eq!(v["tool_calls"][0]["name"], "emit_bubble");
        let args: Value =
            serde_json::from_str(v["tool_calls"][0]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(args["text"], "好呀");
    }

    #[test]
    fn build_openai_body_embeds_image_data_url() {
        let gw = NativeGateway::new(testutil::global_config(""));
        let row = ApiConfigRow {
            provider: "OPENAI".into(),
            api_key: "sk-1".into(),
            extra_api_keys: String::new(),
            base_url: "https://api.openai.com/v1".into(),
            model: "gpt-4o-mini".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let request = crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: Some(crate::agent::ImageInput {
                path: None,
                base64_data: Some("aGVsbG8=".into()),
                mime_type: Some("image/png".into()),
            }),
            system_prompt: None,
            companion_name_map_json: None,
        };
        let messages = json!([{"role": "user", "content": "看图"}]);
        let body = gw.build_openai_body(
            &row,
            messages.as_array().unwrap(),
            &json!([]),
            "auto",
            &request,
            false,
        );
        let msgs = body["messages"].as_array().unwrap();
        let content = msgs[0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "看图");
        assert_eq!(content[1]["type"], "image_url");
        let url = content[1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,aGVsbG8="));
    }

    // ── temperature 2 位小数（智谱清言 HTTP 400 回归）──
    //
    // 线上报障（2026-10-11）：智谱清言渠道聊天必失败
    //   「HTTP 400: temperature参数非法 限制小数点[2]位」
    // 根因：Room 把 Kotlin Float 写 SQLite REAL 时按 Float→Double 加宽
    // （0.7f → 0.699999988079071），Rust 网关原样序列化 → 严格网关直接拒绝。
    // 修复见 temperature.rs。以下测试从**序列化后的请求体字符串**旁路取证。

    /// 构造指定 temperature 的配置行（provider 取报障渠道 ZHIPU）
    fn temperature_row(temperature: f64) -> ApiConfigRow {
        ApiConfigRow {
            provider: "ZHIPU".into(),
            api_key: "sk-1".into(),
            extra_api_keys: String::new(),
            base_url: "https://open.bigmodel.cn/api/paas/v4".into(),
            model: "glm-4-flash".into(),
            temperature,
            max_tokens: None,
            format_hint: String::new(),
        }
    }

    fn temperature_request() -> crate::agent::AgentTurnRequest {
        crate::agent::AgentTurnRequest {
            group_id: None,
            history_json: "[]".to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        }
    }

    /// 取证 + 回归：Float 加宽伪影必须在写进请求体前归一到 2 位小数
    #[test]
    fn zhipu_openai_body_temperature_is_two_decimals() {
        let widened = 0.7f32 as f64; // Room：Float 0.7 → SQLite REAL
        assert_eq!(
            format!("{widened}"),
            "0.699999988079071",
            "前提：Float 加宽确实带伪影（本修复的存在理由）"
        );
        let gw = NativeGateway::new(testutil::global_config(""));
        let body = gw.build_openai_body(
            &temperature_row(widened),
            &[],
            &json!([]),
            "auto",
            &temperature_request(),
            false,
        );
        assert_eq!(body["temperature"], 0.7);
        let serialized = body.to_string();
        assert!(
            serialized.contains("\"temperature\":0.7"),
            "请求体应为干净的 2 位小数: {serialized}"
        );
        assert!(
            !serialized.contains("0.699999988079071"),
            "请求体仍带 Float 伪影（智谱清言会 400）: {serialized}"
        );
    }

    /// 穷举 [0.10, 1.50]：任意两位小数配置都不得序列化出 >2 位小数
    #[test]
    fn openai_body_temperature_never_has_more_than_two_decimals() {
        let gw = NativeGateway::new(testutil::global_config(""));
        for hundredths in 10..=150i32 {
            let raw = (hundredths as f32 / 100.0) as f64; // 模拟 Room 读出的加宽值
            let body = gw.build_openai_body(
                &temperature_row(raw),
                &[],
                &json!([]),
                "auto",
                &temperature_request(),
                false,
            );
            let text = format!("{}", body["temperature"].as_f64().unwrap());
            let decimals = text
                .split_once('.')
                .map(|(_, frac)| frac.trim_end_matches('0').len())
                .unwrap_or(0);
            assert!(decimals <= 2, "hundredths={hundredths} 产出 {text}（小数位 {decimals}）");
        }
    }

    /// Anthropic 通道同样必须归一（与 api_probe 的 Anthropic 测试同源）
    #[test]
    fn anthropic_body_temperature_is_two_decimals() {
        let widened = 0.85f32 as f64;
        assert_eq!(format!("{widened}"), "0.8500000238418579");
        let gw = NativeGateway::new(testutil::global_config(""));
        let body = gw.build_anthropic_body(
            &temperature_row(widened),
            &[],
            "SYSTEM",
            &temperature_request(),
            &json!([]),
            "auto",
        );
        assert_eq!(body["temperature"], 0.85);
        assert!(
            !body.to_string().contains("0.8500000238418579"),
            "Anthropic 请求体仍带 Float 伪影: {body}"
        );
    }

    /// 固定温度模型（kimi-k2.6）不得携带 temperature——连接测试已按此判定通过，
    /// 生成路径此前漏判 → Kimi 渠道「测试通过但聊天必 400」。
    #[test]
    fn openai_body_omits_temperature_for_fixed_temperature_model() {
        let gw = NativeGateway::new(testutil::global_config(""));
        for model in ["kimi-k2.6", "kimi-k2.6-turbo", "Moonshot-k2.6"] {
            let mut row = temperature_row(0.7f32 as f64);
            row.provider = "KIMI".into();
            row.model = model.into();
            let body = gw.build_openai_body(
                &row,
                &[],
                &json!([]),
                "auto",
                &temperature_request(),
                false,
            );
            assert!(
                body.get("temperature").is_none(),
                "model={model} 固定温度模型不应带 temperature: {body}"
            );
        }
        // 对照：非固定温度模型仍带归一后的 temperature
        let mut row = temperature_row(0.7f32 as f64);
        row.model = "glm-4-flash".into();
        let body = gw.build_openai_body(
            &row,
            &[],
            &json!([]),
            "auto",
            &temperature_request(),
            false,
        );
        assert_eq!(body["temperature"], 0.7);
    }

    #[test]
    fn parse_openai_response_carries_error_field() {
        let parsed = parse_openai_response(r#"{"error":{"message":"bad key"}}"#);
        let v: Value = serde_json::from_str(&parsed).unwrap();
        assert_eq!(v["finish_reason"], "error");
        assert_eq!(v["error"], "bad key");
    }

    // ── LLM 调用自动重试（核心诉求：瞬时失败不得中断回合） ──

    /// 构造一个注入了「零等待睡眠」的网关（测试不真实 sleep）。
    fn gateway_with_retry(
        db: &str,
        transport: Arc<dyn HttpTransport>,
        sleeper: Arc<dyn Fn(Duration) + Send + Sync>,
    ) -> NativeGateway {
        NativeGateway::with_transport(testutil::global_config(db), transport)
            .with_retry(RetryPolicy::default(), sleeper)
    }

    fn simple_send_request() -> AgentTurnRequest {
        AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        }
    }

    /// 瞬时错误（5xx / 429）→ 自动重试 → 最终成功；3 次尝试全部发出。
    #[test]
    fn send_retries_transient_errors_then_succeeds() {
        let db = testutil::temp_db();
        let transport = Arc::new(testutil::ScriptedResultTransport::new(vec![
            Err("HTTP 503: service unavailable"),
            Err("HTTP 429: rate limited (retry-after 1s)"),
            Ok(r#"{"choices":[{"message":{"content":"重试成功"},"finish_reason":"stop"}]}"#),
        ]));
        let gw = gateway_with_retry(&db, transport.clone(), Arc::new(|_| {}));
        let request = simple_send_request();
        let raw = gw
            .send(&request, Some(1), &[], &json!([]), "auto")
            .expect("重试后应成功");
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "重试成功");
        // 证据：3 次尝试都被真实发出（前两次失败、第三次成功）
        assert_eq!(transport.requests.lock().unwrap().len(), 3);
    }

    /// 客户端错误（401）不可重试：立即失败，只发出 1 次请求，保留原始错误语义。
    #[test]
    fn send_does_not_retry_client_errors() {
        let db = testutil::temp_db_single_key();
        let transport = Arc::new(testutil::ScriptedResultTransport::new(vec![Err(
            "HTTP 401: invalid api key",
        )]));
        let gw = gateway_with_retry(&db, transport.clone(), Arc::new(|_| {}));
        let request = simple_send_request();
        let err = gw
            .send(&request, Some(1), &[], &json!([]), "auto")
            .unwrap_err();
        assert!(err.contains("401"), "应保留原始状态码，实际: {err}");
        assert!(err.contains("invalid api key"), "应保留原始错误信息，实际: {err}");
        assert_eq!(transport.requests.lock().unwrap().len(), 1, "401 不得重试");
    }

    /// 重试耗尽后保留最终失败信息（不吞错），尝试次数 = max_attempts。
    #[test]
    fn send_preserves_final_error_after_retries_exhausted() {
        let db = testutil::temp_db_single_key();
        let transport = Arc::new(testutil::ScriptedResultTransport::new(vec![Err(
            "HTTP 500: upstream boom",
        )]));
        let gw = gateway_with_retry(&db, transport.clone(), Arc::new(|_| {}));
        let request = simple_send_request();
        let err = gw
            .send(&request, Some(1), &[], &json!([]), "auto")
            .unwrap_err();
        assert!(err.contains("upstream boom"), "最终错误应保留原始信息，实际: {err}");
        assert_eq!(
            transport.requests.lock().unwrap().len(),
            RetryPolicy::default().max_attempts as usize
        );
    }

    /// 取消：退避等待期间被取消 → 停止重试，只尝试 1 次。
    #[test]
    fn send_retry_stops_when_cancelled_during_backoff() {
        let db = testutil::temp_db_single_key();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_for_sleep = cancel.clone();
        let transport = Arc::new(testutil::ScriptedResultTransport::new(vec![Err(
            "HTTP 503: service unavailable",
        )]));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone())
            .with_retry(RetryPolicy::default(), Arc::new(move |_| {
                cancel_for_sleep.store(true, Ordering::SeqCst);
            }))
            .with_cancel(cancel.clone());
        let request = simple_send_request();
        let err = gw
            .send(&request, Some(1), &[], &json!([]), "auto")
            .unwrap_err();
        assert!(err.contains("取消"), "应返回取消语义，实际: {err}");
        assert_eq!(transport.requests.lock().unwrap().len(), 1, "取消后不得再次尝试");
    }

    /// 流式：首个增量交付前的瞬时错误 → 重试成功。
    #[test]
    fn send_stream_retries_before_any_delta() {
        let db = testutil::temp_db();
        let transport = Arc::new(testutil::ScriptedResultTransport::new(vec![
            Err("HTTP 500: boom"),
            Ok(r#"{"content":"流式重试成功","finish_reason":"stop"}"#),
        ]));
        let gw = gateway_with_retry(&db, transport.clone(), Arc::new(|_| {}));
        let request = simple_send_request();
        let sink = collecting_sink();
        let raw = gw
            .send_stream(&request, Some(1), &[], &json!([]), "auto", &sink)
            .expect("流式重试后应成功");
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["content"], "流式重试成功");
        assert_eq!(sink.deltas.lock().unwrap().join(""), "流式重试成功");
        assert_eq!(transport.requests.lock().unwrap().len(), 2);
    }
}

// ── 测试工具（供 agent.rs tests 复用） ──

/// Eval：脚本化传输（生产代码，非测试设施）。
/// 由 AgentRuntime.set_mock_transport 注入：按序弹出预置响应，不发真实网络。
pub(crate) struct ScriptedTransport {
    pub responses: Mutex<Vec<String>>,
    pub requests: Mutex<Vec<(String, Vec<(String, String)>, String)>>,
}

impl ScriptedTransport {
    pub fn new(responses: Vec<String>) -> Self {
        ScriptedTransport {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl HttpTransport for ScriptedTransport {
    fn post_json(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: &str,
        stream: bool,
        sink: Option<&dyn StreamSink>,
    ) -> Result<String, String> {
        self.requests
            .lock()
            .unwrap()
            .push((url.to_string(), headers.to_vec(), body.to_string()));
        let mut responses = self.responses.lock().unwrap();
        let resp = responses
            .first()
            .cloned()
            .ok_or_else(|| "ScriptedTransport: 预置响应耗尽".to_string())?;
        if responses.len() > 1 {
            responses.remove(0);
        }
        if stream {
            if let Some(sink) = sink {
                let value: serde_json::Value =
                    serde_json::from_str(&resp).map_err(|e| e.to_string())?;
                let content = value
                    .pointer("/choices/0/message/content")
                    .or_else(|| value.pointer("/choices/0/delta/content"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if !content.is_empty() {
                    sink.on_text_delta(content.clone());
                }
                sink.on_done(content, "stop".to_string());
            }
        }
        Ok(resp)
    }

    fn get_json(&self, _url: &str, _headers: &[(String, String)]) -> Result<String, String> {
        Ok("[]".to_string())
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;

    /// 测试用签名器（产出 7 个 X-LianYu-* 头；供 tests 与 testutil 内测试共用）
    pub struct FakeSigner;
    impl crate::agent::RequestSignatureProvider for FakeSigner {
        fn sign_headers(
            &self,
            _method: String,
            _path: String,
            _body: String,
            _client_id: String,
        ) -> Vec<crate::agent::RequestHeader> {
            vec![
                crate::agent::RequestHeader { name: "X-LianYu-Sig-Version".into(), value: "v1".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Ts".into(), value: "1700000000".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Nonce".into(), value: "n1".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Body-SHA256".into(), value: "h".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Device-Id".into(), value: "d".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Key-Id".into(), value: "k".into() },
                crate::agent::RequestHeader { name: "X-LianYu-Sig".into(), value: "sig123".into() },
            ]
        }
    }

    /// 构造单 API Key 的临时库（清除备用 Key）——用于断言「尝试次数」的重试用例，
    /// 避免多 Key 故障转移把请求计数放大。
    pub fn temp_db_single_key() -> String {
        let db = temp_db();
        let conn = Connection::open(&db).unwrap();
        conn.execute("UPDATE api_configs SET extraApiKeys='' WHERE id=1", [])
            .unwrap();
        drop(conn);
        db
    }

    /// 构造带 api_configs/companions/memory_entries 的临时 SQLite 库，返回路径
    pub fn temp_db() -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "lianyu_native_test_{}_{}_{}.db",
            std::process::id(),
            rand_suffix(),
            COUNTER.fetch_add(1, Ordering::SeqCst),
        ));
        // 清理可能残留的旧文件（并行测试复用唯一路径）
        let _ = std::fs::remove_file(&path);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS api_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT, apiKey TEXT, extraApiKeys TEXT, baseUrl TEXT, model TEXT,
                temperature REAL, maxTokens INTEGER, isEnabled INTEGER, formatHint TEXT
            );
            INSERT INTO api_configs (provider, apiKey, extraApiKeys, baseUrl, model, temperature, maxTokens, isEnabled, formatHint)
            VALUES ('OPENAI', 'sk-test-main', 'sk-test-extra1,sk-test-extra2', 'https://api.openai.com/v1', 'gpt-4o-mini', 0.7, 800, 1, '');
            CREATE TABLE IF NOT EXISTS companions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, personality TEXT,
                backstory TEXT, speakingStyle TEXT, tags TEXT, rawPrompt TEXT, systemPrompt TEXT,
                intimacy INTEGER, createdAt INTEGER, updatedAt INTEGER
            );
            INSERT INTO companions (id, name, age, personality, backstory, speakingStyle, rawPrompt, systemPrompt, intimacy, createdAt, updatedAt)
            VALUES (1, '小恋', 22, '温柔体贴，爱撒娇', '咖啡馆主理人，喜欢猫', '软糯黏人', NULL, NULL, 0, 0, 0);
            CREATE TABLE IF NOT EXISTS memory_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, companionId INTEGER, content TEXT, category TEXT,
                importance INTEGER, context TEXT, accessCount INTEGER, timestamp INTEGER, lastAccessed INTEGER, deviceId TEXT
            );
            INSERT INTO memory_entries (companionId, content, category, importance, context, accessCount, timestamp, lastAccessed, deviceId)
            VALUES (1, '用户喜欢喝美式咖啡', 'preference', 5, '', 0, 0, 0, 'test-device');
            INSERT INTO memory_entries (companionId, content, category, importance, context, accessCount, timestamp, lastAccessed, deviceId)
            VALUES (1, '用户最近在准备考试', 'daily', 3, '', 0, 0, 0, 'test-device');
            ",
        )
        .unwrap();
        drop(conn);
        path.to_str().unwrap().to_string()
    }

    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{nanos}")
    }

    pub fn global_config(db: &str) -> AgentGlobalConfig {
        AgentGlobalConfig {
            db_path: db.to_string(),
            device_id: "test-device".to_string(),
            settings_json: r#"{"role": "GIRLFRIEND"}"#.to_string(),
            stickers: vec!["happy.png".to_string(), "shy.png".to_string()],
            credentials_json: "{}".to_string(),
            orchestrator: None,
        }
    }

    /// Mock HTTP 传输：依次弹出预置响应；流式时把 content 作为一次 delta 回调
    pub struct MockTransport {
        pub responses: Mutex<Vec<String>>,
        /// (url, headers, body)
        pub requests: Mutex<Vec<(String, Vec<(String, String)>, String)>>,
    }

    impl MockTransport {
        pub fn new(responses: Vec<&str>) -> Self {
            MockTransport {
                responses: Mutex::new(responses.into_iter().map(|s| s.to_string()).collect()),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl HttpTransport for MockTransport {
        fn post_json(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: &str,
            stream: bool,
            sink: Option<&dyn StreamSink>,
        ) -> Result<String, String> {
            self.requests
                .lock()
                .unwrap()
                .push((url.to_string(), headers.to_vec(), body.to_string()));
            let mut responses = self.responses.lock().unwrap();
            let resp = responses
                .first()
                .cloned()
                .ok_or_else(|| "MockTransport: 预置响应耗尽".to_string())?;
            if responses.len() > 1 {
                responses.remove(0);
            }
            if stream {
                if let Some(sink) = sink {
                    let v: Value = serde_json::from_str(&resp).unwrap_or(json!({}));
                    let content = v.get("content").and_then(|c| c.as_str()).unwrap_or("").to_string();
                    let finish = v
                        .get("finish_reason")
                        .and_then(|f| f.as_str())
                        .unwrap_or("stop")
                        .to_string();
                    if !content.is_empty() {
                        sink.on_text_delta(content.clone());
                    }
                    sink.on_done(content, finish);
                }
            }
            Ok(resp)
        }

        fn get_json(&self, url: &str, headers: &[(String, String)]) -> Result<String, String> {
            self.requests
                .lock()
                .unwrap()
                .push((url.to_string(), headers.to_vec(), String::new()));
            let mut responses = self.responses.lock().unwrap();
            let resp = responses
                .first()
                .cloned()
                .ok_or_else(|| "MockTransport: 预置响应耗尽".to_string())?;
            if responses.len() > 1 {
                responses.remove(0);
            }
            Ok(resp)
        }
    }

    /// 可编程结果传输：按序返回 `Ok(响应体)` / `Err(错误文本)`，用于重试与取消测试。
    ///
    /// 与 [MockTransport] 的区别：本传输能**返回 Err**（模拟 429/5xx/网络错误），
    /// 从而驱动 NativeGateway 的重试路径；序列只剩最后一项时重复返回（便于「持续失败」用例）。
    pub struct ScriptedResultTransport {
        pub results: Mutex<Vec<Result<String, String>>>,
        /// (url, headers, body)
        pub requests: Mutex<Vec<(String, Vec<(String, String)>, String)>>,
    }

    impl ScriptedResultTransport {
        pub fn new(results: Vec<Result<&str, &str>>) -> Self {
            ScriptedResultTransport {
                results: Mutex::new(
                    results
                        .into_iter()
                        .map(|r| r.map(|s| s.to_string()).map_err(|s| s.to_string()))
                        .collect(),
                ),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl HttpTransport for ScriptedResultTransport {
        fn post_json(
            &self,
            url: &str,
            headers: &[(String, String)],
            body: &str,
            stream: bool,
            sink: Option<&dyn StreamSink>,
        ) -> Result<String, String> {
            self.requests
                .lock()
                .unwrap()
                .push((url.to_string(), headers.to_vec(), body.to_string()));
            let mut results = self.results.lock().unwrap();
            if results.is_empty() {
                return Err("ScriptedResultTransport: 无预置结果".to_string());
            }
            let outcome = results[0].clone();
            if results.len() > 1 {
                results.remove(0);
            }
            if stream {
                if let (Ok(resp), Some(sink)) = (&outcome, sink) {
                    let v: Value = serde_json::from_str(resp).unwrap_or(json!({}));
                    let content = v.get("content").and_then(|c| c.as_str()).unwrap_or("").to_string();
                    let finish = v
                        .get("finish_reason")
                        .and_then(|f| f.as_str())
                        .unwrap_or("stop")
                        .to_string();
                    if !content.is_empty() {
                        sink.on_text_delta(content.clone());
                    }
                    sink.on_done(content, finish);
                }
            }
            outcome
        }

        fn get_json(&self, _url: &str, _headers: &[(String, String)]) -> Result<String, String> {
            Err("ScriptedResultTransport: get_json 未实现".to_string())
        }
    }
    #[test]
    fn partner_request_injects_device_signature_headers() {
        let mut config = testutil::global_config(&testutil::temp_db());
        config.credentials_json = r#"{"session":"sess-abc","client_id":"cli-123"}"#.into();
        let transport = Arc::new(MockTransport::new(vec!["{}"]));
        let gw = NativeGateway::with_transport_and_signer(
            config.clone(),
            transport.clone(),
            Some(Arc::new(FakeSigner)),
        );
        let row = ApiConfigRow {
            provider: "PARTNER".into(),
            api_key: String::new(),
            extra_api_keys: String::new(),
            base_url: "https://suflow.cloud/v1".into(),
            model: "m".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let body = serde_json::json!({"messages": []});
        let result = gw.http_post_json(&row, "", "/chat/completions", &body, false);
        assert!(result.is_ok(), "{:?}", result);
        let requests = transport.requests.lock().unwrap();
        let headers = &requests[0].1;
        assert!(headers.iter().any(|(k, v)| k == "X-LianYu-Sig-Version" && v == "v1"), "缺 Sig-Version: {headers:?}");
        assert!(headers.iter().any(|(k, v)| k == "X-LianYu-Sig" && v == "sig123"), "缺 Sig: {headers:?}");
        assert!(headers.iter().any(|(k, v)| k == "X-LianYu-Ts" && v == "1700000000"));
        assert!(headers.iter().any(|(k, v)| k == "X-LianYu-Key-Id" && v == "k"));
        assert!(headers.iter().any(|(k, v)| k == "X-LianYu-Client-Id" && v == "cli-123"), "PARTNER 会话头应保留");
    }

    /// PARTNER fail-closed：签名器缺失或未产出 Sig → 拒绝发送
    #[test]
    fn partner_request_fails_closed_without_signature() {
        struct EmptySigner;
        impl crate::agent::RequestSignatureProvider for EmptySigner {
            fn sign_headers(
                &self,
                _method: String,
                _path: String,
                _body: String,
                _client_id: String,
            ) -> Vec<crate::agent::RequestHeader> {
                vec![]
            }
        }
        let config = testutil::global_config(&testutil::temp_db());
        let transport = Arc::new(MockTransport::new(vec!["{}"]));
        let gw = NativeGateway::with_transport_and_signer(
            config.clone(),
            transport.clone(),
            Some(Arc::new(EmptySigner)),
        );
        let row = ApiConfigRow {
            provider: "PARTNER".into(),
            api_key: String::new(),
            extra_api_keys: String::new(),
            base_url: "https://suflow.cloud/v1".into(),
            model: "m".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let body = serde_json::json!({"messages": []});
        let result = gw.http_post_json(&row, "", "/chat/completions", &body, false);
        assert!(result.is_err(), "fail-closed 应拒绝无签名请求");
        assert!(transport.requests.lock().unwrap().is_empty(), "不应发出任何请求");
    }

    /// 非 PARTNER（自定义 API）不注入签名头（服务端仅验 apikey）
    #[test]
    fn custom_api_does_not_inject_signature_headers() {
        let config = testutil::global_config(&testutil::temp_db());
        let transport = Arc::new(MockTransport::new(vec!["{}"]));
        let gw = NativeGateway::with_transport(config.clone(), transport.clone());
        let row = ApiConfigRow {
            provider: "CUSTOM".into(),
            api_key: "sk-1".into(),
            extra_api_keys: String::new(),
            base_url: "https://custom.example/v1".into(),
            model: "m".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: String::new(),
        };
        let body = serde_json::json!({"messages": []});
        let result = gw.http_post_json(&row, "sk-1", "/chat/completions", &body, false);
        assert!(result.is_ok(), "{:?}", result);
        let headers = &transport.requests.lock().unwrap()[0].1;
        assert!(headers.iter().all(|(k, _)| !k.starts_with("X-LianYu-Sig")), "自定义路径不得签名: {headers:?}");
        assert!(headers.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-1"));
    }
}