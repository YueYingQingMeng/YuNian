// agent.rs — Agent 中间层核心（Rust）
//
// 职责：
// - AgentToolRegistry：内置基础聊天工具注册表（emit_segmented / send_sticker / emit_bubble）
// - AgentRuntime：全局 Agent 循环状态机（tool_choice required→auto、工具编排、确认策略）
// - LLM 调用（NativeGateway：Rust 直读 SQLite + 组装 system prompt + HTTP/SSE）
//   与工具副作用（ToolHost）通过 UniFFI foreign trait 回调 Kotlin
// - Kotlin 侧仅实现回调（ToolHost / StreamSink 消息落地适配），不承载 Agent 决策逻辑
//
// 与现有架构的关系（只读规划 Phase 2）：
// - Rust 侧等价物 = Kotlin AiToolLoopRunner + ToolRegistry + MessageSegmenter + AiService HTTP 层
// - Kotlin 现有代码不改动，Agent 层以独立模块接入

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::segmenter::{agent_segment, SplitMode};

// ── 回合级安全预算（P1-5，兜底；正常对话远达不到，行为无感知） ──

/// 单回合工具调用总数上限（超过即终止，防止工具失控循环）
const MAX_TOOL_CALLS_PER_TURN: u32 = 64;

/// 单回合气泡文本总字符上限（超过即终止，防止无限产出）
const MAX_BUBBLE_CHARS_PER_TURN: usize = 8000;

/// 连续「只产出气泡、没有正式正文」的轮数达到该值时，注入一条强制收尾提示。
///
/// 背景（Issue #2 症状 2）：模型每调一次气泡工具，工具就回灌「已输出气泡」，
/// 循环继续，**唯一停止条件是模型自己收手**（硬上限 64 轮 / 8000 字符）。
/// 连续多轮纯气泡是「越说越多、越说越跑偏」的典型前兆，因此在第 K 轮后
/// 主动提示模型用一条文字收尾，而不是让它一路跑到轮数上限。
const PURE_BUBBLE_ROUNDS_BEFORE_NUDGE: u32 = 3;

/// 气泡事件**唯一出口**：统一剥离思考块后再产出（见 `thinking.rs`）。
///
/// Issue #2 症状 1 的根治点：`emit_bubble` / `emit_segmented` / 正文补发 / 最终正文
/// 四条出口都必须经由本函数，禁止各自 `events.push`——否则「思考过程泄漏进回复气泡」
/// 会在任何一条新出口上重新出现。
///
/// 剥离后余文为空（整条都是思考）→ **不产出气泡**（不伪造内容），返回 `false`。
fn push_bubble(events: &mut Vec<AgentEvent>, text: &str) -> bool {
    let cleaned = crate::thinking::strip_thinking_blocks(text);
    if cleaned.is_empty() {
        return false;
    }
    events.push(AgentEvent {
        kind: "bubble".to_string(),
        text: cleaned,
        extra: String::new(),
    });
    true
}

/// 是否属于「气泡协议工具」（只负责把文本切成气泡输出，不推进任务本身）。
fn is_bubble_protocol_tool(name: &str) -> bool {
    matches!(name, "emit_bubble" | "emit_segmented")
}

/// 工具类别（用于确认策略等治理）
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ToolCategory {
    /// 通用工具（toolsets 为空时归属；不涉及确认/治理策略）
    General,
    /// 基础聊天工具（分段输出 / 表情包 / 气泡）
    Chat,
    /// 记忆召回
    Memory,
    /// 商业/支付类（需要用户确认）
    Commerce,
    /// 自定义/领域工具（toolsets 有未知归属，如 domain / skill）
    Custom,
}

/// 工具定义（Kotlin 侧传入的领域工具，执行时走 ToolHost 回调）
#[derive(Clone, Debug, uniffi::Record)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    /// JSON Schema 字符串
    pub parameters_json: String,
    pub category: ToolCategory,
    /// 工具归属的工具集（对齐 Hermes TOOLSETS 分组：如 ["chat"] / ["memory"] /
    /// ["commerce"] / ["domain"]）。空 = 归属「通用」工具集。
    pub toolsets: Vec<String>,
    /// 静态可用性（Kotlin 注册时携带；false = 不参与本轮工具列表）。
    /// 动态 check_fn（按环境实时判断）由 Kotlin AiTool.isAvailable() 承担。
    pub available: bool,
}

/// 工具执行上下文（替代 Kotlin argumentsForTool 特判）
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct ToolContext {
    pub companion_id: Option<i64>,
    pub group_id: Option<i64>,
    pub recent_history_summary: Option<String>,
    /// 可用表情包标签（Kotlin StickerManager 产物，热更新；
    /// builtin_send_sticker 用其做精确匹配校验，无匹配时生成报告）
    pub available_sticker_tags: Vec<String>,
}

/// 图片输入（视觉中间层）
///
/// 视觉链路（sendMessageWithImage）下沉 rs 后的输入载体：
/// - 优先使用 [path]（本地图片文件路径，零拷贝）；
/// - 无 path 时使用 [base64_data] + [mime_type]（base64 编码 + MIME 类型）。
/// Gateway 回调识别 image 字段后分派到视觉 API（KT 层仅做传输，决策在 Rust）。
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct ImageInput {
    /// 本地图片文件路径（优先）
    pub path: Option<String>,
    /// base64 编码数据（path 为空时使用）
    pub base64_data: Option<String>,
    /// MIME 类型（base64 模式必填，如 image/jpeg）
    pub mime_type: Option<String>,
}

/// Agent 单轮请求
#[derive(Clone, Debug, uniffi::Record)]
pub struct AgentTurnRequest {
    /// 当前伴侣 ID（方案 B：移入 run_turn / run_turn_stream 参数，不再随请求传输）
    pub group_id: Option<i64>,
    /// 序列化历史消息 JSON（Kotlin 侧组装，AiDialogueHistoryPolicy 产物）
    pub history_json: String,
    /// 会话级工具集（含内置聊天工具定义）
    pub tools: Vec<ToolDefinition>,
    pub max_rounds: u32,
    /// "required" / "auto"
    pub tool_choice: String,
    pub sticker_probability: u32,
    /// 视觉输入（可选）：非空时 gateway 走视觉链路（sendMessageWithImage）
    pub image: Option<ImageInput>,
    /// 自定义 system prompt（可选，群聊场景）：非空时 gateway 用此覆盖
    /// AiService 内部构建的 system prompt（走 sendMessageWithCustomSystem 通道）
    pub system_prompt: Option<String>,
    /// 角色 ID→名称 映射 JSON（群聊 companionNameMap 序列化，String map JSON）
    pub companion_name_map_json: Option<String>,
}

/// Agent 输出事件（消息层据此落地，Rust 不直接触达 DB/UI）
#[derive(Clone, Debug, uniffi::Record)]
pub struct AgentEvent {
    /// bubble / sticker / status / confirm_request
    pub kind: String,
    pub text: String,
    /// 附加数据（如 sticker 名称）
    pub extra: String,
}

/// Agent 回合结果
#[derive(Clone, Debug, uniffi::Record)]
pub struct AgentTurnResult {
    pub events: Vec<AgentEvent>,
    pub final_text: String,
    pub rounds_used: u32,
    /// completed / max_rounds / confirm_pending / state_stop / error
    pub finished_reason: String,
    pub error: Option<String>,
}

// ── 回合状态控制（编排方案 v3 决策 1：B 控制 + A 存储）──
//
// 设计：
// - B（控制）：TurnStateController 每轮 gateway.send 前回调 on_round，
//   由状态机决定本轮做什么：继续 / 停止 / 覆盖 tool_choice。
// - A（存储）：决策中的 status_text 以特殊 system 消息（[回合状态 N/M]）
//   内嵌进 messages，模型可见进度并自行收敛。
// - 默认状态机（DefaultTurnStateMachine）不主动 Stop（由 max_rounds 兜底），
//   只产出状态摘要，行为向后兼容。

/// 回合状态上下文（每轮 gateway.send 前由 Runner 组装，传给状态控制器）
#[derive(Clone, Debug, uniffi::Record)]
pub struct TurnStateContext {
    /// 当前轮次（1 起）
    pub round: u32,
    /// 最大轮数
    pub max_rounds: u32,
    /// 已执行的工具调用次数（累计）
    pub tool_calls_used: u32,
    /// 已产出文本总字符数（bubble 事件累计）
    pub text_chars_total: u32,
    /// 上一轮 finish_reason（首轮为空串）
    pub finished_reason: String,
    /// 最近工具结果摘要（截断，首轮为空串）
    pub last_tool_results: String,
}

/// 回合动作
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum TurnAction {
    /// 继续循环
    Continue,
    /// 立即停止（finished_reason = "state_stop"）
    Stop,
}

/// 回合状态决策（状态机每轮输出）
#[derive(Clone, Debug, uniffi::Record)]
pub struct TurnStateDecision {
    pub action: TurnAction,
    /// 状态摘要；非空且较上轮有变化时，以 system 消息内嵌 messages（方案 A 存储）
    pub status_text: String,
    /// 工具选择覆盖："required" / "auto" / ""（不覆盖）
    pub tool_choice_override: String,
}

/// 回合状态控制器（UniFFI foreign trait，Kotlin 可实现自定义状态机）
///
/// 决策规则默认在 Rust（DefaultTurnStateMachine）；Kotlin 仅在业务特殊场景
/// （如气泡连发的字数/条数收敛策略、群聊轮询节奏）提供自定义实现。
#[uniffi::export(with_foreign)]
pub trait TurnStateController: Send + Sync {
    fn on_round(&self, ctx: TurnStateContext) -> TurnStateDecision;
}

/// 默认回合状态机（Rust 内置，向后兼容）：
/// - 不主动 Stop（由 max_rounds 兜底，保持现有循环语义）
/// - 产出状态摘要（含工具调用次数与产出字数），**不暴露「N/上限」分母**
/// - 不覆盖 tool_choice
///
/// 分母脱敏的依据（Issue #2 症状 2）：旧文案 `[回合状态 3/64]` 让模型误以为
/// 「预算还很充足、应当继续输出」，是「越说越多、越说越跑偏」的直接推手。
/// 现在文案只描述**已产出的内容**，并明确「够了就自然收尾」。
#[derive(uniffi::Object)]
pub struct DefaultTurnStateMachine;

#[uniffi::export]
impl DefaultTurnStateMachine {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl TurnStateController for DefaultTurnStateMachine {
    fn on_round(&self, ctx: TurnStateContext) -> TurnStateDecision {
        let mut parts: Vec<String> = Vec::new();
        if ctx.tool_calls_used > 0 {
            parts.push(format!("已调用 {} 次工具", ctx.tool_calls_used));
        }
        if ctx.text_chars_total > 0 {
            parts.push(format!("已输出 {} 字", ctx.text_chars_total));
        }
        let detail = if parts.is_empty() {
            String::new()
        } else {
            format!("（{}）", parts.join("，"))
        };
        TurnStateDecision {
            action: TurnAction::Continue,
            // 不写轮次分母：只报「已产出什么」，并给出收尾指引。
            status_text: format!(
                "[回合状态{detail}] 内容已足够表达时请直接用一条文字自然收尾，不要为凑数继续输出"
            ),
            tool_choice_override: String::new(),
        }
    }
}

/// 流式输出回调（Kotlin 实现 → 现有流式消息管线）
///
/// 请求头键值对（签名回调产物）。
#[derive(Clone, Debug, Default, uniffi::Record)]
pub struct RequestHeader {
    pub name: String,
    pub value: String,
}

/// suflow.cloud（PARTNER）设备签名回调。
/// 设备私钥位于 Android Keystore（不可导出到 Rust），签名只能在 Kotlin 侧完成：
/// Rust 发送 PARTNER 请求前回调本接口注入 7 个 X-LianYu-* 签名头。
#[uniffi::export(with_foreign)]
pub trait RequestSignatureProvider: Send + Sync {
    /// 为 PARTNER 请求生成签名头；签名失败返回空列表（Rust 侧 fail-closed）。
    fn sign_headers(
        &self,
        method: String,
        path: String,
        body: String,
        client_id: String,
    ) -> Vec<RequestHeader>;
}

/// SSE 传输已下沉 Rust（NativeGateway::send_stream）；增量经本回调实时
/// 转发给 KT 消息管线落地（决策在 Rust，落地在 KT）。
#[uniffi::export(with_foreign)]
pub trait StreamSink: Send + Sync {
    /// 文本增量（正常对话内容）
    fn on_text_delta(&self, text: String);
    /// 思考增量（reasoning，若模型支持）
    fn on_reasoning_delta(&self, text: String);
    /// 完成（full_text 为完整文本，finish_reason 为 "stop"/"length" 等）
    fn on_done(&self, full_text: String, finish_reason: String);
    /// 出错（流式传输失败）
    fn on_error(&self, error: String);
}

/// 工具宿主回调（Kotlin 实现 → 工具副作用落地：发表情包/领域工具等）
/// 返回工具结果文本（回灌给模型，TOOL 角色）
#[uniffi::export(with_foreign)]
pub trait ToolHost: Send + Sync {
    fn execute(&self, tool_name: String, arguments_json: String, context_json: String) -> String;
}

/// Agent 工具注册表（内置基础聊天工具 + 全局注册工具 + 会话级领域工具）
pub struct AgentToolRegistry {
    /// 内置工具名 → 执行函数（纯 Rust，无副作用，仅产事件）
    builtin: HashMap<String, BuiltinTool>,
    /// 全局注册工具（Kotlin 启动时注册领域工具，跨会话存活；执行走 ToolHost）
    global_tools: Mutex<HashMap<String, ToolDefinition>>,
    /// 会话级工具（Kotlin 传入，执行走 ToolHost）
    session_tools: Mutex<HashMap<String, ToolDefinition>>,
}

type BuiltinTool = fn(&AgentToolRegistry, &ToolContext, &dyn ToolHost, &str, &mut Vec<AgentEvent>) -> String;

impl AgentToolRegistry {
    pub fn new() -> Self {
        let mut builtin = HashMap::new();
        builtin.insert("emit_segmented".to_string(), Self::builtin_emit_segmented as BuiltinTool);
        builtin.insert("send_sticker".to_string(), Self::builtin_send_sticker as BuiltinTool);
        builtin.insert("emit_bubble".to_string(), Self::builtin_emit_bubble as BuiltinTool);
        Self {
            builtin,
            global_tools: Mutex::new(HashMap::new()),
            session_tools: Mutex::new(HashMap::new()),
        }
    }

    /// 全局注册工具（Kotlin 启动时调用，替代 KT ToolRegistry 注册表）
    /// 幂等：同名工具重复注册覆盖定义
    pub fn register_global_tools(&self, tools: Vec<ToolDefinition>) {
        let mut map = self.global_tools.lock().unwrap();
        for t in tools {
            map.insert(t.name.clone(), t);
        }
    }

    /// 注销全局工具
    pub fn unregister_global_tool(&self, name: &str) {
        let mut map = self.global_tools.lock().unwrap();
        map.remove(name);
    }

    /// 全局工具定义列表（供 Kotlin 注入模型工具列表）
    pub fn global_tool_definitions(&self) -> Vec<ToolDefinition> {
        let map = self.global_tools.lock().unwrap();
        map.values().cloned().collect()
    }

    /// 注册会话级工具（Kotlin 在每轮请求前调用）
    pub fn register_session_tools(&self, tools: Vec<ToolDefinition>) {
        let mut map = self.session_tools.lock().unwrap();
        map.clear();
        for t in tools {
            map.insert(t.name.clone(), t);
        }
    }

    /// 内置基础聊天工具定义（供 Kotlin 注入模型工具列表）
    pub fn builtin_tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![
            ToolDefinition {
                name: "emit_segmented".to_string(),
                description: "将一段较长的回复按标点分段输出为多条短气泡，模拟真人连发。触发条件：当回复包含多句话、需要拆成多条气泡逐条发送时使用。约束：短回复请直接用 emit_bubble；本工具只用于文本分段，不发送表情包。".to_string(),
                parameters_json: r#"{"type":"object","properties":{"text":{"type":"string","minLength":1,"description":"要分段的完整文本，至少 1 个字符"},"mode":{"type":"string","enum":["simple","group"],"description":"分段模式：simple=按语义分块，group=逐句拆分。默认 simple"}},"required":["text"],"additionalProperties":false}"#.to_string(),
                category: ToolCategory::Chat,
                toolsets: vec!["chat".to_string()],
                available: true,
            },
            ToolDefinition {
                name: "send_sticker".to_string(),
                description: "发送一个表情包，用图片表达当前情绪。触发条件：当语境需要情感表达（开心、难过、撒娇、生气、抱抱等）或用户明确要求发送表情包或图片时使用。约束：tags 必须为 1~3 个简短情绪标签，且必须从系统提示给出的可用标签中选择（如\"开心\"\"委屈巴巴\"\"抱抱\"），禁止标点、禁止完整句子、禁止编造不在可用标签中的词；一次调用只发 1 个表情包，多个情绪可连续多次调用；表情包是点缀，不能替代文字气泡。返回值会反馈实际发送的表情包名称与标签，无需再次描述图片内容。".to_string(),
                parameters_json: r#"{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string","maxLength":20},"minItems":1,"maxItems":3,"description":"1~3 个表情包标签，必须从系统提示给出的可用标签中选择，按相关性从高到低排列"}},"required":["tags"],"additionalProperties":false}"#.to_string(),
                category: ToolCategory::Chat,
                toolsets: vec!["chat".to_string()],
                available: true,
            },
            ToolDefinition {
                name: "emit_bubble".to_string(),
                description: "输出一条独立气泡消息。触发条件：需要以单条气泡发送一句完整的话时使用；需要多条时多次调用。约束：内容超过一句或多句连发请用 emit_segmented 分段。".to_string(),
                parameters_json: r#"{"type":"object","properties":{"text":{"type":"string","minLength":1,"description":"气泡文本内容，至少 1 个字符"}},"required":["text"],"additionalProperties":false}"#.to_string(),
                category: ToolCategory::Chat,
                toolsets: vec!["chat".to_string()],
                available: true,
            },
        ]
    }

    // ── 内置工具实现 ──

    fn builtin_emit_segmented(
        _registry: &AgentToolRegistry,
        _ctx: &ToolContext,
        _tool_host: &dyn ToolHost,
        arguments: &str,
        events: &mut Vec<AgentEvent>,
    ) -> String {
        let parsed: serde_json::Value = serde_json::from_str(arguments).unwrap_or(serde_json::Value::Null);
        let text = parsed.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let mode = parsed
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("simple");
        let split_mode = if mode == "group" { SplitMode::Group } else { SplitMode::Simple };
        // 先剥思考块再分段：否则同一段思考会被拆进多条气泡（见 thinking.rs）
        let cleaned = crate::thinking::strip_thinking_blocks(&text);
        let segments = agent_segment(cleaned.clone(), split_mode);
        let mut emitted = 0usize;
        for seg in segments {
            if push_bubble(events, &seg) {
                emitted += 1;
            }
        }
        if emitted == 0 {
            return "错误：气泡内容为空（思考内容不得作为气泡输出，请把正式回复写进 text）".to_string();
        }
        format!("已将回复分段输出为 {emitted} 条气泡")
    }

    fn builtin_send_sticker(
        _registry: &AgentToolRegistry,
        ctx: &ToolContext,
        tool_host: &dyn ToolHost,
        arguments: &str,
        events: &mut Vec<AgentEvent>,
    ) -> String {
        let parsed: serde_json::Value = match serde_json::from_str(arguments) {
            Ok(v) => v,
            Err(_) => return "错误：表情包参数必须是合法 JSON".to_string(),
        };
        // ① 解析 tags 数组（1~3 个），逐个 normalize
        let raw_tags: Vec<String> = parsed
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|t| t.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let tags: Vec<String> = raw_tags
            .iter()
            .map(|t| Self::clean_sticker_description(t))
            .filter(|t| !t.is_empty())
            .collect();
        if tags.is_empty() {
            return "错误：tags 缺失或清洗后为空（需为 1~3 个情绪标签，如：开心、委屈巴巴、抱抱）".to_string();
        }
        if tags.len() > 3 {
            return "错误：tags 最多 3 个，请按相关性从高到低选择 1~3 个".to_string();
        }
        // ② 逐个精确匹配可用标签（可用标签 = Kotlin StickerManager 注入的 available_sticker_tags）
        let available: Vec<String> = ctx
            .available_sticker_tags
            .iter()
            .map(|t| Self::clean_sticker_description(t))
            .filter(|t| !t.is_empty())
            .collect();
        // ③ 发送表情包（返回值规范 v2）：命中后经 ToolHost 内部回调 `sticker_pick`
        //    让 Kotlin 按标签预选实际表情包（偏好引擎采样 → DB 条目），工具结果回灌
        //    **实际发送的表情包名称**（fileName/description），模型可感知发送了哪一张；
        //    事件 extra 携带 entry_id → Kotlin 落地时按 id 直接落地（避免二次采样选不同图）。
        //    预选失败（Kotlin 未实现 / 无候选）→ 回退事件透传，由 Kotlin 落地时兜底采样。
        // allow_passthrough：预选失败时是否仍透传 sticker 事件（旧行为）。
        // 库为空（available 为空）时必须为 false——否则 Kotlin 落地时会走
        // 「按描述模糊匹配内置表情」的兜底，于是恒发同一张默认图（真机问题：空库发刷子表情）。
        let mut pick_and_emit = |pick_tags: &[String], fallback_text: &str, allow_passthrough: bool| -> String {
            match Self::sticker_pick(tool_host, pick_tags, ctx) {
                Some(pick) => {
                    let entry_id = pick.get("entryId").and_then(|v| v.as_u64());
                    let file_name = pick
                        .get("fileName")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let description = pick
                        .get("description")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("")
                        .to_string();
                    events.push(AgentEvent {
                        kind: "sticker".to_string(),
                        text: pick_tags.join(","),
                        extra: match entry_id {
                            Some(id) => format!("entry_id={id};file_name={file_name}"),
                            None => format!("file_name={file_name}"),
                        },
                    });
                    let display = if !description.is_empty() {
                        description
                    } else if !file_name.is_empty() {
                        file_name
                    } else {
                        pick_tags.join(",")
                    };
                    format!("已发送表情包：{display}（标签：{}）", pick_tags.join(","))
                }
                None => {
                    if !allow_passthrough {
                        // 表情库为空且预选不到：绝不放行「默认表情包」，
                        // 直接让模型改用文字表达（用户可见且不会误发内置图）。
                        return "当前没有任何可用表情包（表情库为空），请不要发送表情包，改用文字气泡表达情绪。"
                            .to_string();
                    }
                    // 旧行为：事件透传（text=命中标签），Kotlin 落地时按标签采样兜底
                    events.push(AgentEvent {
                        kind: "sticker".to_string(),
                        text: fallback_text.to_string(),
                        extra: format!("companion_id={:?};group_id={:?}", ctx.companion_id, ctx.group_id),
                    });
                    format!("已发送表情包：{fallback_text}")
                }
            }
        };
        // ④ 可用标签为空（表情库为空）→ 只尝试预选；预选不到就明确告知模型「没有表情包」，
        //    不允许回退成「默认表情包」（见 pick_and_emit 的 allow_passthrough 说明）。
        if available.is_empty() {
            return pick_and_emit(&tags, &tags.join(","), false);
        }
        let hits: Vec<String> = tags.iter().filter(|t| available.contains(t)).cloned().collect();
        // ⑤ 有任一命中 → push sticker 事件 + 返回实际发送信息（多 tag → Kotlin 引擎加权随机选一张）
        if !hits.is_empty() {
            return pick_and_emit(&hits, &hits.join(","), true);
        }
        // ⑥ 全部未命中 → 不输出 sticker 事件，返回无匹配报告（含可用标签 Top-30）
        let mut sorted = available.clone();
        sorted.sort();
        sorted.dedup();
        let top30: Vec<String> = sorted.iter().take(30).cloned().collect();
        let available_str = if top30.is_empty() {
            String::new()
        } else {
            format!("。可用标签: [{}]（Top-30）", top30.join(", "))
        };
        format!("tags '{}' 无匹配{available_str}，请从可用标签中选择或改用文字气泡", tags.join(","))
    }

    /// 内部回调：让 Kotlin 按标签预选表情包（`ToolHost.execute("sticker_pick", ...)`）。
    ///
    /// 返回实际选中条目信息 JSON（entryId/fileName/description）——工具结果据此回灌
    /// 实际发送的表情包信息，模型可感知发送了哪一张。Kotlin 未实现 / 无候选 / 解析失败
    /// → None（调用方回退旧行为：事件透传 + Kotlin 落地时兜底采样）。
    fn sticker_pick(
        tool_host: &dyn ToolHost,
        tags: &[String],
        ctx: &ToolContext,
    ) -> Option<serde_json::Value> {
        let args = serde_json::json!({ "tags": tags }).to_string();
        let ctx_json = serde_json::json!({
            "companion_id": ctx.companion_id,
            "group_id": ctx.group_id,
        })
        .to_string();
        let raw = tool_host.execute("sticker_pick".to_string(), args, ctx_json);
        let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
        if !v.get("ok").and_then(|o| o.as_bool()).unwrap_or(false) {
            return None;
        }
        if v.get("entryId").and_then(|e| e.as_u64()).is_none() {
            return None;
        }
        Some(v)
    }

    /// 清洗表情包标签：去空白 + 去全部标点 + 截断 20 字符 + 统一小写
    /// （对齐 sticker_preference::normalize_tag，保证 builtin 校验与引擎匹配口径一致）。
    /// 标签会被 Kotlin 侧用于引擎匹配表情包资源，因此必须是无标点的简短情绪词。
    fn clean_sticker_description(raw: &str) -> String {
        const MAX_LEN: usize = 20;
        raw.chars()
            .filter(|c| {
                !c.is_whitespace()
                    && !matches!(
                        c,
                        '，' | '。' | '！' | '？' | '、' | '；' | '：' | '“' | '”' | '‘' | '’'
                            | '（' | '）' | '【' | '】' | '《' | '》' | '…' | '～' | '~' | '⋯'
                            | ',' | '.' | '!' | '?' | ';' | ':' | '(' | ')' | '[' | ']' | '{'
                            | '}' | '"' | '\'' | '`' | '-' | '_' | '*' | '#' | '+' | '=' | '>'
                            | '<' | '|' | '\\' | '/'
                    )
            })
            .take(MAX_LEN)
            .flat_map(char::to_lowercase)
            .collect()
    }

    fn builtin_emit_bubble(
        _registry: &AgentToolRegistry,
        _ctx: &ToolContext,
        _tool_host: &dyn ToolHost,
        arguments: &str,
        events: &mut Vec<AgentEvent>,
    ) -> String {
        let parsed: serde_json::Value = serde_json::from_str(arguments).unwrap_or(serde_json::Value::Null);
        let text = parsed.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if text.is_empty() {
            return "错误：缺少 text 参数".to_string();
        }
        // 唯一出口：剥离思考块。整条都是思考 → 如实报错并给出协议指引（不得静默产出）
        if !push_bubble(events, &text) {
            return "错误：气泡内容为空（思考内容不得作为气泡输出，请把正式回复写进 text）".to_string();
        }
        "已输出气泡".to_string()
    }

    /// 执行工具：先查内置，再查全局注册，最后查会话级（均走 ToolHost）
    fn execute_tool(
        &self,
        ctx: &ToolContext,
        tool_host: &dyn ToolHost,
        name: &str,
        arguments: &str,
        events: &mut Vec<AgentEvent>,
    ) -> String {
        if let Some(builtin) = self.builtin.get(name) {
            return builtin(self, ctx, tool_host, arguments, events);
        }
        {
            let global = self.global_tools.lock().unwrap();
            if global.get(name).is_some_and(|tool| tool.available) {
                drop(global);
                let ctx_json = serde_json::json!({
                    "companion_id": ctx.companion_id,
                    "group_id": ctx.group_id,
                    "recent_history_summary": ctx.recent_history_summary,
                })
                .to_string();
                return tool_host.execute(name.to_string(), arguments.to_string(), ctx_json);
            }
        }
        let session = self.session_tools.lock().unwrap();
        if session.get(name).is_some_and(|tool| tool.available) {
            drop(session);
            let ctx_json = serde_json::json!({
                "companion_id": ctx.companion_id,
                "group_id": ctx.group_id,
                "recent_history_summary": ctx.recent_history_summary,
            })
            .to_string();
            return tool_host.execute(name.to_string(), arguments.to_string(), ctx_json);
        }
        format!("错误：未注册的工具 {name}")
    }
}

impl Default for AgentToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 确认策略
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ConfirmPolicy {
    /// 不拦截
    None,
    /// 需要用户确认
    RequireConfirm,
}

/// 全局运行器（方案 B：全局状态驻留 Rust）
///
/// App 启动时创建一次，跨所有调用复用：
/// - 持有全局配置（db_path / device_id 固定，settings / stickers / credentials 可热更新）；
/// - 持有全局工具注册表（register_global_tools 替代 KT ToolRegistry）；
/// - 每次 run_turn / run_turn_stream 只传调用级参数（request + companion_id + tool_host / sink），
///   内部从当前配置快照构造 NativeGateway（Rust 直读 SQLite + 组装 system prompt + HTTP）。
#[derive(uniffi::Object)]
pub struct AgentRuntime {
    /// Room 数据库文件路径（固定）
    db_path: String,
    /// 设备 ID（固定）
    device_id: String,
    /// 全局可变配置（settings / stickers / credentials，update_* 热更新）
    mutable: Mutex<RuntimeMutable>,
    /// 全局工具注册表（内置聊天工具 + 全局注册领域工具 + 会话级工具）
    registry: Arc<AgentToolRegistry>,
    /// 提示词编排器（Kotlin 注入；None 时 system prompt 完全由 Kotlin 提供）
    orchestrator: Option<Arc<crate::prompt_orchestrator::PromptOrchestrator>>,
    /// 回合级互斥（P3-12：同一时刻只允许一个回合，聊天/群聊/后台回复串行）
    turn_lock: Mutex<()>,
    /// 核心插件底座（方案 A：cordis-rs 宿主，懒创建；失败不致命）
    core_plugins: std::sync::OnceLock<std::result::Result<Arc<crate::cordis_bridge::CorePluginHost>, ()>>,
    /// 已批准的一次性工具调用（Approval：confirm_pending 后由用户确认；(name, args) 精确匹配）
    approved_tools: Mutex<std::collections::HashSet<(String, String)>>,
    /// 已拒绝的工具调用（回灌「用户拒绝」而不执行，也不再次触发确认）
    rejected_tools: Mutex<std::collections::HashSet<(String, String)>>,
    /// Eval：脚本化传输（set_mock_transport 注入；Some 时 gateway() 走离线 mock，不触网）
    mock_transport: std::sync::Mutex<Option<std::sync::Arc<crate::native_gateway::ScriptedTransport>>>,
    /// suflow.cloud 设备签名回调（PARTNER 通道；Kotlin 注入）
    signature_provider: std::sync::Mutex<Option<std::sync::Arc<dyn RequestSignatureProvider>>>,
    /// 世界书（聊天陪伴：社区 World Info 规范；set_worldbook 注入，None = 未启用）
    worldbook: std::sync::Mutex<Option<Arc<crate::lorebook::Lorebook>>>,
    /// 回合取消标志：跨 FFI 可被 Kotlin 置位（cancel_current_turn），
    /// 供 NativeGateway 的 LLM 自动重试循环中断；每回合开始时自动复位。
    turn_cancel: Arc<AtomicBool>,
}

/// Install a process-wide panic witness once. The hook remains best-effort and must not panic:
/// panic="abort" still runs it before abort, preserving diagnostics even when vendor tombstones
/// are truncated. Sensitive values are redacted before writing.
fn install_panic_hook(db_path: &str) {
    static PANIC_HOOK_PATH: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    let hook_path = std::path::Path::new(db_path).with_file_name("rust_panic_last.txt");
    if PANIC_HOOK_PATH.set(hook_path.clone()).is_err() { return; }
    std::panic::set_hook(Box::new(move |info| {
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        };
        let loc = info.location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let thread = std::thread::current().name().unwrap_or("<unnamed>").to_string();
        let backtrace = std::backtrace::Backtrace::force_capture().to_string();
        let body = format!(
            "=== YuNian Rust Panic ===\nthread: {}\nmessage: {}\nlocation: {}\n--- backtrace (force) ---\n{}\n=== end ===\n",
            crate::agentlog::redact(&thread), crate::agentlog::redact(&msg),
            crate::agentlog::redact(&loc), crate::agentlog::redact(&backtrace),
        );
        let _ = std::fs::write(&hook_path, body.as_bytes());
    }));
}

/// AgentRuntime 的全局可变配置（Mutex 保护，支持 update_* 热更新）
struct RuntimeMutable {
    settings_json: String,
    stickers: Vec<String>,
    credentials_json: String,
}

#[uniffi::export]
impl AgentRuntime {
    /// 创建全局运行器（一次传入全局配置；内置基础聊天工具注册表）
    #[uniffi::constructor]
    pub fn new(config: crate::native_gateway::AgentGlobalConfig) -> Arc<AgentRuntime> {
        install_panic_hook(&config.db_path);
        // Android does not reliably expose native stderr in logcat, so persist key Agent
        // diagnostics beside the Room database for the Kotlin incremental bridge.
        crate::agentlog::init(&config.db_path);

        Arc::new(AgentRuntime {
            db_path: config.db_path,
            device_id: config.device_id,
            mutable: Mutex::new(RuntimeMutable {
                settings_json: config.settings_json,
                stickers: config.stickers,
                credentials_json: config.credentials_json,
            }),
            registry: Arc::new(AgentToolRegistry::new()),
            orchestrator: config.orchestrator,
            turn_lock: Mutex::new(()),
            core_plugins: std::sync::OnceLock::new(),
            approved_tools: Mutex::new(std::collections::HashSet::new()),
            rejected_tools: Mutex::new(std::collections::HashSet::new()),
            mock_transport: std::sync::Mutex::new(None),
            signature_provider: std::sync::Mutex::new(None),
            worldbook: std::sync::Mutex::new(None),
            turn_cancel: Arc::new(AtomicBool::new(false)),
        })
    }

    /// 热更新全局设置 JSON（inner_thought / ntp_time / role / yandere 等）
    pub fn update_settings(&self, settings_json: String) {
        self.mutable.lock().unwrap().settings_json = settings_json;
    }

    /// 热更新可用表情包标签列表（Top-30；StickerManager 增删/偏好变化后调用。
    /// 供 builtin_send_sticker 精确匹配校验与无匹配报告展示）
    pub fn update_stickers(&self, stickers: Vec<String>) {
        self.mutable.lock().unwrap().stickers = stickers;
    }

    /// 热更新 API 凭证覆盖 JSON（PARTNER session / client_id 刷新后调用）
    pub fn update_credentials(&self, credentials_json: String) {
        self.mutable.lock().unwrap().credentials_json = credentials_json;
    }

    /// 运行一轮 Agent 回合（标准循环：required→auto，默认状态机）
    ///
    /// 流程：
    /// 1. 注册会话级工具
    /// 2. 逐轮调用原生网关 NativeGateway（Rust 直读 SQLite + 组装 system prompt
    ///    + HTTP 请求，替代 Kotlin AgentModelGateway / AiService HTTP 层）
    /// 3. 解析响应：tool_calls → 执行工具 → 产出事件 → 结果回灌（TOOL 角色）
    ///    正常文本 → 产出 bubble 事件 → 结束
    /// 4. 超过 max_rounds → 返回 max_rounds
    pub fn run_turn(
        &self,
        request: AgentTurnRequest,
        companion_id: Option<i64>,
        tool_host: Arc<dyn ToolHost>,
    ) -> AgentTurnResult {
        let gw = self.gateway();
        self.run_turn_inner(
            &request,
            companion_id,
            &gw,
            None,
            tool_host.as_ref(),
            &DefaultTurnStateMachine,
        )
    }

    /// 使用本回合独立的凭证快照，避免不同会话覆盖全局凭证后串台。
    pub fn run_turn_with_credentials(
        &self,
        request: AgentTurnRequest,
        companion_id: Option<i64>,
        tool_host: Arc<dyn ToolHost>,
        credentials_json: String,
    ) -> AgentTurnResult {
        let gw = self.gateway_with_credentials(Some(credentials_json));
        self.run_turn_inner(
            &request,
            companion_id,
            &gw,
            None,
            tool_host.as_ref(),
            &DefaultTurnStateMachine,
        )
    }

    /// 使用自定义状态控制器运行（编排方案 v3 决策 1：B 控制 + A 存储）
    ///
    /// 每轮 gateway.send 前调用 controller.on_round：
    /// - action == Stop → 立即停止（finished_reason = "state_stop"）
    /// - status_text 非空且较上轮有变化 → 以 system 消息内嵌 messages
    /// - tool_choice_override 非空 → 覆盖本轮 tool_choice
    pub fn run_turn_with_controller(
        &self,
        request: AgentTurnRequest,
        companion_id: Option<i64>,
        tool_host: Arc<dyn ToolHost>,
        controller: Arc<dyn TurnStateController>,
    ) -> AgentTurnResult {
        let gw = self.gateway();
        self.run_turn_inner(
            &request,
            companion_id,
            &gw,
            None,
            tool_host.as_ref(),
            controller.as_ref(),
        )
    }

    /// 核心插件宿主快照（审计/调试/设置页；宿主未初始化返回空对象）。
    /// 方案 A：cordis-rs 底座可见性（UniFFI 导出，Kotlin AgentFacade 透传）。
    pub fn core_plugin_snapshot(&self) -> String {
        self.ensure_core_plugins()
            .map(|h| h.snapshot_json())
            .unwrap_or_else(|| "{\"core_plugins\":\"unavailable\"}".to_string())
    }

    /// 批准一次待确认的 Commerce 工具调用（Approval）。
    /// 名称+参数完全匹配的一次性授权：下次回合中匹配的调用直接执行（使用后失效）。
    pub fn approve_tool(&self, name: String, args: String) {
        self.approved_tools.lock().unwrap().insert((name, args));
    }

    /// 拒绝一次待确认的 Commerce 工具调用（Approval）。
    /// 后续匹配调用回灌「用户拒绝」而不执行、也不再触发确认。
    pub fn reject_tool(&self, name: String, args: String) {
        self.rejected_tools.lock().unwrap().insert((name, args));
    }

    /// 请求取消当前正在进行中的回合（可从任意线程调用）。
    ///
    /// 语义：置位共享取消标志 → NativeGateway 的 LLM 自动重试循环在
    /// 「下一次尝试前 / 退避等待后」立即停止重试并以「请求已取消」结束，
    /// 不再对上游发起新的请求。每回合开始时自动复位（见 `run_turn_inner`）。
    ///
    /// 用途：用户停止生成、或 WorkManager 停止 worker 时，避免有界重试把
    /// 回合继续拖长（对应「调用方取消/中断时不得继续重试」）。
    pub fn cancel_current_turn(&self) {
        self.turn_cancel.store(true, Ordering::SeqCst);
    }

    /// Eval：注入脚本化传输（离线 mock，按序弹出预置响应，不发真实网络）。
    /// 空列表 = 清空（恢复默认 ureq 传输）。
    pub fn set_mock_transport(&self, responses: Vec<String>) {
        if responses.is_empty() {
            *self.mock_transport.lock().unwrap() = None;
        } else {
            *self.mock_transport.lock().unwrap() =
                Some(std::sync::Arc::new(crate::native_gateway::ScriptedTransport::new(responses)));
        }
    }

    /// 注入 suflow.cloud 设备签名回调（Kotlin 实现；None 清除，PARTNER 请求将 fail-closed）。
    pub fn set_signature_provider(&self, provider: Option<Arc<dyn RequestSignatureProvider>>) {
        *self.signature_provider.lock().unwrap() = provider;
    }

    /// 注入世界书 JSON（社区 World Info 格式；None/空串 = 清除，回合组装时按需扫描注入）。
    pub fn set_worldbook(&self, json: Option<String>) {
        *self.worldbook.lock().unwrap() = json.and_then(|j| {
            if j.trim().is_empty() {
                None
            } else {
                match crate::lorebook::Lorebook::parse(&j) {
                    Ok(lb) => Some(Arc::new(lb)),
                    Err(e) => {
                        crate::agentlog::warn("agent", &format!("worldbook parse failed: {e}"));
                        None
                    }
                }
            }
        });
    }

    /// 流式运行一轮 Agent 回合（SSE 流式输出完全下沉 rs）
    ///
    /// 与 run_turn 相同决策循环；每轮经 NativeGateway::send_stream 直连
    /// LLM 的 SSE 流，增量文本经 [StreamSink] 实时回调给 KT 消息管线
    /// （打字机效果）；返回与 run_turn 同构的 AgentTurnResult。
    pub fn run_turn_stream(
        &self,
        request: AgentTurnRequest,
        companion_id: Option<i64>,
        tool_host: Arc<dyn ToolHost>,
        sink: Arc<dyn StreamSink>,
    ) -> AgentTurnResult {
        let gw = self.gateway();
        self.run_turn_inner(
            &request,
            companion_id,
            &gw,
            Some(sink),
            tool_host.as_ref(),
            &DefaultTurnStateMachine,
        )
    }

    /// 流式回合的独立凭证入口；凭证只属于本次网关，不写入全局运行时。
    pub fn run_turn_stream_with_credentials(
        &self,
        request: AgentTurnRequest,
        companion_id: Option<i64>,
        tool_host: Arc<dyn ToolHost>,
        sink: Arc<dyn StreamSink>,
        credentials_json: String,
    ) -> AgentTurnResult {
        let gw = self.gateway_with_credentials(Some(credentials_json));
        self.run_turn_inner(
            &request,
            companion_id,
            &gw,
            Some(sink),
            tool_host.as_ref(),
            &DefaultTurnStateMachine,
        )
    }

    // ── 全局工具注册表（下沉 rs，替代 KT ToolRegistry）──

    /// 全局注册工具（Kotlin 启动时调用）
    pub fn register_global_tools(&self, tools: Vec<ToolDefinition>) {
        self.registry.register_global_tools(tools);
    }

    /// 注销全局工具
    pub fn unregister_global_tool(&self, name: String) {
        self.registry.unregister_global_tool(&name);
    }

    /// 全局工具定义列表
    pub fn global_tool_definitions(&self) -> Vec<ToolDefinition> {
        self.registry.global_tool_definitions()
    }
}

impl AgentRuntime {
    /// 从当前全局配置快照构造 NativeGateway（默认 ureq 传输；Eval 注入 mock 时离线）
    fn gateway(&self) -> crate::native_gateway::NativeGateway {
        self.gateway_with_credentials(None)
    }

    fn gateway_with_credentials(&self, credentials_json: Option<String>) -> crate::native_gateway::NativeGateway {
        let m = self.mutable.lock().unwrap();
        let cfg = crate::native_gateway::AgentGlobalConfig {
            db_path: self.db_path.clone(),
            device_id: self.device_id.clone(),
            settings_json: m.settings_json.clone(),
            stickers: m.stickers.clone(),
            credentials_json: credentials_json.unwrap_or_else(|| m.credentials_json.clone()),
            orchestrator: self.orchestrator.clone(),
        };
        let mock = self.mock_transport.lock().unwrap().clone();
        let signer = self.signature_provider.lock().unwrap().clone();
        drop(m);
        let gw = if let Some(transport) = mock {
            crate::native_gateway::NativeGateway::with_transport_and_signer(cfg, transport, signer)
        } else {
            crate::native_gateway::NativeGateway::with_signer(cfg, signer)
        };
        // 注入回合取消标志：LLM 自动重试循环据此在取消后停止重试
        gw.with_cancel(self.turn_cancel.clone())
    }

    /// 组装编排器选项：静态元数据（device_id / timezone / session_id / owner_name）
    /// + 群聊成员映射（request）+ 短记忆上限（settings_json，默认 200）
    /// 读取热更新 settings_json 中的字符串项（提示词组装期使用）。
    ///
    /// 用途：Kotlin 下发、随设置变化的文本（如生图协议）——提示词本体已由 Rust 组装。
    fn setting_str(&self, key: &str) -> Option<String> {
        let m = self.mutable.lock().unwrap();
        serde_json::from_str::<serde_json::Value>(&m.settings_json)
            .ok()
            .and_then(|v| v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string()))
    }

    fn orchestration_options(
        &self,
        request: &AgentTurnRequest,
    ) -> crate::prompt_orchestrator::PromptOrchestratorOptions {
        use crate::prompt_orchestrator::PromptOrchestratorOptions;
        let m = self.mutable.lock().unwrap();
        let settings =
            serde_json::from_str::<serde_json::Value>(&m.settings_json).unwrap_or(serde_json::json!({}));
        let str_opt = |k: &str| settings.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
        PromptOrchestratorOptions {
            device_id: self.device_id.clone(),
            timezone: str_opt("timezone").unwrap_or_default(),
            session_id: str_opt("session_id"),
            owner_name: str_opt("owner_name"),
            companion_name_map_json: request.companion_name_map_json.clone(),
            working_memory_limit: settings
                .get("working_memory_limit")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(200),
            // 汇合点：技能索引按 available_tools 过滤显示——本轮实际注入模型的
            // 工具名集合（builtin 聊天工具 + 全局工具 + 会话工具），与 tool_defs 过滤逻辑一致
            available_tools: self.available_tool_names(request),
            ..PromptOrchestratorOptions::default()
        }
    }

    /// 本轮实际注入模型的工具名集合（与 run_turn_inner 中 tool_defs 的过滤逻辑一致：
    /// available=true 的工具才会被模型看到，技能目录按此集合过滤显示；空 = 不过滤）
    fn available_tool_names(&self, request: &AgentTurnRequest) -> Vec<String> {
        self.available_tool_definitions(request)
            .into_iter()
            .map(|t| t.name)
            .collect()
    }

    /// 工具定义、确认类别与执行路由共用同一优先级，避免同名注册使声明和执行错位。
    fn available_tool_definitions(&self, request: &AgentTurnRequest) -> Vec<ToolDefinition> {
        let mut seen = std::collections::HashSet::new();
        self.registry
            .builtin_tool_definitions()
            .into_iter()
            .chain(self.registry.global_tool_definitions())
            .chain(request.tools.iter().cloned())
            .chain(self.core_plugin_tools())
            .filter(|t| t.available && seen.insert(t.name.clone()))
            .collect()
    }

    fn run_turn_inner(
        &self,
        request: &AgentTurnRequest,
        companion_id: Option<i64>,
        native: &crate::native_gateway::NativeGateway,
        stream_sink: Option<Arc<dyn StreamSink>>,
        tool_host: &dyn ToolHost,
        controller: &dyn TurnStateController,
    ) -> AgentTurnResult {
        // 回合级互斥（P3-12）：聊天 / 群聊 / 后台回复串行执行，避免并发放大上游请求。
        // 工具回调（ToolHost → Kotlin）不会重入 run_turn_inner，无死锁风险。
        let _turn_guard = self.turn_lock.lock().unwrap();

        // 每回合开始复位取消标志（上一回合若被取消，不得影响本回合）
        self.turn_cancel.store(false, Ordering::SeqCst);

        // 方案 A：cordis-rs 核心插件底座——回合生命周期事件（start/end）。
        // RAII 作用域保证 end 事件在任意返回路径上都发出；宿主失败不致命。
        // Scope ①：scope 标签 = companion:{id} / group:{gid} / global，随事件参数下发。
        let _turn_scope = TurnScope::begin(self.ensure_core_plugins(), &self.turn_scope_tag(companion_id, request.group_id));

        self.registry.register_session_tools(request.tools.clone());

        // 可用表情包标签快照（Kotlin 经 update_stickers 热更新注入；builtin_send_sticker
        // 用它做精确匹配校验 + 无匹配报告 Top-30 展示）
        let available_sticker_tags = self.mutable.lock().unwrap().stickers.clone();
        let ctx = ToolContext {
            companion_id,
            group_id: request.group_id,
            recent_history_summary: None,
            available_sticker_tags,
        };

        let mut events: Vec<AgentEvent> = Vec::new();
        let mut messages: Vec<serde_json::Value> = Vec::new();
        // history_json 已含 system + 历史对话（Kotlin 侧 AiDialogueHistoryPolicy 产物）
        if !request.history_json.trim().is_empty() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&request.history_json) {
                if let Some(arr) = v.as_array() {
                    messages.extend(arr.iter().cloned());
                } else {
                    messages.push(v);
                }
            }
        }

        // 回合快照也是工具执行的白名单：模型不得调用本轮未公开或已禁用的工具。
        let available_tools = self.available_tool_definitions(request);
        let tool_defs: Vec<serde_json::Value> = available_tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": serde_json::from_str::<serde_json::Value>(&t.parameters_json)
                            .unwrap_or(serde_json::json!({"type":"object"})),
                    }
                })
            })
            .collect();

        let max_rounds = if request.max_rounds == 0 { 1 } else { request.max_rounds };

        // 用户上下文前缀（用户 L0 动态环境 + L1 WORKING 短期记忆）：仅第一轮注入最新用户消息。
        // 模型感知 [当前时间] / [对话轮数] / [近期记忆]，与系统提示词 L0-L5 互为镜像。
        if let Some(orchestrator) = &self.orchestrator {
            let history_rounds = messages
                .iter()
                .filter(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
                .count() as u32;
            let options = self.orchestration_options(request);
            let prefix = orchestrator.build_user_context(
                companion_id,
                request.group_id,
                history_rounds,
                options,
            );
            if !prefix.trim().is_empty() {
                if let Some(idx) = messages
                    .iter()
                    .rposition(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
                {
                    let content = messages[idx]
                        .get("content")
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .to_string();
                    messages[idx]["content"] =
                        serde_json::Value::String(format!("{}\n\n{}", prefix.trim_end(), content));
                }
            }
        }

        let mut rounds_used = 0u32;

        // 回合状态跟踪（决策 1：B 控制 + A 存储）
        let mut last_status_text = String::new();
        let mut tool_calls_used: u32 = 0;
        let mut text_chars_total: u32 = 0;
        let mut last_finish_reason = String::new();
        // send_sticker 同轮连续无匹配重试上限（2 次；达到后注入 system 消息阻止继续尝试）
        let mut sticker_fail_streak: u32 = 0;
        const STICKER_FAIL_CAP: u32 = 2;
        // 通道发送的副作用事实：工具明确返回 send_succeeded=true/status=sent 后，
        // 模型第二轮不得用相反文本把成功误报为失败。仅记录严格 JSON 成功契约；
        // 失败/未知/旧格式均不触发，避免把真实失败改写成成功。
        let mut channel_send_succeeded = false;
        // 连续「纯气泡」轮计数与「已注入收尾提示」标志（见 PURE_BUBBLE_ROUNDS_BEFORE_NUDGE）
        let mut pure_bubble_streak: u32 = 0;
        let mut bubble_nudge_sent = false;

        while rounds_used < max_rounds {
            rounds_used += 1;
            let is_first_round = rounds_used == 1;
            let mut tool_choice = if !request.tool_choice.is_empty()
                && (is_first_round || request.tool_choice != "required") {
                request.tool_choice.clone()
            } else if is_first_round {
                // 兜底用 auto 而非 required：DeepSeek 思考模式（deepseek-v4-pro /
                // deepseek-reasoner）拒绝 tool_choice="required"（HTTP 400），
                // 而 emit_bubble 气泡协议靠 system rules 引导模型调用，auto 足够。
                "auto".to_string()
            } else {
                "auto".to_string()
            };

            // 状态机决策（B 控制：每轮 gateway.send 前）
            let decision = controller.on_round(TurnStateContext {
                round: rounds_used,
                max_rounds,
                tool_calls_used,
                text_chars_total,
                finished_reason: last_finish_reason.clone(),
                last_tool_results: String::new(),
            });
            if decision.action == TurnAction::Stop {
                return AgentTurnResult {
                    events,
                    final_text: String::new(),
                    rounds_used,
                    finished_reason: "state_stop".to_string(),
                    error: None,
                };
            }
            if !decision.tool_choice_override.is_empty() {
                tool_choice = decision.tool_choice_override;
            }
            // A 存储：状态摘要以 system 消息内嵌（较上轮有变化才写入，避免刷屏）
            if !decision.status_text.is_empty() && decision.status_text != last_status_text {
                messages.push(serde_json::json!({
                    "role": "system",
                    "content": decision.status_text,
                }));
                last_status_text = decision.status_text;
            }

            let tools_value = serde_json::Value::Array(tool_defs.clone());
            // 每轮重建 system prompt（L0-L5；记忆/技能随轮次变化）：
            // Kotlin 显式 system_prompt 覆盖优先（群聊场景）；否则编排器统一组装。
            // `pending_worldbook_injections` 每轮重置：世界书注入不跨轮累积。
            let mut pending_worldbook_injections: Vec<crate::lorebook::MessageInjection> = Vec::new();
            let mut turn_request = request.clone();
            if request.system_prompt.as_deref().map(str::trim).unwrap_or("").is_empty() {
                if let Some(orchestrator) = &self.orchestrator {
                    let profile = native.load_companion_profile(companion_id).ok();
                    let last_user = messages
                        .iter()
                        .rev()
                        .find(|m| m.get("role").and_then(|v| v.as_str()) == Some("user"))
                        .and_then(|m| m.get("content"))
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .to_string();
                    let options = self.orchestration_options(request);
                    let mut sys = orchestrator.build_system_prompt(
                        profile,
                        companion_id,
                        request.group_id,
                        last_user,
                        options,
                    );
                    // 核心插件提示词片段（cordis：core-prompt-fragments 插件注入）
                    for fragment in self.core_plugin_prompt_fragments() {
                        if fragment.content.trim().is_empty() {
                            continue;
                        }
                        sys.push_str("\n\n");
                        sys.push_str(&fragment.content);
                    }
                    // 生图协议（Kotlin 经 settings 下发）：提示词已下沉 Rust 组装，旧做法把协议
                    // 拼进 aiCompanion.systemPrompt，Agent 链路不经过那里 → 模型不知道能生图，
                    // 却仍被关键词触发生图（真机问题）。这里并入 system prompt。
                    if let Some(rules) = self.setting_str("image_gen_rules") {
                        if !rules.trim().is_empty() {
                            sys.push_str("\n\n");
                            sys.push_str(&rules);
                        }
                    }
                    // 世界书注入（聊天陪伴）：Q2 重写 —— 5 个注入位置分流到
                    // system 文本（before_char / after_char）与 messages[] 消息流
                    // （top_of_chat / bottom_of_chat / at_depth）。
                    // `before_char` 拼在 system 文本最前，`after_char` 拼在最后。
                    if let Some(plan) = self.worldbook_injection_plan(&messages) {
                        if !plan.sys_before.is_empty() {
                            sys = format!("{}\n\n{}", plan.sys_before, sys);
                        }
                        if !plan.sys_after.is_empty() {
                            sys.push_str("\n\n");
                            sys.push_str(&plan.sys_after);
                        }
                        // 消息流注入须在 system prompt 定稿后进行（见下方 apply）
                        pending_worldbook_injections = plan.injections;
                    }
                    if !sys.trim().is_empty() {
                        turn_request.system_prompt = Some(sys);
                    }
                }
            }
            // 世界书消息流注入（Q2）：每轮 send 前按注入计划生成**发送副本**，
            // 不修改 `messages` 本体 —— 避免污染后续轮次的索引定位与 tool_calls 回灌。
            let injected_messages;
            let send_messages: &Vec<serde_json::Value> = if pending_worldbook_injections.is_empty() {
                &messages
            } else {
                injected_messages =
                    apply_worldbook_injections(&messages, &pending_worldbook_injections);
                &injected_messages
            };
            let raw = if let Some(sink) = &stream_sink {
                native.send_stream(&turn_request, companion_id, send_messages, &tools_value, &tool_choice, sink.as_ref())
            } else {
                native.send(&turn_request, companion_id, send_messages, &tools_value, &tool_choice)
            };
            let raw = match raw {
                Ok(r) => r,
                Err(e) => {
                    return AgentTurnResult {
                        events,
                        final_text: String::new(),
                        rounds_used,
                        finished_reason: "error".to_string(),
                        error: Some(e),
                    };
                }
            };
            let parsed: serde_json::Value = match serde_json::from_str(&raw) {
                Ok(v) => v,
                Err(e) => {
                    return AgentTurnResult {
                        events,
                        final_text: String::new(),
                        rounds_used,
                        finished_reason: "error".to_string(),
                        error: Some(format!("模型响应解析失败: {e}")),
                    };
                }
            };

            let content = parsed.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let finish_reason = parsed.get("finish_reason").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let tool_calls: Vec<serde_json::Value> = parsed
                .get("tool_calls")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            last_finish_reason = finish_reason.clone();

            // ── 用量与思考过程上报（事件流）──
            // Agent 路径的 HTTP 已下沉 Rust，Kotlin AiService 不再经过，因此
            // ① token 统计（token_usage 表）②「显示思考过程」都必须由这里带出，
            // 否则二者在迁移后恒为空 / 恒不生效（真机实测：token_usage 0 行）。
            if let Some(usage) = parsed.get("usage") {
                if !usage.is_null() {
                    events.push(AgentEvent {
                        kind: "usage".to_string(),
                        text: usage.to_string(),
                        extra: String::new(),
                    });
                }
            }
            if let Some(reasoning) = parsed.get("reasoning_content").and_then(|v| v.as_str()) {
                if !reasoning.trim().is_empty() {
                    events.push(AgentEvent {
                        kind: "reasoning".to_string(),
                        text: reasoning.to_string(),
                        extra: String::new(),
                    });
                }
            }

            // 模型调用了工具
            if !tool_calls.is_empty() {
                // 多轮叙述（「边调边说」）：本轮既产出正文又调用工具时，把正文作为**独立气泡**
                // 先发出，保持「文本 → 工具 → 文本 → 工具」的先后顺序（对齐参考截图的真人节奏）。
                // 约束：
                //  - 仅非流式路径（流式增量已经由 StreamSink 实时交付，再补发会重复）；
                //  - 本轮未使用 emit_bubble / emit_segmented 气泡协议时（否则正文与工具气泡重复）。
                if stream_sink.is_none() && !content.trim().is_empty() {
                    let uses_bubble_protocol = tool_calls.iter().any(|tc| {
                        matches!(
                            tc.get("name").and_then(|v| v.as_str()),
                            Some("emit_bubble") | Some("emit_segmented")
                        )
                    });
                    if !uses_bubble_protocol {
                        // 唯一出口：正文补发同样要剥思考块（否则本出口成为泄漏旁路）
                        push_bubble(&mut events, &content);
                    }
                }
                let mut tool_messages: Vec<serde_json::Value> = Vec::new();
                // 本轮是否「纯气泡轮」（只调用了气泡协议工具，没有任何其他进展）——
                // 用于连续 K 轮后注入强制收尾提示（见 PURE_BUBBLE_ROUNDS_BEFORE_NUDGE）。
                let mut round_bubble_only = true;
                for call in &tool_calls {
                    let name = call.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if !is_bubble_protocol_tool(&name) {
                        round_bubble_only = false;
                    }
                    let args = call
                        .get("arguments")
                        .and_then(|v| v.as_str())
                        .unwrap_or("{}")
                        .to_string();
                    let call_id = call.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();

                    let chars_before = bubble_chars(&events);
                    let sticker_before = events.iter().filter(|e| e.kind == "sticker").count();
                    // ── 确认治理（Approval）──
                    // Commerce 类工具默认 RequireConfirm：未批准/未拒绝时停止回合，产出 confirm_request 事件，
                    // finished_reason="confirm_pending"；Kotlin 确认后经 approve_tool/reject_tool 授权，
                    // 重跑回合时匹配的调用直接执行（一次性授权）或回灌「用户拒绝」。
                    let definition = available_tools.iter().find(|tool| tool.name == name);
                    let needs_confirm = definition.is_some_and(|tool| tool.category == ToolCategory::Commerce);
                    let mut approved = false;
                    let mut rejected = false;
                    if needs_confirm {
                        let key = (name.clone(), args.clone());
                        approved = self.approved_tools.lock().unwrap().contains(&key);
                        rejected = self.rejected_tools.lock().unwrap().contains(&key);
                        if !approved && !rejected {
                            events.push(AgentEvent {
                                kind: "confirm_request".to_string(),
                                text: name.clone(),
                                extra: args.clone(),
                            });
                            return AgentTurnResult {
                                events,
                                final_text: String::new(),
                                rounds_used,
                                finished_reason: "confirm_pending".to_string(),
                                error: None,
                            };
                        }
                    }
                    let result_text = if definition.is_none() {
                        format!("错误：工具 {name} 在本回合不可用，请使用当前工具列表中的工具")
                    } else if needs_confirm && rejected {
                        // 用户已拒绝：结果回灌模型，指导其调整（不执行、不再次触发确认）。
                        // 一次性拒绝：消费后即失效，否则同名同参数的调用会被永久拒绝。
                        self.rejected_tools.lock().unwrap().remove(&(name.clone(), args.clone()));
                        format!("用户拒绝了该操作（工具 {name} 未授权执行）。请勿再次调用该工具，改用其他方式或询问用户。")
                    } else {
                        if approved {
                            // 一次性授权：使用后即失效
                            self.approved_tools.lock().unwrap().remove(&(name.clone(), args.clone()));
                        }
                        // 与公开定义的 builtin/global/session/core 优先级一致。
                        let registered = self.registry.builtin.contains_key(&name)
                            || self.registry.global_tools.lock().unwrap().get(&name).is_some_and(|t| t.available)
                            || self.registry.session_tools.lock().unwrap().get(&name).is_some_and(|t| t.available);
                        if registered {
                            self.registry.execute_tool(&ctx, tool_host, &name, &args, &mut events)
                        } else if let Some(executor) = self.core_tool_executor(&name) {
                            executor(&args)
                        } else {
                            format!("错误：未注册的工具 {name}")
                        }
                    };
                    if name == "send_channel_message" && channel_send_result_is_success(&result_text) {
                        channel_send_succeeded = true;
                    }
                    tool_calls_used += 1;
                    text_chars_total += bubble_chars(&events).saturating_sub(chars_before);

                    // 回合级工具调用预算（P1-5）：达到上限立即终止，防止失控循环
                    if tool_calls_used >= MAX_TOOL_CALLS_PER_TURN {
                        return AgentTurnResult {
                            events,
                            final_text: String::new(),
                            rounds_used,
                            finished_reason: "max_tool_calls".to_string(),
                            error: Some(format!("达到单回合工具调用上限 {MAX_TOOL_CALLS_PER_TURN}")),
                        };
                    }

                    // send_sticker 连续无匹配重试上限：本轮未产出新 sticker 事件且返回
                    // 无匹配报告 → 计入连续失败；达到上限后注入 system 消息阻止模型继续尝试
                    // （设计文档 §6：同轮失败重试上限 2 次，模型应修正 tags 或放弃改发文字）。
                    if name == "send_sticker" {
                        let sticker_pushed = events.iter().filter(|e| e.kind == "sticker").count() > sticker_before;
                        if !sticker_pushed && result_text.contains("无匹配") {
                            sticker_fail_streak += 1;
                            if sticker_fail_streak >= STICKER_FAIL_CAP {
                                messages.push(serde_json::json!({
                                    "type": "system",
                                    "role": "system",
                                    "_agent_preserve_system": true,
                                    "content": format!(
                                        "系统提示：你已连续 {STICKER_FAIL_CAP} 次发送表情包失败（标签无法匹配可用表情包）。请立即停止调用 send_sticker，改用文字气泡（emit_bubble）回复用户，或改用文字描述情绪。"
                                    ),
                                }));
                            }
                        } else {
                            sticker_fail_streak = 0;
                        }
                    }

                    // 工具结果以 TOOL 角色回灌（对齐 AiDialogueHistoryPolicy.toolResultMessage）
                    // ⚠️ DeepSeek 思考模式消息为带 type 标签的枚举（serde tagged enum）：
                    // 每条消息必须带 "type" 字段（"user"/"assistant"/"tool"/"system"），
                    // 否则服务端反序列化报 `missing field type`（HTTP 400）。
                    // OpenAI 标准接口仅需 role；这里 type 与 role 对齐，双兼容。
                    tool_messages.push(serde_json::json!({
                        "type": "tool",
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": result_text,
                    }));
                }
                // 模型当前轮的输出（若有 content）作为 assistant 消息 —— 无条件回灌：
                // OpenAI/DeepSeek 要求 tool_calls 对应的 assistant 消息必须存在，
                // 否则 tool 消息无法匹配（HTTP 400）。content 可为空。
                // ⚠️ 重建标准 tool_calls 格式：OpenAI/DeepSeek 要求
                // {"id","type":"function","function":{"name","arguments"}}。
                // parse_openai_response 产出的扁平结构（id/name/arguments）不能直接回灌，
                // 否则服务端反序列化报 missing field 'type'（HTTP 400）。
                let normalized_calls: Vec<serde_json::Value> = tool_calls
                    .iter()
                    .map(|tc| {
                        serde_json::json!({
                            "id": tc.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                            "type": "function",
                            "function": {
                                "name": tc.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                                "arguments": tc.get("arguments").and_then(|v| v.as_str()).unwrap_or("{}"),
                            }
                        })
                    })
                    .collect();
                let mut assistant_message = serde_json::json!({
                    "type": "assistant",
                    "role": "assistant",
                    "content": content.clone(),
                    "tool_calls": normalized_calls,
                });
                // 思考模型要求工具调用对应的推理内容原样回灌，不能在中间层丢弃。
                if let Some(reasoning) = parsed.get("reasoning_content").and_then(|v| v.as_str()) {
                    assistant_message["reasoning_content"] = serde_json::Value::String(reasoning.to_string());
                }
                // 连续纯气泡轮计数：达到阈值注入一次「立即用文字收尾」提示。
                // 依据（Issue #2 症状 2）：纯气泡轮是「越说越多、越说越跑偏」的前兆，
                // 而循环的唯一停止条件此前是模型自己收手（硬上限 64 轮 / 8000 字符）。
                if round_bubble_only {
                    pure_bubble_streak += 1;
                } else {
                    pure_bubble_streak = 0;
                    bubble_nudge_sent = false;
                }
                if pure_bubble_streak >= PURE_BUBBLE_ROUNDS_BEFORE_NUDGE && !bubble_nudge_sent {
                    bubble_nudge_sent = true;
                    messages.push(serde_json::json!({
                        "type": "system",
                        "role": "system",
                        "_agent_preserve_system": true,
                        "content": format!(
                            "系统提示：你已连续 {PURE_BUBBLE_ROUNDS_BEFORE_NUDGE} 轮只输出气泡、没有给出正式回复。请**立即停止继续拆分气泡**，用一条完整文字回复结束本轮；不要再调用 emit_bubble / emit_segmented。"
                        ),
                    }));
                }
                messages.push(assistant_message);
                messages.extend(tool_messages);
                continue;
            }

            // 正常文本 → 产出 bubble 事件，结束。通道发送已成功时，发布前做事实一致性校验：
            // 模型可以自然表达，但不能把刚完成的副作用反说成失败。
            let content = reconcile_channel_send_statement(content, channel_send_succeeded);
            // 唯一出口：最终正文同样剥思考块（模型把思考写在正文里时的主要泄漏点）
            push_bubble(&mut events, &content);
            // ── 修1：finish_reason 合法性判断 ──
            // parse_openai_response 把模型/API 错误平铺为 finish_reason="error"（含 error 字段）：
            // 不得标记为 completed，原样上报错误。
            if finish_reason == "error" {
                let msg = parsed
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("模型返回错误响应")
                    .to_string();
                return AgentTurnResult {
                    events,
                    final_text: content,
                    rounds_used,
                    finished_reason: "error".to_string(),
                    error: Some(msg),
                };
            }
            // 空响应 → error 而非 completed：仅当本回合**从未产出任何事件**（无 bubble/sticker）
            // 时才判 error——模型多轮工具调用后以空正文收尾是正常结束（事件流已承载输出），不得误判。
            // ⚠️ 同时覆盖 finish_reason="length"：思考型模型可能把 max_tokens 预算全耗在 reasoning 上，
            // 正文为空且无任何输出。旧实现把它当成成功的 max_rounds 静默返回，上层只看到
            // 「AI 未返回有效内容」而无法判断是截断，故此处显式上报可操作的错误。
            if content.is_empty() {
                let has_output = events.iter().any(|e| e.kind == "bubble" || e.kind == "sticker");
                if !has_output {
                    let msg = if finish_reason == "length" {
                        "模型输出被 max_tokens 截断且未产生任何可见内容（finish_reason=length）——思考型模型可能把预算耗在推理上，请提高该 API 的最大输出（max_tokens）后重试".to_string()
                    } else {
                        format!("模型返回空响应（finish_reason={finish_reason:?}）")
                    };
                    return AgentTurnResult {
                        events,
                        final_text: String::new(),
                        rounds_used,
                        finished_reason: "error".to_string(),
                        error: Some(msg),
                    };
                }
            }
            // 回合级文本预算（P1-5）：累计气泡字符超限即终止（正常对话远达不到）
            if bubble_chars(&events) as usize >= MAX_BUBBLE_CHARS_PER_TURN {
                return AgentTurnResult {
                    events,
                    final_text: content,
                    rounds_used,
                    finished_reason: "max_text".to_string(),
                    error: Some(format!("单回合气泡文本超过 {MAX_BUBBLE_CHARS_PER_TURN} 字符上限")),
                };
            }
            let finish = if finish_reason == "length" {
                "max_rounds".to_string()
            } else {
                "completed".to_string()
            };
            return AgentTurnResult {
                events,
                final_text: content,
                rounds_used,
                finished_reason: finish,
                error: None,
            };
        }

        AgentTurnResult {
            events,
            final_text: String::new(),
            rounds_used,
            finished_reason: "max_rounds".to_string(),
            error: Some(format!("达到最大轮数 {max_rounds}")),
        }
    }
}

impl AgentRuntime {
    /// 懒初始化核心插件宿主（OnceLock；失败记录一次并永久降级为空操作）。
    fn ensure_core_plugins(&self) -> Option<Arc<crate::cordis_bridge::CorePluginHost>> {
        self.core_plugins
            .get_or_init(|| {
                let selectors = crate::cordis_bridge::CoreSelectorSet {
                    memory: self.orchestrator.as_ref().and_then(|o| o.memory_selector()),
                    skill: self.orchestrator.as_ref().and_then(|o| o.skill_selector()),
                };
                crate::cordis_bridge::CorePluginHost::new(selectors).map_err(|e| {
                    crate::agentlog::warn("agent", &format!("core plugin host init failed: {e}"));
                })
            })
            .clone()
            .ok()
    }

    /// 回合事件 scope 标签（Scope ①）：companion:{id} / group:{gid} / global。
    fn turn_scope_tag(&self, companion_id: Option<i64>, group_id: Option<i64>) -> String {
        if let Some(id) = companion_id {
            format!("companion:{id}")
        } else if let Some(gid) = group_id {
            format!("group:{gid}")
        } else {
            "global".to_string()
        }
    }

    /// 核心插件工具定义（宿主未初始化返回空列表）。
    fn core_plugin_tools(&self) -> Vec<ToolDefinition> {
        self.ensure_core_plugins()
            .map(|h| h.core_tool_definitions())
            .unwrap_or_default()
    }

    /// 核心插件工具执行器（按名；None = 未注册/宿主不可用）。
    fn core_tool_executor(&self, name: &str) -> Option<Arc<dyn Fn(&str) -> String + Send + Sync>> {
        self.ensure_core_plugins().and_then(|h| h.core_tool_executor(name))
    }

    /// 核心插件提示词片段（宿主未初始化返回空列表）。
    fn core_plugin_prompt_fragments(&self) -> Vec<crate::prompt_orchestrator::PromptFragment> {
        self.ensure_core_plugins()
            .map(|h| h.prompt_fragments())
            .unwrap_or_default()
    }

    /// Q2：世界书命中 → 注入计划（未设置世界书返回 `None`）。
    ///
    /// 扫描上下文取全部消息文本（与历史行为一致：由世界书顶层 `scan_depth` 限窗）。
    fn worldbook_injection_plan(
        &self,
        messages: &[serde_json::Value],
    ) -> Option<crate::lorebook::InjectionPlan> {
        let wb = self.worldbook.lock().unwrap().clone()?;
        let texts: Vec<String> = messages
            .iter()
            .filter_map(|m| m.get("content").and_then(|c| c.as_str()).map(str::to_string))
            .collect();
        let hits = crate::lorebook::LorebookInjector::scan(&wb, &texts);
        if hits.is_empty() {
            return None;
        }
        Some(crate::lorebook::plan_injections(&hits))
    }
}

/// Q2：把注入计划落到消息数组副本上（**不修改**入参）。
///
/// 锚点语义对齐本地 `PromptInjectionTransformer`：
/// - `TopOfChat` → 首条非 system 消息之前
/// - `BottomOfChat` → 末条消息之前
/// - `AtDepth(n)` → 倒数第 `n` 条消息之前
///
/// 安全落点：插入前调用 [`find_safe_insert_index`] 规避工具调用链断裂
/// （`user → assistant(tool_calls)`、`assistant(tool_calls) → tool`、`tool → assistant`、
/// `assistant → assistant`）。
fn apply_worldbook_injections(
    messages: &[serde_json::Value],
    injections: &[crate::lorebook::MessageInjection],
) -> Vec<serde_json::Value> {
    let mut out: Vec<serde_json::Value> = messages.to_vec();
    for inj in injections {
        let target = match inj.anchor {
            crate::lorebook::InjectionAnchor::TopOfChat => first_non_system_index(&out),
            crate::lorebook::InjectionAnchor::BottomOfChat => out.len().saturating_sub(1),
            crate::lorebook::InjectionAnchor::AtDepth(depth) => {
                out.len().saturating_sub(depth.max(1) as usize)
            }
        };
        let idx = find_safe_insert_index(&out, target);
        out.insert(
            idx,
            serde_json::json!({ "role": inj.role, "content": inj.content, "_agent_preserve_system": true }),
        );
    }
    out
}

/// 首条非 system 消息下标（全为 system 时返回长度，即追加到末尾）。
fn first_non_system_index(messages: &[serde_json::Value]) -> usize {
    messages
        .iter()
        .position(|m| m.get("role").and_then(|r| r.as_str()) != Some("system"))
        .unwrap_or(messages.len())
}

fn role_of(m: &serde_json::Value) -> &str {
    m.get("role").and_then(|r| r.as_str()).unwrap_or("")
}

fn has_tool_calls(m: &serde_json::Value) -> bool {
    m.get("tool_calls")
        .and_then(|t| t.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false)
}

/// 安全插入索引：向前回退直到不打断工具调用链（对齐本地 `findSafeInsertIndex`）。
fn find_safe_insert_index(messages: &[serde_json::Value], target: usize) -> usize {
    let mut idx = target.min(messages.len());
    while idx > 0 && idx < messages.len() {
        let prev = &messages[idx - 1];
        let curr = &messages[idx];
        let (pr, cr) = (role_of(prev), role_of(curr));
        let break_chain = (pr == "user" && cr == "assistant" && has_tool_calls(curr))
            || (pr == "assistant" && has_tool_calls(prev) && cr == "tool")
            || (pr == "tool" && cr == "assistant")
            || (pr == "assistant" && cr == "assistant");
        if !break_chain {
            break;
        }
        idx -= 1;
    }
    idx
}

/// 回合生命周期事件作用域（RAII：进入发出 start，Drop 发出 end，任意返回路径必达）。
/// Scope ①：事件携带 scope 标签，供插件按作用域记账/过滤。
struct TurnScope(Option<Arc<crate::cordis_bridge::CorePluginHost>>, String);

impl TurnScope {
    fn begin(host: Option<Arc<crate::cordis_bridge::CorePluginHost>>, scope: &str) -> Self {
        if let Some(h) = &host {
            h.emit_turn_start(scope);
        }
        Self(host, scope.to_string())
    }
}

impl Drop for TurnScope {
    fn drop(&mut self) {
        if let Some(h) = &self.0 {
            h.emit_turn_end(&self.1);
        }
    }
}

/// 统计 events 中 bubble 事件文本总字符数
fn bubble_chars(events: &[AgentEvent]) -> u32 {
    events
        .iter()
        .filter(|e| e.kind == "bubble")
        .map(|e| e.text.chars().count() as u32)
        .sum()
}

/// 只接受 `send_channel_message` 的新成功契约，避免旧格式或模糊文本被误判为成功。
fn channel_send_result_is_success(result: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(result)
        .ok()
        .is_some_and(|value| {
            value.get("ok").and_then(|v| v.as_bool()) == Some(true)
                && value.get("send_succeeded").and_then(|v| v.as_bool()) == Some(true)
                && value.get("status").and_then(|v| v.as_str()) == Some("sent")
        })
}

/// 成功副作用与最终自然语言冲突时，使用保守且不声称对端已收到的确定性文本。
fn reconcile_channel_send_statement(content: String, send_succeeded: bool) -> String {
    if !send_succeeded {
        return content;
    }
    let normalized = content.to_lowercase();
    const CONTRADICTIONS: &[&str] = &[
        "没发出去", "没有发出去", "发不出去", "发送失败", "发送不成功",
        "送不出去", "未能发送", "直接报错", "通道报错", "还是不行",
        "send failed", "failed to send", "sending failed",
    ];
    if CONTRADICTIONS.iter().any(|marker| normalized.contains(marker)) {
        "消息已经通过通道发出去了；通道没有对端送达回执。".to_string()
    } else {
        content
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_gateway::testutil::{self, MockTransport};
    use crate::native_gateway::NativeGateway;
    use std::sync::atomic::Ordering;

    struct NoopHost;
    impl ToolHost for NoopHost {
        fn execute(&self, _name: String, _args: String, _ctx: String) -> String {
            "ok".to_string()
        }
    }

    /// 构造注入 mock 传输的运行时（指向临时 SQLite 库）
    /// 返回 (runtime, gw, transport)：gw 注入 mock 传输，供 run_turn_inner 直接使用
    fn mock_gateway(responses: Vec<&str>) -> (Arc<AgentRuntime>, NativeGateway, Arc<MockTransport>) {
        let db = testutil::temp_db();
        let transport = Arc::new(MockTransport::new(responses));
        let runtime = AgentRuntime::new(testutil::global_config(&db));
        let gw = NativeGateway::with_transport(testutil::global_config(&db), transport.clone());
        (runtime, gw, transport)
    }

    /// 生图协议经 settings 注入：Kotlin 下发 image_gen_rules 后，提示词组装必须读得到。
    ///
    /// 真机问题：提示词下沉 Rust 后，Kotlin 把协议拼进 aiCompanion.systemPrompt 的做法
    /// 在 Agent 链路上失效 → 模型自称「不会画画」，而关键词触发照旧生图。
    #[test]
    fn settings_supply_image_gen_rules_for_prompt_assembly() {
        let (runtime, _gw, _t) = mock_gateway(vec!["{}"]);
        assert!(runtime.setting_str("image_gen_rules").is_none(), "默认不应带该键");
        runtime.update_settings(
            serde_json::json!({
                "role": "GIRLFRIEND",
                "image_gen_rules": "【生图协议】需要配图时输出 [[生图: 画面描述]]"
            })
            .to_string(),
        );
        let rules = runtime
            .setting_str("image_gen_rules")
            .expect("settings 热更新后应能读到生图协议");
        assert!(rules.contains("[[生图"), "{rules}");
    }

    #[test]
    fn runner_produces_events() {
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: "required".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let (runner, gw, _transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_segmented","arguments":"{\"text\":\"今天天气真好呀。我们去散步吧！\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的呢"},"finish_reason":"stop"}]}"#,
        ]);
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed");
        assert!(result.events.iter().any(|e| e.kind == "bubble"));
        assert_eq!(result.final_text, "好的呢");
    }

    /// 多轮叙述（「边调边说」）：带工具调用的那一轮的正文，必须作为**独立气泡**先于工具产出送达，
    /// 保持「文本 → 工具」的先后顺序（对齐参考截图的真人节奏）；后续轮正文同样独立成泡。
    #[test]
    fn tool_round_narration_text_emitted_as_bubble_in_order() {
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let (runner, gw, _t) = mock_gateway(vec![
            // 第1轮：正文叙述 + 非气泡工具调用（模拟手机控制）→ 正文应先成为独立气泡
            r#"{"choices":[{"message":{"role":"assistant","content":"我点进去看看","tool_calls":[{"id":"c1","type":"function","function":{"name":"screen_tap","arguments":"{\"x\":1}"}}]},"finish_reason":"tool_calls"}]}"#,
            // 第2轮：叙述 + 结束
            r#"{"choices":[{"message":{"role":"assistant","content":"找到了，我点购买"},"finish_reason":"stop"}]}"#,
        ]);
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed");
        let bubbles: Vec<String> = result
            .events
            .iter()
            .filter(|e| e.kind == "bubble")
            .map(|e| e.text.clone())
            .collect();
        assert_eq!(bubbles, vec!["我点进去看看", "找到了，我点购买"]);
    }

    /// 使用 emit_bubble 气泡协议时，带工具调用的那一轮的 content 不得再补发气泡（避免重复）。
    #[test]
    fn tool_round_with_emit_bubble_protocol_does_not_duplicate_narration() {
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let (runner, gw, _t) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"不应补发的气泡","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"协议气泡\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"收尾"},"finish_reason":"stop"}]}"#,
        ]);
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        let bubbles: Vec<String> = result
            .events
            .iter()
            .filter(|e| e.kind == "bubble")
            .map(|e| e.text.clone())
            .collect();
        assert_eq!(bubbles, vec!["协议气泡", "收尾"]);
        assert!(
            !bubbles.iter().any(|b| b == "不应补发的气泡"),
            "emit_bubble 协议下不得重复补发 content: {bubbles:?}"
        );
    }

    /// 单回合工具调用预算：撞到上限必须**立即终止并显式上报** `max_tool_calls`，
    /// 而不是一路跑到 max_rounds 静默收尾。同时钉死上限取值，防被误改。
    #[test]
    fn tool_call_budget_trips_at_configured_limit() {
        // 只预置一个响应时 MockTransport 会一直返回它 → 每轮都产生一次工具调用
        let (runner, gw, _t) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"noop_probe","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            // 轮数给得远大于工具预算，确保先撞的是工具上限而不是轮数上限
            max_rounds: MAX_TOOL_CALLS_PER_TURN + 10,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(
            result.finished_reason, "max_tool_calls",
            "应撞工具预算而非轮数预算: {result:?}"
        );
        assert_eq!(
            result.rounds_used, MAX_TOOL_CALLS_PER_TURN,
            "第 {MAX_TOOL_CALLS_PER_TURN} 次工具调用后应立即终止"
        );
        assert_eq!(MAX_TOOL_CALLS_PER_TURN, 64, "单回合工具调用上限应为 64");
        let err = result.error.unwrap_or_default();
        assert!(err.contains("64"), "错误文案应带上限数值: {err}");
    }

    /// 工具调用完整链路：模型首轮返回 emit_bubble tool_calls →
    /// builtin 执行产出 bubble 事件 → 工具结果以 TOOL 角色回灌第二轮请求，
    /// 且回灌消息**不含 name 字段**（OpenAI/DeepSeek 兼容格式）。
    #[test]
    fn emit_bubble_tool_call_full_chain() {
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_abc","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"第一句\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"第二句"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed");
        // ① emit_bubble 执行 → bubble 事件（工具气泡 + 收尾文本气泡）
        let bubbles: Vec<&str> = result
            .events
            .iter()
            .filter(|e| e.kind == "bubble")
            .map(|e| e.text.as_str())
            .collect();
        assert_eq!(
            bubbles,
            vec!["第一句", "第二句"],
            "emit_bubble 产出工具气泡 + 第二轮收尾文本气泡: {result:?}"
        );
        // ② 第二轮请求中工具结果以 TOOL 角色回灌且不含 name 字段
        let requests = transport.requests.lock().unwrap();
        let second_body: serde_json::Value = serde_json::from_str(&requests[1].2).unwrap();
        let msgs = second_body.get("messages").and_then(|v| v.as_array()).unwrap();
        // 2a) 回灌的 assistant 消息必须带标准 tool_calls（type=function + function 嵌套）
        let assistant_tool = msgs
            .iter()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("assistant") && m.get("tool_calls").is_some())
            .expect("应回灌 assistant-with-tool_calls 消息");
        let calls = assistant_tool.get("tool_calls").and_then(|t| t.as_array()).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].get("type").and_then(|v| v.as_str()),
            Some("function"),
            "tool_calls 每项必须带 type=function（否则 DeepSeek 报 missing field type）: {second_body}"
        );
        assert_eq!(
            calls[0]
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|v| v.as_str()),
            Some("emit_bubble"),
            "tool_calls 必须为 function.name 嵌套格式: {second_body}"
        );
        // 2b) tool 消息：type/role/tool_call_id/content，不含 name
        let tool_msgs: Vec<&serde_json::Value> = msgs
            .iter()
            .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("tool"))
            .collect();
        assert_eq!(tool_msgs.len(), 1, "应有 1 条 tool 回灌消息: {second_body}");
        assert_eq!(
            tool_msgs[0].get("tool_call_id").and_then(|v| v.as_str()),
            Some("call_abc"),
            "tool_call_id 应回灌: {second_body}"
        );
        assert!(
            tool_msgs[0].get("name").is_none(),
            "tool 消息不应含 name 字段（OpenAI/DeepSeek 兼容格式）: {second_body}"
        );
        assert_eq!(
            tool_msgs[0].get("content").and_then(|v| v.as_str()),
            Some("已输出气泡"),
            "工具结果应回灌: {second_body}"
        );
    }

    #[test]
    fn segmented_tool_emits_multiple_bubbles() {
        let reg = AgentToolRegistry::new();
        let mut events = Vec::new();
        let ctx = ToolContext::default();
        let out = AgentToolRegistry::builtin_emit_segmented(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"text":"今天天气真好呀。我们去散步吧！你觉得呢？"}"#,
            &mut events,
        );
        assert!(events.iter().filter(|e| e.kind == "bubble").count() >= 2, "{out} {events:?}");
    }

    /// Issue #2 症状 1 回归：`emit_bubble` 收到的文本若含思考块，气泡必须只带正式正文。
    #[test]
    fn emit_bubble_strips_thinking_blocks() {
        let reg = AgentToolRegistry::new();
        let ctx = ToolContext::default();
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_emit_bubble(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"text":"<thinking>先想想怎么回</thinking>好呀，我在呢"}"#,
            &mut events,
        );
        let bubbles: Vec<&str> = events.iter().map(|e| e.text.as_str()).collect();
        assert_eq!(bubbles, vec!["好呀，我在呢"], "思考块必须被剥离: {out} {events:?}");
        assert!(!out.contains("错误"), "{out}");
    }

    /// Issue #2 症状 1 回归：整条都是思考 → **不产出气泡**（不伪造内容）且如实报错。
    #[test]
    fn emit_bubble_rejects_pure_thinking_text() {
        let reg = AgentToolRegistry::new();
        let ctx = ToolContext::default();
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_emit_bubble(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"text":"<thinking>嗯，等待用户回答"}"#,
            &mut events,
        );
        assert!(events.is_empty(), "纯思考不得产出气泡: {events:?}");
        assert!(out.contains("思考内容不得作为气泡输出"), "{out}");
    }

    /// Issue #2 症状 1 回归：`emit_segmented` 必须先剥思考再分段，
    /// 否则思考会被切进多条气泡。
    #[test]
    fn emit_segmented_strips_thinking_before_splitting() {
        let reg = AgentToolRegistry::new();
        let ctx = ToolContext::default();
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_emit_segmented(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"text":"<thinking>先分析用户意图再决定怎么回</thinking>今天天气真好呀。我们去散步吧！"}"#,
            &mut events,
        );
        let joined = events.iter().map(|e| e.text.as_str()).collect::<Vec<_>>().join("");
        assert!(!joined.contains("思考") && !joined.contains("分析用户意图"), "思考不得进入气泡: {out} {events:?}");
        assert!(joined.contains("天气真好"), "{out} {events:?}");
    }

    /// Issue #2 症状 2 回归：连续 3 轮「纯气泡」后，下一轮请求必须带上强制收尾提示
    /// （此前唯一停止条件是模型自己收手，会一路跑到轮数上限）。
    #[test]
    fn pure_bubble_rounds_trigger_forced_wrapup_nudge() {
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"第一句\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c2","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"第二句\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c3","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"第三句\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好了，就说这么多～"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 6,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed", "{result:?}");
        let requests = transport.requests.lock().unwrap();
        assert!(requests.len() >= 4, "应至少发起 4 轮请求: {}", requests.len());
        // 第 3 轮处理完（连续 3 轮纯气泡）后注入 → 第 4 轮请求可见
        let fourth: serde_json::Value = serde_json::from_str(&requests[3].2).unwrap();
        let msgs = fourth.get("messages").and_then(|v| v.as_array()).unwrap();
        assert!(
            msgs.iter().any(|m| m
                .get("content")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .contains("立即停止继续拆分气泡")),
            "连续纯气泡后必须注入强制收尾提示: {fourth}"
        );
        // 提示必须是保留型 system 消息（与既有通道一致，不被 persona 重建清掉）
        assert!(msgs.iter().any(|m| m
            .get("_agent_preserve_system")
            .and_then(|v| v.as_bool())
            == Some(true)));
    }

    /// 空表情库（可用标签为空）且预选失败 → **绝不产出 sticker 事件**。
    ///
    /// 旧行为会透传事件，Kotlin 落地时按描述模糊匹配内置表情，于是恒发同一张
    /// 「默认」表情包（真机问题：表情包列表为空时模型发固定刷子图）。
    #[test]
    fn empty_sticker_library_does_not_emit_default_sticker() {
        let reg = AgentToolRegistry::new();
        let ctx = ToolContext::default(); // available_sticker_tags 为空 = 表情库为空
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["开心"]}"#,
            &mut events,
        );
        assert!(events.is_empty(), "空库不得产出 sticker 事件: {events:?}");
        assert!(
            out.contains("表情库为空") || out.contains("不要发送表情包"),
            "应明确告知模型没有表情包: {out}"
        );
    }

    #[test]
    fn sticker_tool_cleans_tags() {
        let reg = AgentToolRegistry::new();
        // 带标点/空白/大小写：清洗后应为无标点小写词，emoji 保留。
        // 必须给非空可用标签才能走「命中 → 产出 sticker 事件」路径；
        // 空库路径由 empty_sticker_library_does_not_emit_default_sticker 覆盖。
        let ctx = ToolContext {
            available_sticker_tags: vec!["开心😊".to_string(), "happy".to_string()],
            ..Default::default()
        };
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["  开心！😊 ","Happy!"]}"#,
            &mut events,
        );
        assert_eq!(events.len(), 1, "{out}");
        assert_eq!(events[0].kind, "sticker");
        assert_eq!(events[0].text, "开心😊,happy", "{out}");

        // 清洗后全部为空 → 错误返回，不产生事件
        let mut events2 = Vec::new();
        let out2 = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["！！！","。。。"]}"#,
            &mut events2,
        );
        assert!(out2.starts_with("错误"), "{out2}");
        assert!(events2.is_empty(), "{events2:?}");

        // 缺失 tags → 错误返回
        let mut events3 = Vec::new();
        let out3 = AgentToolRegistry::builtin_send_sticker(&reg, &ctx, &NoopHost, r#"{}"#, &mut events3);
        assert!(out3.starts_with("错误"), "{out3}");
        assert!(events3.is_empty(), "{events3:?}");

        // 超过 3 个 tag → 错误返回
        let mut events4 = Vec::new();
        let out4 = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["a","b","c","d"]}"#,
            &mut events4,
        );
        assert!(out4.starts_with("错误"), "{out4}");
        assert!(events4.is_empty(), "{events4:?}");

        // 超长 tag 截断到 20 字符
        let long = "这是一个非常非常非常非常非常非常非常非常长的表情包描述描述描述描述";
        let cleaned = AgentToolRegistry::clean_sticker_description(long);
        assert_eq!(cleaned.chars().count(), 20, "{cleaned}");
    }

    /// 设计文档 §6：可用标签注入后 → 命中 tag 产出 sticker 事件；全部未命中 →
    /// 不输出事件、返回无匹配报告（含可用标签 Top-30）。
    #[test]
    fn sticker_tool_matches_available_tags() {
        let reg = AgentToolRegistry::new();
        // 可用标签（Kotlin StickerManager 注入，Top-30）
        let ctx = ToolContext {
            available_sticker_tags: vec![
                "开心".to_string(),
                "生气".to_string(),
                "撒娇".to_string(),
                "委屈巴巴".to_string(),
                "抱抱".to_string(),
            ],
            ..ToolContext::default()
        };

        // ① 单 tag 命中 → 产出 sticker 事件
        let mut events = Vec::new();
        let out = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["开心"]}"#,
            &mut events,
        );
        assert_eq!(events.len(), 1, "{out}");
        assert_eq!(events[0].kind, "sticker");
        assert_eq!(events[0].text, "开心", "{out}");

        // ② 多 tag 部分命中 → 只回灌命中的 tag（逗号连接）
        let mut events2 = Vec::new();
        let out2 = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["抱抱","不存在的标签"]}"#,
            &mut events2,
        );
        assert_eq!(events2.len(), 1, "{out2}");
        assert_eq!(events2[0].text, "抱抱", "{out2}");

        // ③ 全部未命中 → 无 sticker 事件，返回无匹配报告（含可用标签 Top-30）
        let mut events3 = Vec::new();
        let out3 = AgentToolRegistry::builtin_send_sticker(
            &reg,
            &ctx,
            &NoopHost,
            r#"{"tags":["发癫","大哭"]}"#,
            &mut events3,
        );
        assert!(events3.is_empty(), "{events3:?}");
        assert!(out3.contains("无匹配"), "{out3}");
        assert!(out3.contains("开心"), "报告应含可用标签: {out3}");
        assert!(out3.contains("抱抱"), "报告应含可用标签: {out3}");
    }

    /// 设计文档 §6：同轮 send_sticker 连续无匹配达到 2 次 → 注入 system 消息阻止继续尝试。
    #[test]
    fn send_sticker_no_match_retry_cap_injects_system_message() {
        // 三轮响应：两次 send_sticker 无匹配 → 第三轮收尾文本
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"s1","type":"function","function":{"name":"send_sticker","arguments":"{\"tags\":[\"发癫\"]}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"s2","type":"function","function":{"name":"send_sticker","arguments":"{\"tags\":[\"大哭\"]}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的呢"},"finish_reason":"stop"}]}"#,
        ]);
        // 注入可用标签（Kotlin StickerManager 产物；内置工具经 ctx.available_sticker_tags 读取）
        runner.update_stickers(vec!["开心".to_string(), "抱抱".to_string()]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 4,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        // 两次无匹配报告回灌后，应出现阻止继续尝试的 system 消息
        let requests = transport.requests.lock().unwrap();
        let bodies: Vec<serde_json::Value> = requests.iter().map(|r| serde_json::from_str(&r.2).unwrap()).collect();
        let all_system: String = bodies
            .iter()
            .flat_map(|b| {
                b.get("messages")
                    .and_then(|m| m.as_array())
                    .unwrap_or(&vec![])
                    .iter()
                    .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("system"))
                    .filter_map(|m| m.get("content").and_then(|c| c.as_str()))
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            all_system.contains("连续 2 次发送表情包失败"),
            "达到重试上限应注入 system 阻止消息: {all_system}"
        );
        assert!(
            result.events.iter().filter(|e| e.kind == "sticker").count() == 0,
            "两次均未命中，不应产出 sticker 事件: {result:?}"
        );
        assert_eq!(result.final_text, "好的呢");
    }

    /// 决策 1 测试：默认状态机注入 [回合状态] system 消息（A 存储）
    #[test]
    fn default_state_machine_injects_status_message() {
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_segmented","arguments":"{\"text\":\"嗨\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的呢"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        let requests = transport.requests.lock().unwrap();
        let first_body: serde_json::Value = serde_json::from_str(&requests[0].2).unwrap();
        let msgs = first_body.get("messages").and_then(|v| v.as_array()).unwrap();
        assert!(
            msgs.iter().any(|m| m.get("role").and_then(|r| r.as_str()) == Some("system")
                && m.get("content").and_then(|c| c.as_str()).unwrap_or("").contains("[回合状态")),
            "首轮应注入 [回合状态] system 消息: {first_body}"
        );
        // Issue #2 症状 2 回归：状态消息**不得**再暴露「N/上限」分母
        // （旧文案 [回合状态 1/3] 会让模型误以为预算充足、应当继续输出）
        assert!(
            !msgs.iter().any(|m| m
                .get("content")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .contains("1/3")),
            "状态消息不得含轮次分母: {first_body}"
        );
    }

    /// 决策 1 测试：B 控制 —— 自定义控制器返回 Stop 时立即停止（state_stop）
    #[test]
    fn controller_stops_early() {
        struct StopAfterOne;
        impl TurnStateController for StopAfterOne {
            fn on_round(&self, ctx: TurnStateContext) -> TurnStateDecision {
                if ctx.round >= 2 {
                    TurnStateDecision {
                        action: TurnAction::Stop,
                        status_text: String::new(),
                        tool_choice_override: String::new(),
                    }
                } else {
                    TurnStateDecision {
                        action: TurnAction::Continue,
                        status_text: format!("[回合状态 {}/{}]", ctx.round, ctx.max_rounds),
                        tool_choice_override: String::new(),
                    }
                }
            }
        }
        let (runner, gw, _transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_segmented","arguments":"{\"text\":\"嗨\"}"}}]},"finish_reason":"tool_calls"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 10,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &StopAfterOne);
        assert_eq!(result.finished_reason, "state_stop");
        assert_eq!(result.rounds_used, 2);
    }

    /// 决策 1 测试：A 存储 —— 相同 status_text 只注入一次（避免刷屏）
    #[test]
    fn status_message_injected_only_once_when_unchanged() {
        struct StaticStatus;
        impl TurnStateController for StaticStatus {
            fn on_round(&self, _ctx: TurnStateContext) -> TurnStateDecision {
                TurnStateDecision {
                    action: TurnAction::Continue,
                    status_text: "[回合状态] 状态固定，无变化".to_string(),
                    tool_choice_override: String::new(),
                }
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_segmented","arguments":"{\"text\":\"嗨\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的呢"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &StaticStatus);
        let requests = transport.requests.lock().unwrap();
        // 每轮请求携带完整 messages，验证每个请求内状态消息不超过 1 条（相同文本不重复注入）
        for req_str in requests.iter() {
            let v: serde_json::Value = serde_json::from_str(&req_str.2).unwrap();
            let msgs = v.get("messages").and_then(|m| m.as_array()).unwrap();
            let status_count = msgs
                .iter()
                .filter(|m| {
                    m.get("role").and_then(|r| r.as_str()) == Some("system")
                        && m.get("content").and_then(|c| c.as_str()).unwrap_or("").contains("[回合状态")
                })
                .count();
            assert!(status_count <= 1, "相同状态文本不应重复注入: {v}");
        }
    }

    /// 决策 1 测试：默认状态机累计产出字数并反映在状态文本中
    #[test]
    fn default_machine_tracks_chars() {
        let machine = DefaultTurnStateMachine;
        let d = machine.on_round(TurnStateContext {
            round: 3,
            max_rounds: 5,
            tool_calls_used: 2,
            text_chars_total: 46,
            finished_reason: String::new(),
            last_tool_results: String::new(),
        });
        assert_eq!(d.action, TurnAction::Continue);
        assert!(d.status_text.contains("[回合状态"));
        assert!(d.status_text.contains("已调用 2 次工具"));
        assert!(d.status_text.contains("已输出 46 字"));
        // Issue #2 症状 2 回归：不得出现「轮次/上限」分母
        assert!(
            !d.status_text.contains("3/5"),
            "状态文案不得暴露轮次分母: {}",
            d.status_text
        );
        assert!(d.tool_choice_override.is_empty());
    }

    /// 全局工具注册：register_global_tools 后 run_turn 会携带定义且执行走 ToolHost
    #[test]
    fn global_tools_are_registered_and_executed() {
        struct GlobalHost;
        impl ToolHost for GlobalHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                format!("executed:{name}")
            }
        }
        // 全局工具应出现在请求 JSON 的 tools 中，且执行走 ToolHost
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"order_coffee","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的"},"finish_reason":"stop"}]}"#,
        ]);
        runner.register_global_tools(vec![ToolDefinition {
            name: "order_coffee".to_string(),
            description: "下单咖啡".to_string(),
            parameters_json: r#"{"type":"object","properties":{}}"#.to_string(),
            category: ToolCategory::Commerce,
            toolsets: vec!["commerce".to_string()],
            available: true,
        }]);
        let defs = runner.global_tool_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "order_coffee");
        // Approval 门：Commerce 类工具需确认——本测试聚焦全局工具注册与执行链，先预授权
        runner.approve_tool("order_coffee".to_string(), "{}".to_string());

        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: "required".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        runner.run_turn_inner(&req, Some(1), &gw, None, &GlobalHost, &DefaultTurnStateMachine);
        let requests = transport.requests.lock().unwrap();
        let first_body = serde_json::from_str::<serde_json::Value>(&requests[0].2).unwrap();
        let tools = first_body.get("tools").and_then(|t| t.as_array()).unwrap();
        assert!(
            tools.iter().any(|t| t.get("function").and_then(|f| f.get("name")).and_then(|n| n.as_str()) == Some("order_coffee")),
            "全局工具应注入请求: {first_body}"
        );
        // 工具结果经 ToolHost 回灌为 TOOL 角色消息（第二轮请求）
        let second_body = serde_json::from_str::<serde_json::Value>(&requests[1].2).unwrap();
        let msgs = second_body.get("messages").and_then(|m| m.as_array()).unwrap();
        assert!(
            msgs.iter().any(|m| m.get("role").and_then(|r| r.as_str()) == Some("tool")
                && m.get("content").and_then(|c| c.as_str()).unwrap_or("").contains("executed:order_coffee")),
            "工具结果应回灌: {second_body}"
        );

        // 注销后不再出现
        runner.unregister_global_tool("order_coffee".to_string());
        assert!(runner.global_tool_definitions().is_empty());
    }

    struct ChannelSendHost {
        result: String,
    }
    impl ToolHost for ChannelSendHost {
        fn execute(&self, _name: String, _args: String, _ctx: String) -> String {
            self.result.clone()
        }
    }

    fn channel_send_request() -> AgentTurnRequest {
        AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        }
    }

    fn register_channel_send_tool(runner: &AgentRuntime) {
        runner.register_global_tools(vec![ToolDefinition {
            name: "send_channel_message".to_string(),
            description: "发送通道消息".to_string(),
            parameters_json: r#"{"type":"object","properties":{}}"#.to_string(),
            category: ToolCategory::General,
            toolsets: vec!["channel".to_string()],
            available: true,
        }]);
    }

    #[test]
    fn successful_channel_send_cannot_be_reported_as_failure() {
        let (runner, gw, _transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"send-1","type":"function","function":{"name":"send_channel_message","arguments":"{\"channelKey\":\"qqbot\",\"target\":\"group\",\"text\":\"测试\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"还是不行呢，这回直接报错了，消息没发出去。"},"finish_reason":"stop"}]}"#,
        ]);
        register_channel_send_tool(&runner);
        let host = ChannelSendHost {
            result: r#"{"ok":true,"send_succeeded":true,"status":"sent","delivery_status":"unknown"}"#.to_string(),
        };

        let result = runner.run_turn_inner(
            &channel_send_request(), Some(1), &gw, None, &host, &DefaultTurnStateMachine,
        );

        assert_eq!(result.finished_reason, "completed");
        assert_eq!(result.final_text, "消息已经通过通道发出去了；通道没有对端送达回执。");
        let visible: Vec<&str> = result.events.iter()
            .filter(|e| e.kind == "bubble")
            .map(|e| e.text.as_str())
            .collect();
        assert_eq!(visible, vec!["消息已经通过通道发出去了；通道没有对端送达回执。"]);
    }

    #[test]
    fn failed_channel_send_keeps_model_failure_statement() {
        let (runner, gw, _transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"send-2","type":"function","function":{"name":"send_channel_message","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"发送失败了，请稍后重试。"},"finish_reason":"stop"}]}"#,
        ]);
        register_channel_send_tool(&runner);
        let host = ChannelSendHost {
            result: r#"{"ok":false,"status":"failed","reason":"通道未连接"}"#.to_string(),
        };

        let result = runner.run_turn_inner(
            &channel_send_request(), Some(1), &gw, None, &host, &DefaultTurnStateMachine,
        );

        assert_eq!(result.final_text, "发送失败了，请稍后重试。");
        assert!(result.events.iter().any(|e| e.kind == "bubble" && e.text.contains("发送失败")));
    }

    /// image / system_prompt / companion_name_map 字段应影响请求组装
    #[test]
    fn request_extra_fields_reach_gateway() {
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"收到"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: Some(7),
            history_json: r#"[{"role":"user","content":"今天去哪玩"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: "auto".to_string(),
            sticker_probability: 30,
            image: Some(ImageInput {
                path: Some("/tmp/pic.jpg".to_string()),
                base64_data: None,
                mime_type: None,
            }),
            system_prompt: Some("群聊人设".to_string()),
            companion_name_map_json: Some(r#"{"1":"小爱","2":"小美"}"#.to_string()),
        };
        runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        let requests = transport.requests.lock().unwrap();
        let first = serde_json::from_str::<serde_json::Value>(&requests[0].2).unwrap();
        let msgs = first.get("messages").and_then(|m| m.as_array()).unwrap();
        // system_prompt 覆盖原样透传；成员映射由编排器 L0 环境层负责（gateway 不再追加）
        let sys = msgs[0].get("content").and_then(|c| c.as_str()).unwrap_or("");
        assert_eq!(sys, "群聊人设", "system_prompt 应原样透传: {first}");
        // image：路径不存在 → content 数组含加载失败占位（user 消息转数组）
        let user_msg = msgs
            .iter()
            .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
            .expect("应存在 user 消息");
        let content_arr = user_msg.get("content").and_then(|c| c.as_array()).expect("image 场景 content 应为数组");
        let joined = content_arr
            .iter()
            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("");
        assert!(joined.contains("今天去哪玩"), "原文本应保留: {joined}");
        assert!(joined.contains("图片加载失败"), "图片加载失败占位应出现: {joined}");
    }

    /// 流式：SSE 增量经 sink 实时回调（Rust 原生 SSE 解析）
    #[test]
    fn stream_round_trips_through_sink() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CollectSink {
            deltas: Mutex<Vec<String>>,
            done: AtomicUsize,
        }
        impl StreamSink for CollectSink {
            fn on_text_delta(&self, text: String) {
                self.deltas.lock().unwrap().push(text);
            }
            fn on_reasoning_delta(&self, _text: String) {}
            fn on_done(&self, _full_text: String, _finish_reason: String) {
                self.done.fetch_add(1, Ordering::SeqCst);
            }
            fn on_error(&self, _error: String) {}
        }

        let (runner, gw, _transport) = mock_gateway(vec![r#"{"content":"你好呀","finish_reason":"stop"}"#]);
        let sink = Arc::new(CollectSink {
            deltas: Mutex::new(Vec::new()),
            done: AtomicUsize::new(0),
        });
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, Some(sink.clone()), &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed");
        assert_eq!(result.final_text, "你好呀");
        let deltas = sink.deltas.lock().unwrap();
        assert_eq!(*deltas, vec!["你好呀".to_string()]);
        assert_eq!(sink.done.load(Ordering::SeqCst), 1);
    }

    /// 方案 A 集成：core-tools 插件工具 core_status 出现在请求 tools 中，
    /// 且模型调用时走核心插件执行器（不回调 ToolHost）。
    #[test]
    fn core_plugin_tool_reaches_gateway_and_executes() {
        struct NeverHost;
        impl ToolHost for NeverHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                panic!("核心插件工具不应回调 ToolHost: {name}")
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"core_status","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#,
            r#"{"choices":[{"message":{"role":"assistant","content":"好的"},"finish_reason":"stop"}]}"#,
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"你好"}]"#.to_string(),
            tools: vec![],
            max_rounds: 4,
            tool_choice: "auto".to_string(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NeverHost, &DefaultTurnStateMachine);
        assert!(result.error.is_none(), "回合不应失败: {:?}", result.error);
        assert_eq!(result.finished_reason, "completed");

        let requests = transport.requests.lock().unwrap();
        let first_body: serde_json::Value = serde_json::from_str(&requests[0].2).unwrap();
        let names: Vec<String> = first_body["tools"].as_array().unwrap().iter()
            .map(|t| t["function"]["name"].as_str().unwrap_or("").to_string())
            .collect();
        assert!(names.contains(&"core_status".to_string()), "tools 应含 core_status: {names:?}");

        let snap = runner.core_plugin_snapshot();
        assert!(snap.contains("core_status"), "快照: {snap}");
    }

    /// Approval：Commerce 类工具默认 RequireConfirm —— 未批准时停止回合（confirm_pending + confirm_request 事件），工具不执行
    #[test]
    fn commerce_tool_requires_confirmation() {
        struct NeverHost;
        impl ToolHost for NeverHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                panic!("未批准的 Commerce 工具不应执行: {name}")
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![]);
        runner.register_global_tools(vec![ToolDefinition {
            name: "commerce_buy".to_string(),
            description: "购买".to_string(),
            parameters_json: r#"{"type":"object","properties":{"sku":{"type":"string"}}}"#.to_string(),
            category: ToolCategory::Commerce,
            toolsets: vec!["commerce".to_string()],
            available: true,
        }]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"买一个"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let raw = r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"commerce_buy","arguments":"{\"sku\":\"A1\"}"}}]},"finish_reason":"tool_calls"}]}"#;
        transport.responses.lock().unwrap().push(raw.to_string());
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NeverHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "confirm_pending");
        assert!(result.error.is_none());
        assert!(result.events.iter().any(|e| e.kind == "confirm_request" && e.text == "commerce_buy"), "应产出 confirm_request 事件");
        assert!(result.events.iter().all(|e| e.kind != "bubble"), "确认前不应落地气泡");
    }

    /// Approval：approve_tool 后重跑回合 → Commerce 工具执行（一次性授权）
    #[test]
    fn approved_commerce_tool_executes_once() {
        struct RecordingHost {
            called: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl ToolHost for RecordingHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                self.called.fetch_add(1, Ordering::SeqCst);
                format!("executed:{name}")
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![]);
        runner.register_global_tools(vec![ToolDefinition {
            name: "commerce_buy".to_string(),
            description: "购买".to_string(),
            parameters_json: r#"{"type":"object","properties":{"sku":{"type":"string"}}}"#.to_string(),
            category: ToolCategory::Commerce,
            toolsets: vec!["commerce".to_string()],
            available: true,
        }]);
        runner.approve_tool("commerce_buy".to_string(), r#"{"sku":"A1"}"#.to_string());
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"买一个"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let raw = r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"commerce_buy","arguments":"{\"sku\":\"A1\"}"}}]},"finish_reason":"tool_calls"}]}"#;
        transport.responses.lock().unwrap().push(raw.to_string());
        transport.responses.lock().unwrap().push(r#"{"choices":[{"message":{"role":"assistant","content":"已下单"},"finish_reason":"stop"}]}"#.to_string());
        let called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let host = RecordingHost { called: called.clone() };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &host, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed", "{:?}", result.error);
        assert_eq!(result.final_text, "已下单");
        assert_eq!(called.load(Ordering::SeqCst), 1, "批准后工具应执行一次");
    }

    /// Approval：reject_tool 后重跑回合 → 不执行、回灌「用户拒绝」、不再次触发确认
    #[test]
    fn rejected_commerce_tool_feeds_back_without_executing() {
        struct NeverHost;
        impl ToolHost for NeverHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                panic!("被拒绝的工具不应执行: {name}")
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![]);
        runner.register_global_tools(vec![ToolDefinition {
            name: "commerce_buy".to_string(),
            description: "购买".to_string(),
            parameters_json: r#"{"type":"object","properties":{"sku":{"type":"string"}}}"#.to_string(),
            category: ToolCategory::Commerce,
            toolsets: vec!["commerce".to_string()],
            available: true,
        }]);
        runner.reject_tool("commerce_buy".to_string(), r#"{"sku":"A1"}"#.to_string());
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"买一个"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let raw = r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"commerce_buy","arguments":"{\"sku\":\"A1\"}"}}]},"finish_reason":"tool_calls"}]}"#;
        transport.responses.lock().unwrap().push(raw.to_string());
        transport.responses.lock().unwrap().push(r#"{"choices":[{"message":{"role":"assistant","content":"好的，不买了"},"finish_reason":"stop"}]}"#.to_string());
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NeverHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "completed");
        assert_eq!(result.final_text, "好的，不买了");
        assert!(result.events.iter().all(|e| e.kind != "confirm_request"), "拒绝后不应再次触发确认");
    }

    /// Approval：拒绝必须是一次性的——消费后同名同参调用再次进入确认门，
    /// 否则一次误点/一次通道自动拒绝会让该工具被永久拒绝。
    #[test]
    fn rejection_is_consumed_after_one_turn() {
        struct NeverHost;
        impl ToolHost for NeverHost {
            fn execute(&self, name: String, _args: String, _ctx: String) -> String {
                panic!("被拒绝的工具不应执行: {name}")
            }
        }
        let (runner, gw, transport) = mock_gateway(vec![]);
        runner.register_global_tools(vec![ToolDefinition {
            name: "commerce_buy".to_string(),
            description: "购买".to_string(),
            parameters_json: r#"{"type":"object","properties":{"sku":{"type":"string"}}}"#.to_string(),
            category: ToolCategory::Commerce,
            toolsets: vec!["commerce".to_string()],
            available: true,
        }]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"买一个"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let call = r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"commerce_buy","arguments":"{\"sku\":\"A1\"}"}}]},"finish_reason":"tool_calls"}]}"#;
        // 第一次：拒绝生效并回灌 → 完成
        runner.reject_tool("commerce_buy".to_string(), r#"{"sku":"A1"}"#.to_string());
        transport.responses.lock().unwrap().push(call.to_string());
        transport.responses.lock().unwrap().push(
            r#"{"choices":[{"message":{"role":"assistant","content":"好的"},"finish_reason":"stop"}]}"#
                .to_string(),
        );
        let first = runner.run_turn_inner(&req, Some(1), &gw, None, &NeverHost, &DefaultTurnStateMachine);
        assert_eq!(first.finished_reason, "completed");
        assert!(
            runner.rejected_tools.lock().unwrap().is_empty(),
            "拒绝应在回灌后被消费"
        );
        // 第二次：清掉 MockTransport 的粘性遗留响应，同一调用应再次进入确认门
        transport.responses.lock().unwrap().clear();
        transport.responses.lock().unwrap().push(call.to_string());
        let second = runner.run_turn_inner(&req, Some(1), &gw, None, &NeverHost, &DefaultTurnStateMachine);
        assert_eq!(second.finished_reason, "confirm_pending", "拒绝应只生效一次");
    }

    /// 用量与思考过程必须经事件流带出（否则 token 统计为空、思考过程设置不生效）。
    #[test]
    fn turn_emits_usage_and_reasoning_events() {
        let (runner, gw, transport) = mock_gateway(vec![]);
        transport.responses.lock().unwrap().push(
            r#"{"choices":[{"message":{"role":"assistant","content":"你好","reasoning_content":"想了一下"},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}"#
                .to_string(),
        );
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        let usage = result.events.iter().find(|e| e.kind == "usage").expect("应产出 usage 事件");
        assert!(usage.text.contains("prompt_tokens"), "{}", usage.text);
        let reasoning = result.events.iter().find(|e| e.kind == "reasoning").expect("应产出 reasoning 事件");
        assert_eq!(reasoning.text, "想了一下");
        assert_eq!(result.final_text, "你好");
    }

    /// 思考模型把 max_tokens 全耗在 reasoning 上时：正文为空且无任何输出，
    /// 必须显式上报截断错误，而不是伪装成成功的 max_rounds。
    #[test]
    fn truncated_empty_turn_reports_explicit_error() {
        let (runner, gw, transport) = mock_gateway(vec![]);
        transport.responses.lock().unwrap().push(
            r#"{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"length"}]}"#
                .to_string(),
        );
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"在吗"}]"#.to_string(),
            tools: vec![],
            max_rounds: 1,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn_inner(&req, Some(1), &gw, None, &NoopHost, &DefaultTurnStateMachine);
        assert_eq!(result.finished_reason, "error");
        let err = result.error.unwrap_or_default();
        assert!(err.contains("max_tokens"), "错误应指明 max_tokens 截断: {err}");
        assert!(result.events.is_empty());
    }

    /// Eval：脚本化传输（离线 mock）——不触网，按序弹出预置响应驱动完整回合
    #[test]
    fn eval_scripted_transport_drives_full_turn_offline() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"离线评估通过"},"finish_reason":"stop"}]}"#.to_string(),
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"离线评估"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        assert_eq!(result.finished_reason, "completed", "{:?}", result.error);
        assert!(result.final_text.contains("离线评估通过"), "final: {}", result.final_text);
        // 清空后恢复默认 ureq（不在此处触网，仅验证状态可复位）
        runner.set_mock_transport(vec![]);
    }

    /// Eval：脚本化传输耗尽时返回错误而非无限等待
    #[test]
    fn eval_scripted_transport_exhaustion_returns_error() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![r#"{"choices":[{"message":{"role":"assistant","content":"一","tool_calls":[{"id":"c1","type":"function","function":{"name":"noop_tool","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#.to_string()]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"x"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        // 第二轮请求时响应耗尽 → error 非空且回合终止（不崩溃）
        assert!(result.error.is_some() || result.finished_reason == "max_rounds", "error={:?} reason={}", result.error, result.finished_reason);
        runner.set_mock_transport(vec![]);
    }

    /// 修1：模型/API 错误（finish_reason=error）不得标记为 completed
    #[test]
    fn text_branch_rejects_error_finish_reason() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![r#"{"error":{"message":"bad key"}}"#.to_string()]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"hi"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        assert_eq!(result.finished_reason, "error");
        // send 层经 extract_error_message 把 API 错误归一化为 Err（含消息），文本分支 error 判断为防御层
        let err = result.error.unwrap_or_default();
        assert!(!err.is_empty(), "error 应携带消息");
        runner.set_mock_transport(vec![]);
    }

    /// 修1：空响应（无内容无工具调用）返回 error 而非 completed
    #[test]
    fn text_branch_rejects_empty_response() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![r#"{"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}"#.to_string()]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"hi"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        assert_eq!(result.finished_reason, "error");
        let err = result.error.unwrap_or_default();
        assert!(err.contains("空响应"), "error: {err}");
        runner.set_mock_transport(vec![]);
    }

    /// 修1：length 截断仍按 max_rounds（不误判为 error），正常文本不受影响
    #[test]
    fn text_branch_keeps_length_and_normal_text_semantics() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"截断内容"},"finish_reason":"length"}]}"#.to_string(),
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"hi"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        assert_eq!(result.finished_reason, "max_rounds");
        assert_eq!(result.final_text, "截断内容");
        runner.set_mock_transport(vec![]);
    }

    /// 修1 修正：工具链已产出气泡后的空正文收尾是正常结束（不得误判 error）
    #[test]
    fn text_branch_keeps_completed_when_tools_already_emitted() {
        let (runner, _gw, _t) = mock_gateway(vec![]);
        runner.set_mock_transport(vec![
            r#"{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"c1","type":"function","function":{"name":"emit_bubble","arguments":"{\"text\":\"第一句\"}"}}]},"finish_reason":"tool_calls"}]}"#.to_string(),
            r#"{"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}"#.to_string(),
        ]);
        let req = AgentTurnRequest {
            group_id: None,
            history_json: r#"[{"role":"user","content":"hi"}]"#.to_string(),
            tools: vec![],
            max_rounds: 3,
            tool_choice: String::new(),
            sticker_probability: 0,
            image: None,
            system_prompt: None,
            companion_name_map_json: None,
        };
        let result = runner.run_turn(req, Some(1), Arc::new(NoopHost));
        assert_eq!(result.finished_reason, "completed", "{:?}", result.error);
        assert!(result.events.iter().any(|e| e.kind == "bubble" && e.text == "第一句"), "工具产出应保留");
        runner.set_mock_transport(vec![]);
    }
}
