// api_probe.rs — API 探测/网络测试（下沉 rs，T9 最小集）
//
// 设计决策（用户拍板，2026-08）：
// - 方案 A：Rust 做纯 HTTP（无状态探测），Kotlin 提供 session/密钥。
//   PARTNER 的 X-LianYu-Session / X-LianYu-Client-Id 由 Kotlin 侧
//   RemoteKeyProvider.ensureSession 取好后经 extra_headers 传入；
//   Rust 不持久化任何凭证。
// - 证书固定：接受系统根证书（ureq rustls 默认），不移植 SPKI pinning。
// - 最小集：只下沉 SettingsViewModel 实际调用的函数：
//   fetch_models / query_balance / test_openai / test_anthropic
//   + 静态纯函数（uses_anthropic_protocol / supports_openai_model_list /
//     family_balanced_random / requires_fixed_temperature）。
// - 删除 AiService / AiServiceProvider / AiToolLoopRunner 后由本模块接管
//   设置页的「测试连接 / 模型列表 / 余额查询」。

use std::sync::Arc;

use serde_json::{json, Value};

use crate::native_gateway::{HttpTransport, UreqTransport};

/// API 探测配置（Kotlin ApiConfig 的精简快照）
#[derive(Clone, Debug, uniffi::Record)]
pub struct ApiProbeConfig {
    /// 提供商名（ApiProvider.name，如 OPENAI / ANTHROPIC / PARTNER / CUSTOM / XIAOMI）
    pub provider: String,
    /// baseUrl（无尾部斜杠要求）
    pub base_url: String,
    /// 主 API Key（PARTNER 模式下可为空，keys 单独传入）
    pub api_key: String,
    /// 模型名
    pub model: String,
    /// 温度
    pub temperature: f64,
    /// maxTokens（Anthropic 测试固定 5，OpenAI 测试固定 1）
    pub max_tokens: Option<i64>,
    /// 格式提示（"anthropic" / "openai" / ""）
    pub format_hint: String,
}

/// 探测用消息（role + content）
#[derive(Clone, Debug, uniffi::Record)]
pub struct ProbeMessage {
    pub role: String,
    pub content: String,
}

/// HTTP 头（uniffi 不支持元组，用 Record 表达）
#[derive(Clone, Debug, uniffi::Record)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

impl HttpHeader {
    fn into_pair(self) -> (String, String) {
        (self.name, self.value)
    }
}

/// 探测错误（flat error：仅 lower to_string() 文本）
///
/// uniffi 0.29 不支持 `Result<T, String>` 直接导出（Kotlin 生成器只认
/// Enum/Object 错误类型），故定义本枚举承载错误文本，Kotlin 侧抛出
/// `ApiProbeException(message)`。
#[derive(Debug, uniffi::Error)]
#[uniffi(flat_error)]
pub enum ApiProbeError {
    Message(String),
}

impl std::fmt::Display for ApiProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiProbeError::Message(msg) => write!(f, "{msg}"),
        }
    }
}

impl From<String> for ApiProbeError {
    fn from(s: String) -> Self {
        ApiProbeError::Message(s)
    }
}

impl From<&str> for ApiProbeError {
    fn from(s: &str) -> Self {
        ApiProbeError::Message(s.to_string())
    }
}

/// 余额信息（与 Kotlin AiService.BalanceInfo 同构）
#[derive(Clone, Debug, uniffi::Record)]
pub struct BalanceInfo {
    pub total_limit: Option<f64>,
    pub total_used: Option<f64>,
    pub total_available: Option<f64>,
    pub remaining_balance: Option<f64>,
    pub raw_subscription: Option<String>,
    pub raw_usage: Option<String>,
}

/// 探测服务对象（默认 ureq 传输；测试可注入 mock）
#[derive(uniffi::Object)]
pub struct ApiProbe {
    transport: Arc<dyn HttpTransport>,
}

#[uniffi::export]
impl ApiProbe {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            transport: Arc::new(UreqTransport),
        })
    }

    /// 拉取模型列表（GET /models）
    ///
    /// 认证规则（对齐 Kotlin fetchModels）：
    /// - PARTNER：extra_headers 需含 X-LianYu-Session / X-LianYu-Client-Id；
    /// - XIAOMI：api-key 头；
    /// - 其他：Authorization: Bearer。
    /// HTML 响应 → "该API不支持模型列表查询"。
    pub fn fetch_models(
        &self,
        cfg: ApiProbeConfig,
        extra_headers: Vec<HttpHeader>,
    ) -> Result<Vec<String>, ApiProbeError> {
        let base_url = normalize_base_url(&cfg.base_url);
        let url = format!("{base_url}/models");
        let extra: Vec<(String, String)> = extra_headers.into_iter().map(|h| h.into_pair()).collect();
        let headers = auth_headers(&cfg, &cfg.api_key, &extra);
        let body = self.transport.get_json(&url, &headers)?;
        if looks_like_html(&body) {
            return Err("该API不支持模型列表查询".to_string().into());
        }
        let v: Value = serde_json::from_str(&body)
            .map_err(|e| format!("解析模型列表失败: {e}"))?;
        let mut models: Vec<String> = Vec::new();
        if let Some(data) = v.get("data").and_then(|d| d.as_array()) {
            for item in data {
                if let Some(id) = item.get("id").and_then(|i| i.as_str()) {
                    if !id.trim().is_empty() {
                        models.push(id.trim().to_string());
                    }
                }
            }
        }
        Ok(models)
    }

    /// 查询余额（GET subscription + usage 四端点循环，对齐 Kotlin queryBalanceInternal）
    ///
    /// - keys：要尝试的 API Key 列表（PARTNER 由 Kotlin 侧 fetchKeysAsync 取好后传入）
    /// - extra_headers：PARTNER 的 session 头等
    pub fn query_balance(
        &self,
        cfg: ApiProbeConfig,
        keys: Vec<String>,
        extra_headers: Vec<HttpHeader>,
    ) -> Result<BalanceInfo, ApiProbeError> {
        let base_url = normalize_base_url(&cfg.base_url);
        let extra: Vec<(String, String)> = extra_headers.into_iter().map(|h| h.into_pair()).collect();
        let keys = if keys.is_empty() {
            if cfg.api_key.trim().is_empty() {
                return Err("没有可用的 API Key".to_string().into());
            }
            vec![cfg.api_key.trim().to_string()]
        } else {
            keys
        };

        let mut total_limit: Option<f64> = None;
        let mut total_used: Option<f64> = None;
        let mut total_available: Option<f64> = None;
        let mut raw_sub: Option<String> = None;
        let mut raw_usage: Option<String> = None;

        'keys: for key in &keys {
            let sub_endpoints = [
                "/dashboard/billing/subscription",
                "/v1/dashboard/billing/subscription",
            ];
            for endpoint in sub_endpoints {
                let url = format!("{base_url}{endpoint}");
                let mut headers: Vec<(String, String)> = Vec::new();
                headers.push(("Authorization".to_string(), format!("Bearer {key}")));
                // 丢弃 X-LianYu-* 会话头：常规接口只走 Bearer，避免触发服务端 session 路径
                headers.extend(extra.iter().filter(|(k, _)| !is_lianyu_session_header(k)).cloned());
                if let Ok(body) = self.transport.get_json(&url, &headers) {
                    raw_sub = Some(body.clone());
                    if let Ok(v) = serde_json::from_str::<Value>(&body) {
                        let hard_limit = v.get("hard_limit_usd").and_then(|x| x.as_f64());
                        let granted = v.get("total_granted").and_then(|x| x.as_f64());
                        if let Some(limit) = hard_limit.or(granted) {
                            if limit > 0.0 {
                                total_limit = Some(limit);
                            }
                        }
                        if let Some(used) = v.get("total_used").and_then(|x| x.as_f64()) {
                            total_used = Some(used);
                        }
                        if let Some(avail) = v.get("total_available").and_then(|x| x.as_f64()) {
                            if avail > 0.0 {
                                total_available = Some(avail);
                            }
                        }
                        if total_limit.is_some() || total_available.is_some() {
                            break;
                        }
                    }
                }
            }

            let usage_endpoints = [
                "/dashboard/billing/usage",
                "/v1/dashboard/billing/usage",
            ];
            for endpoint in usage_endpoints {
                let url = format!("{base_url}{endpoint}");
                let mut headers: Vec<(String, String)> = Vec::new();
                headers.push(("Authorization".to_string(), format!("Bearer {key}")));
                // 丢弃 X-LianYu-* 会话头：常规接口只走 Bearer，避免触发服务端 session 路径
                headers.extend(extra.iter().filter(|(k, _)| !is_lianyu_session_header(k)).cloned());
                if let Ok(body) = self.transport.get_json(&url, &headers) {
                    raw_usage = Some(body.clone());
                    if let Ok(v) = serde_json::from_str::<Value>(&body) {
                        if let Some(total_usage) = v.get("total_usage").and_then(|x| x.as_f64()) {
                            if total_usage >= 0.0 {
                                total_used = Some(total_usage / 100.0);
                            }
                        }
                        break;
                    }
                }
            }

            if total_limit.is_some() || total_available.is_some() {
                break 'keys;
            }
        }

        let remaining = match total_available {
            Some(avail) => Some(avail),
            None => match (total_limit, total_used) {
                (Some(limit), Some(used)) => Some(limit - used),
                _ => None,
            },
        };

        Ok(BalanceInfo {
            total_limit,
            total_used,
            total_available,
            remaining_balance: remaining,
            raw_subscription: raw_sub,
            raw_usage,
        })
    }

    /// 轻量 OpenAI 兼容测试（对齐 Kotlin callOpenAiCompatibleForTest）
    ///
    /// - POST /chat/completions，stream=false，temperature=0.0（除非固定温度模型），
    ///   max_tokens=1（XIAOMI 用 max_completion_tokens）
    /// - keys：要尝试的 Key 列表（Kotlin resolveKeysWithPartnerFallback 产物）
    pub fn test_openai(
        &self,
        cfg: ApiProbeConfig,
        keys: Vec<String>,
        messages: Vec<ProbeMessage>,
        extra_headers: Vec<HttpHeader>,
    ) -> Result<String, ApiProbeError> {
        let base_url = normalize_base_url(&cfg.base_url);
        let url = format!("{base_url}/chat/completions");
        let extra: Vec<(String, String)> = extra_headers.into_iter().map(|h| h.into_pair()).collect();
        let keys = if keys.is_empty() {
            vec![cfg.api_key.trim().to_string()]
        } else {
            keys
        };
        let mut last_err: Option<String> = None;

        for key in &keys {
            let msgs: Vec<Value> = messages
                .iter()
                .map(|m| json!({"role": m.role, "content": m.content}))
                .collect();
            let mut body = json!({
                "model": cfg.model,
                "messages": msgs,
                "stream": false,
            });
            if !api_requires_fixed_temperature(cfg.model.clone()) {
                body["temperature"] = json!(0.0);
            }
            let max_param = if cfg.provider.eq_ignore_ascii_case("XIAOMI") {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
            body[max_param] = json!(1);

            let mut headers = Vec::new();
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
            headers.extend(auth_headers(&cfg, key, &extra));

            match self.transport.post_json(&url, &headers, &body.to_string(), false, None) {
                Ok(resp_body) => {
                    if looks_like_html(&resp_body) {
                        last_err = Some("服务器返回了网页而非API响应，请检查API密钥/地址是否正确".to_string());
                        continue;
                    }
                    match serde_json::from_str::<Value>(&resp_body) {
                        Ok(v) => {
                            if let Some(err) = v.get("error") {
                                let msg = err
                                    .get("message")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("API返回错误")
                                    .to_string();
                                last_err = Some(msg);
                                continue;
                            }
                            let content = v
                                .get("choices")
                                .and_then(|c| c.as_array())
                                .and_then(|a| a.first())
                                .and_then(|ch| ch.get("message"))
                                .and_then(|m| m.get("content"))
                                .and_then(|c| c.as_str())
                                .unwrap_or("")
                                .to_string();
                            // 注意：返回 raw content；思考剥离由 Kotlin 侧 ResponsePostProcessor 完成（对齐旧行为）
                            return Ok(content);
                        }
                        Err(_) => {
                            last_err = Some("HTTP 200: 服务器返回错误页面".to_string());
                            continue;
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| "所有 API Key 均请求失败".to_string()).into())
    }

    /// Anthropic 协议测试（对齐 Kotlin callAnthropicForTest）
    ///
    /// - POST /messages，max_tokens=5，temperature=cfg.temperature
    /// - headers：x-api-key + anthropic-version: 2023-06-01
    pub fn test_anthropic(
        &self,
        cfg: ApiProbeConfig,
        keys: Vec<String>,
        messages: Vec<ProbeMessage>,
        system_prompt: String,
        extra_headers: Vec<HttpHeader>,
    ) -> Result<String, ApiProbeError> {
        let base_url = normalize_base_url(&cfg.base_url);
        let url = format!("{base_url}/messages");
        let extra: Vec<(String, String)> = extra_headers.into_iter().map(|h| h.into_pair()).collect();
        let keys = if keys.is_empty() {
            vec![cfg.api_key.trim().to_string()]
        } else {
            keys
        };
        let mut last_err: Option<String> = None;

        for key in &keys {
            let anthro_messages: Vec<Value> = messages
                .iter()
                .filter(|m| m.role != "system")
                .map(|m| {
                    let role = if m.role == "user" { "user" } else { "assistant" };
                    json!({"role": role, "content": m.content})
                })
                .collect();
            let body = json!({
                "model": cfg.model,
                "messages": anthro_messages,
                "system": system_prompt,
                "max_tokens": 5,
                // 同生成路径：连接测试的 temperature 同样必须 2 位小数（见 temperature.rs）
                "temperature": crate::temperature::normalize_temperature(cfg.temperature),
            });

            let mut headers = Vec::new();
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
            headers.push(("x-api-key".to_string(), key.to_string()));
            headers.push(("anthropic-version".to_string(), "2023-06-01".to_string()));
            headers.extend(extra.iter().cloned());

            match self.transport.post_json(&url, &headers, &body.to_string(), false, None) {
                Ok(resp_body) => match serde_json::from_str::<Value>(&resp_body) {
                    Ok(v) => {
                        if let Some(err) = v.get("error") {
                            let msg = err
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("API返回错误")
                                .to_string();
                            last_err = Some(msg);
                            continue;
                        }
                        let text = v
                            .get("content")
                            .and_then(|c| c.as_array())
                            .and_then(|a| a.first())
                            .and_then(|b| b.get("text"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("")
                            .to_string();
                        if text.is_empty() {
                            last_err = Some("API返回空内容".to_string());
                            continue;
                        }
                        // 注意：返回 raw content；思考剥离由 Kotlin 侧 ResponsePostProcessor 完成
                        return Ok(text);
                    }
                    Err(_) => {
                        last_err = Some("解析 Anthropic 响应失败".to_string());
                        continue;
                    }
                },
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| "所有 API Key 均请求失败".to_string()).into())
    }
}

// ── 静态纯函数（替代 AiService companion object 中 SettingsViewModel 使用面） ──

/// 是否使用 Anthropic 协议（对齐 Kotlin AiService.usesAnthropicProtocol）
#[uniffi::export]
pub fn api_uses_anthropic_protocol(provider: String, format_hint: String) -> bool {
    provider.eq_ignore_ascii_case("ANTHROPIC")
        || (provider.eq_ignore_ascii_case("CUSTOM") && format_hint.eq_ignore_ascii_case("anthropic"))
}

/// 是否支持 OpenAI 模型列表（对齐 Kotlin AiService.supportsOpenAiModelList）
#[uniffi::export]
pub fn api_supports_openai_model_list(provider: String, format_hint: String) -> bool {
    !api_uses_anthropic_protocol(provider, format_hint)
}

/// 模型是否要求固定温度（kimi-k2.6，对齐 Kotlin AiService.requiresFixedTemperature）
#[uniffi::export]
pub fn api_requires_fixed_temperature(model: String) -> bool {
    let lower = model.to_lowercase();
    lower.contains("kimi-k2.6") || lower.contains("k2.6")
}

/// 家族均衡随机（对齐 Kotlin AiService.familyBalancedRandom）
#[uniffi::export]
pub fn api_family_balanced_random(candidates: Vec<String>) -> String {
    if candidates.len() <= 1 {
        return candidates.first().cloned().unwrap_or_default();
    }
    const FAMILY_KEYWORDS: [&str; 12] = [
        "deepseek", "qwen", "glm", "kimi", "moonshot", "gpt", "claude", "gemini", "yi-",
        "ernie", "hunyuan", "doubao",
    ];
    // 按首个匹配家族分组（保序）
    let mut order: Vec<String> = Vec::new();
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for m in &candidates {
        let family = FAMILY_KEYWORDS
            .iter()
            .find(|k| m.to_lowercase().contains(**k))
            .map(|s| s.to_string())
            .unwrap_or_else(|| "other".to_string());
        if let Some((_, pool)) = groups.iter_mut().find(|(f, _)| f == &family) {
            pool.push(m.clone());
        } else {
            order.push(family.clone());
            groups.push((family, vec![m.clone()]));
        }
    }
    let family_idx = urandom_int(order.len() as u32) as usize;
    let pool = &groups[family_idx.min(groups.len() - 1)].1;
    let idx = urandom_int(pool.len() as u32) as usize;
    pool.get(idx).cloned().unwrap_or_default()
}

// ── 内部工具 ──

fn normalize_base_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "https://api.openai.com/v1".to_string()
    } else {
        trimmed.to_string()
    }
}

/// 组装认证头（对齐 Kotlin addProviderAuthHeaders + fetchModels PARTNER 分支）
/// 规则（对应 suflow-api 服务端 auth.rs authenticate() 分发）：
///   - PARTNER（内置 Clove API）→ 仅 session/clientId 头（Kotlin 经 extra_headers 传入）
///   - 常规 provider（含 suflow CUSTOM，OpenAI 规范接口）→ Bearer <key>，
///     绝不附加 X-LianYu-Session / X-LianYu-Client-Id：一旦携带这些头，服务端
///     authenticate() 会优先走 session 路径并强制设备签名验证（verify_device_signature，
///     要求 X-LianYu-Sig-Version 等 7 个头），ureq 无签名 → 401 "Unsupported signature version"
fn auth_headers(
    cfg: &ApiProbeConfig,
    key: &str,
    extra_headers: &[(String, String)],
) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();
    if cfg.provider.eq_ignore_ascii_case("PARTNER") {
        // session/clientId 由 Kotlin 侧经 extra_headers 传入
        for (k, v) in extra_headers {
            headers.push((k.clone(), v.clone()));
        }
    } else if cfg.provider.eq_ignore_ascii_case("XIAOMI") {
        headers.push(("api-key".to_string(), key.to_string()));
    } else {
        headers.push(("Authorization".to_string(), format!("Bearer {key}")));
    }
    // 补充额外头：非 PARTNER 丢弃 X-LianYu-* 会话头（防御：常规接口只走 Bearer，
    // 避免触发服务端 session 认证路径 + 设备签名验证）
    if !cfg.provider.eq_ignore_ascii_case("PARTNER") {
        for (k, v) in extra_headers {
            if is_lianyu_session_header(k) {
                continue;
            }
            if !headers.iter().any(|(hk, _)| hk.eq_ignore_ascii_case(k)) {
                headers.push((k.clone(), v.clone()));
            }
        }
    }
    headers
}

/// PARTNER 专属会话头（服务端 authenticate() 一旦检测到即劫持认证路径）。
fn is_lianyu_session_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("X-LianYu-Session") || name.eq_ignore_ascii_case("X-LianYu-Client-Id")
}

/// 响应是否为 HTML 错误页（对齐 Kotlin ensureNotHtml）
fn looks_like_html(body: &str) -> bool {
    let trimmed = body.trim_start();
    trimmed.starts_with("<!") || trimmed.to_lowercase().starts_with("<html")
}

/// 从 /dev/urandom 读取 4 字节 → 0..bound（对齐 Kotlin AiService.urandomInt）
fn urandom_int(bound: u32) -> u32 {
    if bound <= 1 {
        return 0;
    }
    let mut buf = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        if f.read_exact(&mut buf).is_ok() {
            let raw = ((buf[0] as u32) << 24)
                | ((buf[1] as u32) << 16)
                | ((buf[2] as u32) << 8)
                | (buf[3] as u32);
            return (raw & i32::MAX as u32) % bound;
        }
    }
    // fallback：时间戳哈希（测试/无 /dev/urandom 环境）
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    ((nanos as u32) & i32::MAX as u32) % bound
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_gateway::testutil::MockTransport;

    #[test]
    fn api_uses_anthropic_protocol_detects() {
        assert!(api_uses_anthropic_protocol("ANTHROPIC".into(), "".into()));
        assert!(api_uses_anthropic_protocol("CUSTOM".into(), "anthropic".into()));
        assert!(!api_uses_anthropic_protocol("OPENAI".into(), "".into()));
        assert!(!api_uses_anthropic_protocol("CUSTOM".into(), "openai".into()));
    }

    #[test]
    fn api_requires_fixed_temperature_detects_kimi() {
        assert!(api_requires_fixed_temperature("kimi-k2.6-turbo".into()));
        assert!(api_requires_fixed_temperature("Moonshot-k2.6".into()));
        assert!(!api_requires_fixed_temperature("gpt-4o".into()));
    }

    #[test]
    fn family_balanced_random_returns_from_pool() {
        let pool = vec![
            "deepseek-v3".to_string(),
            "deepseek-r1".to_string(),
            "qwen-max".to_string(),
            "gpt-4o".to_string(),
        ];
        let pick = api_family_balanced_random(pool.clone());
        assert!(pool.contains(&pick));
        // 单元素直接返回
        assert_eq!(api_family_balanced_random(vec!["only".to_string()]), "only");
        // 空池返回空串
        assert_eq!(api_family_balanced_random(vec![]), "");
    }

    #[test]
    fn fetch_models_parses_ids() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"object":"list","data":[{"id":"gpt-4o"},{"id":"gpt-4o-mini"}]}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let models = probe.fetch_models(cfg, vec![]).unwrap();
        assert_eq!(models, vec!["gpt-4o", "gpt-4o-mini"]);
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].0.ends_with("/models"));
        assert!(reqs[0].1.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-test"));
    }

    #[test]
    fn fetch_models_html_returns_friendly_error() {
        let transport = Arc::new(MockTransport::new(vec![
            "<html><body>Unauthorized</body></html>",
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://x.x".into(),
            api_key: "bad".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let err = probe.fetch_models(cfg, vec![]).unwrap_err();
        assert!(err.to_string().contains("不支持模型列表查询"));
    }

    #[test]
    fn query_balance_parses_subscription_and_usage() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"hard_limit_usd": 10.0, "total_used": 3.5, "total_available": 6.5}"#,
            r#"{"total_usage": 350}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let info = probe.query_balance(cfg, vec!["sk-test".to_string()], vec![]).unwrap();
        assert_eq!(info.total_limit, Some(10.0));
        assert_eq!(info.total_used, Some(3.5));
        assert_eq!(info.total_available, Some(6.5));
        assert_eq!(info.remaining_balance, Some(6.5));
    }

    #[test]
    fn query_balance_remaining_falls_back_to_limit_minus_used() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"total_granted": 20.0, "total_used": 5.0}"#,
            r#"{"total_usage": 500}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let info = probe.query_balance(cfg, vec!["sk-test".to_string()], vec![]).unwrap();
        assert_eq!(info.total_limit, Some(20.0));
        assert_eq!(info.total_used, Some(5.0));
        assert_eq!(info.total_available, None);
        assert_eq!(info.remaining_balance, Some(15.0));
    }

    #[test]
    fn test_openai_uses_temperature_zero_and_max_tokens_one() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "gpt-4o-mini".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let out = probe.test_openai(cfg, vec!["sk-test".to_string()], messages, vec![]).unwrap();
        assert_eq!(out, "ok");
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        let body: Value = serde_json::from_str(&reqs[0].2).unwrap();
        assert_eq!(body["temperature"], 0.0);
        assert_eq!(body["max_tokens"], 1);
        assert_eq!(body["stream"], false);
    }

    #[test]
    fn test_anthropic_builds_request_and_parses_text() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn"}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "ANTHROPIC".into(),
            base_url: "https://api.anthropic.com".into(),
            api_key: "sk-ant-test".into(),
            model: "claude-3-5-sonnet".into(),
            temperature: 0.7,
            max_tokens: Some(800),
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let out = probe
            .test_anthropic(cfg, vec!["sk-ant-test".to_string()], messages, "Reply with ok.".into(), vec![])
            .unwrap();
        assert_eq!(out, "ok");
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].0.ends_with("/messages"));
        assert!(reqs[0].1.iter().any(|(k, v)| k == "x-api-key" && v == "sk-ant-test"));
        assert!(reqs[0].1.iter().any(|(k, _v)| k == "anthropic-version"));
        let body: Value = serde_json::from_str(&reqs[0].2).unwrap();
        assert_eq!(body["max_tokens"], 5);
    }

    #[test]
    fn test_openai_falls_over_to_next_key() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"error":{"message":"rate limit"}}"#,
            r#"{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "gpt-4o-mini".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let out = probe
            .test_openai(
                cfg,
                vec!["sk-bad".to_string(), "sk-good".to_string()],
                messages,
                vec![],
            )
            .unwrap();
        assert_eq!(out, "ok");
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2);
    }

    // ── 认证头分支（PARTNER / XIAOMI / 普通 Bearer） ──

    #[test]
    fn auth_headers_partner_forwards_extra_headers() {
        let cfg = ApiProbeConfig {
            provider: "PARTNER".into(),
            base_url: "https://partner.example".into(),
            api_key: String::new(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let extra = vec![
            ("X-LianYu-Session".to_string(), "sess-1".to_string()),
            ("X-LianYu-Client-Id".to_string(), "cli-1".to_string()),
        ];
        let h = auth_headers(&cfg, "ignored", &extra);
        assert_eq!(h.len(), 2);
        assert!(h.iter().any(|(k, v)| k == "X-LianYu-Session" && v == "sess-1"));
        assert!(h.iter().any(|(k, v)| k == "X-LianYu-Client-Id" && v == "cli-1"));
        assert!(!h.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn auth_headers_xiaomi_uses_api_key() {
        let cfg = ApiProbeConfig {
            provider: "XIAOMI".into(),
            base_url: "https://api.xiaomi.com".into(),
            api_key: "mk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let h = auth_headers(&cfg, "mk-test", &[]);
        assert!(h.iter().any(|(k, v)| k == "api-key" && v == "mk-test"));
        assert!(!h.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn auth_headers_bearer_merges_extra_without_duplicate() {
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-real".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let extra = vec![
            ("Authorization".to_string(), "Bearer should-not-dup".to_string()),
            ("X-Custom".to_string(), "v".to_string()),
        ];
        let h = auth_headers(&cfg, "sk-real", &extra);
        assert_eq!(
            h.iter().filter(|(k, _)| k == "Authorization").count(),
            1,
            "extra 中的 Authorization 不应覆盖主 key"
        );
        assert!(h.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-real"));
        assert!(h.iter().any(|(k, v)| k == "X-Custom" && v == "v"));
    }

    #[test]
    fn auth_headers_non_partner_drops_session_headers() {
        // suflow/CUSTOM（OpenAI 规范接口）即使误传 session 头也必须丢弃：
        // 服务端 authenticate() 检测到 X-LianYu-Session 会走 session 路径并强制
        // 设备签名验证（verify_device_signature），ureq 无签名 → 401。
        let cfg = ApiProbeConfig {
            provider: "CUSTOM".into(),
            base_url: "https://suflow.cloud/v1".into(),
            api_key: "sk-suflow".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "openai".into(),
        };
        let extra = vec![
            ("X-LianYu-Session".to_string(), "sess-1".to_string()),
            ("X-LianYu-Client-Id".to_string(), "cli-1".to_string()),
            ("X-Custom".to_string(), "keep-me".to_string()),
        ];
        let h = auth_headers(&cfg, "sk-suflow", &extra);
        assert!(h.iter().any(|(k, v)| k == "Authorization" && v == "Bearer sk-suflow"));
        assert!(!h.iter().any(|(k, _)| k.eq_ignore_ascii_case("X-LianYu-Session")));
        assert!(!h.iter().any(|(k, _)| k.eq_ignore_ascii_case("X-LianYu-Client-Id")));
        assert!(h.iter().any(|(k, v)| k == "X-Custom" && v == "keep-me"));
    }

    #[test]
    fn auth_headers_xiaomi_drops_session_headers() {
        let cfg = ApiProbeConfig {
            provider: "XIAOMI".into(),
            base_url: "https://api.xiaomi.com".into(),
            api_key: "mk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let extra = vec![("X-LianYu-Session".to_string(), "sess-1".to_string())];
        let h = auth_headers(&cfg, "mk-test", &extra);
        assert!(h.iter().any(|(k, v)| k == "api-key" && v == "mk-test"));
        assert!(!h.iter().any(|(k, _)| k.eq_ignore_ascii_case("X-LianYu-Session")));
    }

    // ── 工具函数边界 ──

    #[test]
    fn normalize_base_url_defaults_when_empty() {
        assert_eq!(normalize_base_url(""), "https://api.openai.com/v1");
        assert_eq!(normalize_base_url("   "), "https://api.openai.com/v1");
        assert_eq!(normalize_base_url("https://x.com/"), "https://x.com");
    }

    #[test]
    fn looks_like_html_heuristics() {
        assert!(looks_like_html("<!DOCTYPE html>"));
        assert!(looks_like_html("<html><body>unauthorized</body></html>"));
        assert!(!looks_like_html("{\"ok\":true}"));
        assert!(!looks_like_html("plain text"));
    }

    // ── fetch_models / query_balance / test_* 网络路径 ──

    #[test]
    fn fetch_models_partner_sends_session_header() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"data":[{"id":"partner-model"}]}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "PARTNER".into(),
            base_url: "https://partner.example".into(),
            api_key: String::new(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let extra = vec![HttpHeader {
            name: "X-LianYu-Session".into(),
            value: "sess-1".into(),
        }];
        let models = probe.fetch_models(cfg, extra).unwrap();
        assert_eq!(models, vec!["partner-model"]);
        let reqs = transport.requests.lock().unwrap();
        assert!(reqs[0].1.iter().any(|(k, v)| k == "X-LianYu-Session" && v == "sess-1"));
        assert!(!reqs[0].1.iter().any(|(k, _)| k == "Authorization"));
    }

    #[test]
    fn query_balance_stops_after_first_successful_key() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"hard_limit_usd": 5.0}"#,
            r#"{"total_usage": 100}"#,
            // 第二个 key 不应被请求（第一个 key 已命中余额）
            r#"{"hard_limit_usd": 99.0}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key: "sk-test".into(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let info = probe
            .query_balance(cfg, vec!["key1".to_string(), "key2".to_string()], vec![])
            .unwrap();
        assert_eq!(info.total_limit, Some(5.0));
        assert_eq!(info.total_used, Some(1.0));
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2, "命中余额后不应再请求第二个 key");
        assert!(reqs[0].1.iter().any(|(_, v)| v == "Bearer key1"));
    }

    #[test]
    fn query_balance_without_keys_returns_error() {
        let probe = ApiProbe {
            transport: Arc::new(MockTransport::new(vec![])),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://x".into(),
            api_key: String::new(),
            model: "".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let err = probe.query_balance(cfg, vec![], vec![]).unwrap_err();
        assert!(err.to_string().contains("没有可用的 API Key"));
    }

    #[test]
    fn test_openai_xiaomi_uses_max_completion_tokens_and_api_key() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "XIAOMI".into(),
            base_url: "https://api.xiaomi.com".into(),
            api_key: "mk-test".into(),
            // k2.6 固定温度模型：不带 temperature
            model: "mi-k2.6-turbo".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let out = probe.test_openai(cfg, vec![], messages, vec![]).unwrap();
        assert_eq!(out, "ok");
        let reqs = transport.requests.lock().unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].1.iter().any(|(k, v)| k == "api-key" && v == "mk-test"));
        let body: Value = serde_json::from_str(&reqs[0].2).unwrap();
        assert_eq!(body["max_completion_tokens"], 1);
        assert!(body.get("max_tokens").is_none());
        assert!(body.get("temperature").is_none(), "XIAOMI 固定温度模型不应带 temperature");
    }

    #[test]
    fn test_anthropic_empty_content_returns_error() {
        let transport = Arc::new(MockTransport::new(vec![
            r#"{"content":[{"type":"text","text":""}],"stop_reason":"end_turn"}"#,
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "ANTHROPIC".into(),
            base_url: "https://api.anthropic.com".into(),
            api_key: "sk-ant-test".into(),
            model: "claude-3-5-sonnet".into(),
            temperature: 0.7,
            max_tokens: Some(800),
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let err = probe
            .test_anthropic(cfg, vec![], messages, "sys".into(), vec![])
            .unwrap_err();
        assert!(err.to_string().contains("空内容"));
    }

    #[test]
    fn test_openai_html_response_returns_friendly_error() {
        let transport = Arc::new(MockTransport::new(vec![
            "<html><body>Cloudflare</body></html>",
        ]));
        let probe = ApiProbe {
            transport: transport.clone(),
        };
        let cfg = ApiProbeConfig {
            provider: "OPENAI".into(),
            base_url: "https://blocked.example".into(),
            api_key: "sk-test".into(),
            model: "gpt-4o-mini".into(),
            temperature: 0.7,
            max_tokens: None,
            format_hint: "".into(),
        };
        let messages = vec![ProbeMessage {
            role: "user".into(),
            content: "ping".into(),
        }];
        let err = probe.test_openai(cfg, vec![], messages, vec![]).unwrap_err();
        assert!(err.to_string().contains("网页而非API响应"));
    }
}
