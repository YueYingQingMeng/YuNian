// api_penalty.rs — 复读惩罚（presence/frequency penalty）provider 能力门控
//
// 背景（与链 1 同一病根：生成链路下沉 Rust，但 Kotlin 侧的修复没跟着下沉）：
// `core/network/ApiPenalty.kt` 是历史上为「AI 复述之前说过的内容」做的修复——
// 在请求体注入 `presence_penalty` / `frequency_penalty`。但聊天生成早已整体下沉
// 到本 Rust 网关，而该助手只作用于**已废弃的 Kotlin 请求路径** →
// 线上路径的复读惩罚恒为 0，修复等于失效。本模块把同一份门控表逐条移植过来。
//
// === provider 能力门控（关键安全约束，与 ApiPenalty.kt 逐条同表）===
// 不同端点的参数支持度不同，非标准 OpenAI 语义的路径可能因**未知字段直接 400**，
// 从而把一个原本可用的 provider 弄挂。因此只对「明确为标准 OpenAI 语义且官方文档
// 确认接受这两个字段」的 provider 注入；无法确认 / 明确会报错的一律**不注入**。
//
// === 字段级注入原则（与 Kotlin 同源）===
// 若某 provider 该字段的**服务端默认值 ≥ 我们要注入的值**，显式注入等于把这个轴的
// 惩罚**反向调低**，比不注入更利于复述（典型：IFLYTEK presence 默认 1.2，注入 0.4
// 即从 1.2 降到 0.4）。因此**逐字段**判定：注入值 ≥ 默认值才注入，否则不注入该字段。
//
// —— 白名单：官方文档确认可安全注入（至少一个轴），且该轴注入值 ≥ 服务端默认值 ——
//  - OPENAI / DEEPSEEK / ZHIPU / SILICONFLOW / OPENROUTER / GROQ / XIAOMI：
//    两轴默认值均为 0 → 两轴都注入。
//  - IFLYTEK：presence 默认 1.2 > 0.4 → 该轴不注入；frequency 默认 0.02 < 0.3 → 注入。
//  - DASHSCOPE：presence 默认随模型为 1.5 / 0.5 / 0.0（存在 > 0.4）→ 该轴不注入；
//    frequency 默认 0 → 注入。
//
// —— 黑名单：不支持 / 明确报错 / 无法确认，绝不注入 ——
//  - KIMI：kimi-k2.6 两字段固定 0，且注明「传入其他值会报错」→ 排除。
//  - GEMINI：Google 官方 OpenAI 兼容层对这两个字段会抛 API 错误 → 排除。
//  - PARTNER：Clove 私有网关，无公开文档且历史上对字段极严（曾因温度精度 400）→ 排除。
//  - CUSTOM：用户自填任意 relay，是否接受未知字段不可知 → 排除（宁可不发也不 400）。
//  - ANTHROPIC：Messages API 无此参数（走独立请求体）→ 排除。
//
// 单一门控原则：**所有**把惩罚写进请求体的点都必须经由 [apply_repetition_penalty]。

use serde_json::{json, Value};

/// 聊天生成路径的 `presence_penalty`（用于「该字段默认值 ≤ 此值」的 provider）。
const CHAT_PRESENCE_PENALTY: f64 = 0.4;

/// 聊天生成路径的 `frequency_penalty`。
const CHAT_FREQUENCY_PENALTY: f64 = 0.3;

/// 解析该 provider 应注入的惩罚参数（逐字段：`None` = 不注入该字段）；
/// 完全不可安全注入时返回 `None`。
///
/// 依据见本文件顶部取证记录。provider 名称大小写不敏感。
pub(crate) fn resolve_penalty_params(provider: &str) -> Option<(Option<f64>, Option<f64>)> {
    match provider.trim().to_ascii_uppercase().as_str() {
        // 两轴默认值均为 0 → 两轴都注入。
        "OPENAI" | "DEEPSEEK" | "ZHIPU" | "SILICONFLOW" | "OPENROUTER" | "GROQ" | "XIAOMI" => {
            Some((Some(CHAT_PRESENCE_PENALTY), Some(CHAT_FREQUENCY_PENALTY)))
        }
        // IFLYTEK：presence 默认 1.2 > 0.4 → 不注入该轴；frequency 默认 0.02 < 0.3 → 注入。
        "IFLYTEK" => Some((None, Some(CHAT_FREQUENCY_PENALTY))),
        // DASHSCOPE：presence 默认随模型为 1.5 / 0.5 / 0.0 → 不注入该轴；frequency 默认 0 → 注入。
        "DASHSCOPE" => Some((None, Some(CHAT_FREQUENCY_PENALTY))),
        // 黑名单：完全不注入（不支持 / 明确报错 / 无法确认）。
        _ => None,
    }
}

/// 按 provider 门控把复读惩罚**逐字段**写入请求体；某字段策略为 `None` 或不支持的
/// provider 则不写该字段。
///
/// 这是 Rust 侧**唯一**的惩罚字段写入点（单一门控原则）：禁止在请求构造点散落判断。
///
/// @return `true` 表示至少注入了 `presence_penalty` 或 `frequency_penalty` 之一。
pub(crate) fn apply_repetition_penalty(provider: &str, body: &mut Value) -> bool {
    let Some((presence, frequency)) = resolve_penalty_params(provider) else {
        return false;
    };
    let mut injected = false;
    if let Some(v) = presence {
        body["presence_penalty"] = json!(v);
        injected = true;
    }
    if let Some(v) = frequency {
        body["frequency_penalty"] = json!(v);
        injected = true;
    }
    injected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitelist_injects_both_axes() {
        for provider in [
            "OPENAI",
            "DEEPSEEK",
            "ZHIPU",
            "SILICONFLOW",
            "OPENROUTER",
            "GROQ",
            "XIAOMI",
            "openai",
            " deepseek ",
        ] {
            let mut body = json!({"model": "m"});
            assert!(apply_repetition_penalty(provider, &mut body), "{provider}");
            assert_eq!(body["presence_penalty"], 0.4, "{provider}");
            assert_eq!(body["frequency_penalty"], 0.3, "{provider}");
        }
    }

    #[test]
    fn per_field_gating_for_default_value_conflicts() {
        for provider in ["IFLYTEK", "DASHSCOPE"] {
            let mut body = json!({"model": "m"});
            assert!(apply_repetition_penalty(provider, &mut body), "{provider}");
            assert!(
                body.get("presence_penalty").is_none(),
                "{provider} presence 默认值高于注入值，不得注入: {body}"
            );
            assert_eq!(body["frequency_penalty"], 0.3, "{provider}");
        }
    }

    #[test]
    fn blacklist_never_injects() {
        for provider in [
            "KIMI",
            "GEMINI",
            "PARTNER",
            "CUSTOM",
            "ANTHROPIC",
            "",
            "UNKNOWN",
        ] {
            let mut body = json!({"model": "m"});
            assert!(!apply_repetition_penalty(provider, &mut body), "{provider}");
            assert_eq!(body.as_object().unwrap().len(), 1, "{provider}: {body}");
        }
    }
}
