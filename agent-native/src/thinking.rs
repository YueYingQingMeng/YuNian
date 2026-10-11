// thinking.rs — 思考块剥离（防「思考过程泄漏进回复气泡」）
//
// === 背景（Issue #2 症状 1）===
// 生成链路整体下沉 Rust 后，Kotlin 侧历史上的 `ResponsePostProcessor.stripThinkingContent()`
// **没有跟着下沉**：
//   · Rust 只处理结构化的 `reasoning_content` 字段，从不清理 `content` 正文里的思考块
//     （全仓此前无任何 `<think` 处理）；
//   · 气泡落到 UI 前只过 `AgentReplyText.forDisplay`，它仅清洗生图标记。
// 结果：无独立 reasoning 通道的模型/服务商把思考写进正文时，思考被 `emit_bubble`
// 直接当气泡发给用户（典型：「嗯，等待用户回答」这类元叙述）。
//
// === 修法 ===
// 在**气泡事件的唯一出口**（agent.rs `push_bubble`）统一剥离，覆盖四条出口：
// `emit_bubble` / `emit_segmented` / 正文补发 / 最终正文。剥离后余文为空则不产出气泡
// ——**不伪造内容、不强行发送**（与 WeChatOutboundText 的既有红线一致）。
//
// === 覆盖形态（与 Kotlin ResponsePostProcessor 同源的常见泄漏形态）===
//   1) 闭合标签：`<thinking>…</thinking>`、`<think …>…</think>` 及常见别名
//      （thought / reflection / reasoning / LM_THINK / redacted_reasoning）；
//   2) 未闭合标签：`<thinking>…` 直到结尾（模型被截断时最常见的泄漏形态）；
//   3) 围栏代码块：```thinking … ``` / ```reasoning … ```（含未闭合）。
//
// 单一门控原则：**所有**产出气泡文本的点都必须经由 [strip_thinking_blocks]。

use regex::Regex;
use std::sync::OnceLock;

/// 思考块标签别名（与 Kotlin `ResponsePostProcessor` 的分类正则同源）。
const THINK_TAGS: &str =
    r"(?:think|thinking|thought|reflection|reasoning|lm_think|redacted_reasoning)";

/// 围栏代码块语言别名（```thinking 等）。
const THINK_FENCES: &str = r"(?:thinking|thought|reasoning|analysis|think)";

fn closed_tag_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&format!(
            r"(?is)<{THINK_TAGS}\b[^>]*>.*?</{THINK_TAGS}\s*>"
        ))
        .expect("closed tag regex")
    })
}

fn unclosed_tag_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&format!(r"(?is)<{THINK_TAGS}\b[^>]*>.*$")).expect("unclosed tag regex")
    })
}

fn closed_fence_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&format!(r"(?is)```{THINK_FENCES}\s*.*?```")).expect("closed fence regex")
    })
}

fn unclosed_fence_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(&format!(r"(?is)```{THINK_FENCES}\s*.*$")).expect("unclosed fence regex")
    })
}

/// 剥离文本中的思考块，返回可展示正文（首尾空白已裁剪）。
///
/// 先剥闭合形态，再剥未闭合形态（未闭合的会吃掉至结尾，因此必须放在最后）。
pub(crate) fn strip_thinking_blocks(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let mut out = closed_tag_re().replace_all(text, "").into_owned();
    out = closed_fence_re().replace_all(&out, "").into_owned();
    out = unclosed_tag_re().replace_all(&out, "").into_owned();
    out = unclosed_fence_re().replace_all(&out, "").into_owned();
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_closed_think_tag_and_keeps_reply() {
        let raw = "<thinking>先分析用户意图</thinking>\n今晚吃火锅吗？";
        assert_eq!(strip_thinking_blocks(raw), "今晚吃火锅吗？");
    }

    #[test]
    fn strips_closed_tag_with_attributes_and_aliases() {
        for raw in [
            "<think reason=\"x\">内心独白</think>好呀",
            "<THINKING>内心独白</THINKING>好呀",
            "<reasoning>内心独白</reasoning>好呀",
            "<redacted_reasoning>内心独白</redacted_reasoning>好呀",
            "<LM_THINK>内心独白</LM_THINK>好呀",
        ] {
            assert_eq!(strip_thinking_blocks(raw), "好呀", "raw={raw}");
        }
    }

    #[test]
    fn strips_unclosed_tag_to_end() {
        // 最常见泄漏形态：被截断的未闭合思考块
        let raw = "在的呀～\n<thinking>嗯，等待用户回答";
        assert_eq!(strip_thinking_blocks(raw), "在的呀～");
    }

    #[test]
    fn strips_fenced_thinking_block() {
        let raw = "```thinking\n先想想怎么回\n```\n好呀，我在";
        assert_eq!(strip_thinking_blocks(raw), "好呀，我在");
    }

    #[test]
    fn pure_thinking_yields_empty() {
        // 整条回复只有思考 → 必须为空（调用方据此不产出气泡，不伪造内容）
        assert_eq!(
            strip_thinking_blocks("<thinking>嗯，等待用户回答</thinking>"),
            ""
        );
        assert_eq!(strip_thinking_blocks("<thinking>嗯，等待用户回答"), "");
    }

    #[test]
    fn plain_reply_untouched() {
        for raw in ["今晚想吃什么呀～", "嗯……我在呢", "好呀！那我给你讲个故事"] {
            assert_eq!(strip_thinking_blocks(raw), raw, "raw={raw}");
        }
    }

    #[test]
    fn multibyte_not_broken() {
        let raw = "<thinking>他在问天气</thinking>今天多云转晴，出门带伞哦";
        assert_eq!(strip_thinking_blocks(raw), "今天多云转晴，出门带伞哦");
    }
}
