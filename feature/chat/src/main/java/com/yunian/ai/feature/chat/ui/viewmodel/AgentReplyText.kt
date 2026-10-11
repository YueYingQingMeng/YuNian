package com.yunian.ai.feature.chat.ui.viewmodel

import com.yunian.ai.domain.imagegen.ImageGenProtocol
import com.yunian.ai.network.ResponsePostProcessor

/**
 * Native 气泡与视觉回复共用的展示清洗。
 *
 * 两道清洗，顺序固定：
 * 1. **思考块剥离**（Issue #2 症状 1 的第二道防线）：Rust 气泡出口已统一剥离
 *    （`agent-native/src/thinking.rs`），这里再兜一层——所有气泡与视觉回复都必经本函数，
 *    即使将来新增了绕过 Rust 出口的投放路径，用户也不会看到「嗯，等待用户回答」这类
 *    元叙述。复用仓库既有的 `ResponsePostProcessor.stripThinkingContent`，不另造正则。
 * 2. **生图指令清洗**：避免仅含生图指令的有效回复被丢弃（既有行为，保持逐字不变）。
 *
 * 注意：思考剥离在生图清洗**之前**——否则「`<thinking>…画一张猫</thinking>`」会把
 * 思考里的生图指令当成有效生图请求处理。
 */
internal object AgentReplyText {
    private const val IMAGE_GEN_ONLY_REPLY_TEXT = "（正在为你配图…）"

    fun forDisplay(raw: String, imageGenEnabled: Boolean): String {
        val withoutThinking = ResponsePostProcessor.stripThinkingContent(raw)
        if (!imageGenEnabled) return withoutThinking
        val stripped = ImageGenProtocol.sanitizeForDisplay(withoutThinking)
        return when {
            stripped.isNotBlank() -> stripped
            ImageGenProtocol.isPromptOnly(withoutThinking) -> IMAGE_GEN_ONLY_REPLY_TEXT
            else -> withoutThinking
        }
    }
}
