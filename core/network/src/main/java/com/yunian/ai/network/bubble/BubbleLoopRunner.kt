package com.yunian.ai.network.bubble

import com.yunian.ai.common.SecureLog

class BubbleLoopRunner(

    private val maxBubbles: Int = MAX_BUBBLES,

    private val maxRetries: Int = MAX_RETRIES,
) {

    suspend fun runFollowingBubbles(
        generateOnce: suspend (alreadyGenerated: List<String>) -> String,
    ): List<String> {
        val bubbles = mutableListOf<String>()
        var continueChat = true

        val remainingSlots = (maxBubbles - 1).coerceAtLeast(0)
        var attemptCount = 0
        while (continueChat && bubbles.size < remainingSlots) {
            var reply: BubbleReply? = null
            for (attempt in 0 until maxRetries) {
                attemptCount++
                val raw = try {
                    generateOnce(bubbles)
                } catch (e: kotlinx.coroutines.CancellationException) {
                    // 取消不是生成失败：必须向上传播（用户打断/新消息到达），绝不能吞进重试循环。
                    throw e
                } catch (e: Exception) {
                    SecureLog.w("BubbleLoopRunner", "generateOnce failed (attempt ${attempt + 1}): ${e.message}")
                    ""
                }

                reply = BubbleJsonProtocol.parseStrict(raw)
                if (reply != null && reply.text.isNotBlank()) break
            }
            if (reply == null || reply.text.isBlank()) {

                SecureLog.w("BubbleLoopRunner", "Bubble generation exhausted after $attemptCount attempts; stop chaining")
                break
            }
            bubbles.add(reply.text)
            continueChat = reply.continueChat
        }
        if (bubbles.size == remainingSlots && remainingSlots > 0) {
            SecureLog.d("BubbleLoopRunner", "Reached hard cap of $maxBubbles bubbles per turn")
        }
        return bubbles
    }

    companion object {

        /**
         * 单轮连发气泡的**兜底安全上限**（非产品限流）：条数本身由模型 `continue` 自决，
         * 该值只在模型失控连发时兜底——注意链式连发的每一条都是一次独立的模型调用，
         * 因此保留一个高水位护栏（用户要求「条数不限」，常规聊天远达不到此值）。
         *
         * ⚠️ 这是**气泡条数**护栏，**不是** Agent 回合的 API 调用轮数预算。
         * 历史上单聊把它直接当 `maxRounds` 传给 Rust 循环（Issue #2 症状 2：连发失控、
         * 内容逐轮漂移）。轮数预算请用
         * [com.yunian.ai.common.ChatConstants.AGENT_MAX_TOOL_ROUNDS]。
         */
        const val MAX_BUBBLES = 64

        const val MAX_RETRIES = 3
    }
}
