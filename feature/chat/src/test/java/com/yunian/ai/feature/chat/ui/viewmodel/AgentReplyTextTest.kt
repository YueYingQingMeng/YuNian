package com.yunian.ai.feature.chat.ui.viewmodel

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class AgentReplyTextTest {
    @Test
    fun `image prompt only native bubble remains visible`() {
        val text = AgentReplyText.forDisplay("[[生图: 雨中的白猫]]", imageGenEnabled = true)

        assertEquals("（正在为你配图…）", text)
        assertFalse(text.contains("白猫"))
    }

    @Test
    fun `native bubble keeps dialogue and strips image description`() {
        val text = AgentReplyText.forDisplay("好呀，马上画给你\n[[生图: 雨中的白猫]]", true)

        assertEquals("好呀，马上画给你", text.trim())
        assertFalse(text.contains("白猫"))
    }

    @Test
    fun `empty reply is not converted into a successful image reply`() {
        assertTrue(AgentReplyText.forDisplay("  ", true).isBlank())
    }

    @Test
    fun `disabled image generation and ordinary text retain original behavior`() {
        assertEquals("[[生图: 白猫]]", AgentReplyText.forDisplay("[[生图: 白猫]]", false))
        assertEquals("我在呢", AgentReplyText.forDisplay("我在呢", true))
    }

    // ── Issue #2 症状 1 第二道防线：所有气泡必经此处，思考块必须被剥离 ──

    @Test
    fun `thinking block is stripped from bubble before display`() {
        val text = AgentReplyText.forDisplay(
            "<thinking>先分析用户意图</thinking>\n今晚吃火锅吗？",
            imageGenEnabled = false,
        )
        assertEquals("今晚吃火锅吗？", text)
    }

    @Test
    fun `unclosed thinking block (truncated model output) never reaches the user`() {
        val text = AgentReplyText.forDisplay("在的呀\n<thinking>嗯，等待用户回答", imageGenEnabled = true)
        assertEquals("在的呀", text)
    }

    @Test
    fun `pure thinking reply becomes blank instead of leaking meta narration`() {
        val text = AgentReplyText.forDisplay("<thinking>嗯，等待用户回答</thinking>", imageGenEnabled = false)
        assertTrue("纯思考必须被清空: $text", text.isBlank())
    }

    @Test
    fun `thinking stripped before image-gen sanitize so thinking-only image prompt is not honored`() {
        // 思考里提到生图不算用户可见的生图指令：剥离后为空 → 不得转成「正在为你配图…」
        val text = AgentReplyText.forDisplay(
            "<thinking>[[生图: 雨中的白猫]]</thinking>",
            imageGenEnabled = true,
        )
        assertTrue("思考内的生图指令不得被当成有效回复: $text", text.isBlank())
    }
}
