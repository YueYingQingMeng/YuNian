package com.yunian.ai.wechat.map

object WeChatContentCleaner {

    private val STICKER_PATTERN = Regex("^\\[([^\\[\\]]+)\\]$")
    private val ROLE_PREFIX_REGEX = Regex("(?m)^\\s*\\[(?:角色\\d+|[^\\[\\]]+?)\\]\\s*")
    private val ENC_LEAK_REGEX = Regex("(?m)^enc:\\S+$")

    private val THINK_CLOSED_REGEX = Regex("(?is)<(?:think|thinking|thought|reflection|reasoning|LM_THINK|redacted_reasoning)[^>]*>[\\s\\S]*?</(?:think|thinking|thought|reflection|reasoning|LM_THINK|redacted_reasoning)\\s*>")
    private val THINK_UNCLOSED_REGEX = Regex("(?is)<(?:think|thinking|thought|reflection|reasoning|LM_THINK|redacted_reasoning)[^>]*>[\\s\\S]*$")

    fun isStickerContent(content: String): Boolean {
        return STICKER_PATTERN.matches(content.trim())
    }

    fun extractStickerName(content: String): String? {
        return STICKER_PATTERN.find(content.trim())?.groupValues?.get(1)
    }

    /**
     * 只剥思考块，**不动其他任何清洗**（换行/标点/去重都不碰）。
     *
     * 为什么要单独开一个窄接口：`clean()` 是完整的出站清洗管线（角色前缀、表情标签、换行归一、
     * 句级去重…），而微信的 AI 自动回复需要**先按换行拆气泡**——跑 `clean()` 会把换行折成
     * 「，」，气泡结构就没了。而思考块是必须挡住的（Issue #2 症状 1）：AI 回复的事件文本一旦
     * 带 `<thinking>…` 就会被当正文发给用户，且这条路径此前不经过任何清洗。
     *
     * 与 Rust 出口（`agent-native/src/thinking.rs`）覆盖同一类标签形态，属第二道防线。
     */
    fun stripThinking(raw: String): String {
        if (raw.isEmpty()) return raw
        var text = THINK_CLOSED_REGEX.replace(raw, "")
        text = THINK_UNCLOSED_REGEX.replace(text, "")
        return text.trim()
    }

    fun clean(raw: String): String {
        var text = raw
        text = ROLE_PREFIX_REGEX.replace(text, "")
        text = THINK_CLOSED_REGEX.replace(text, "")
        text = THINK_UNCLOSED_REGEX.replace(text, "")
        text = ENC_LEAK_REGEX.replace(text, "")
        val bracketRegex = Regex("\\[([^\\[\\]]+?)\\]")
        val systemTags = setOf("语音", "图片", "视频", "文件", "位置", "红包", "转账")
        val cleanedStickerDescs = mutableSetOf<String>()
        text = bracketRegex.replace(text) { match: MatchResult ->
            val inner = match.groupValues[1].trim()
            if (inner in systemTags) {
                match.value
            } else {
                cleanedStickerDescs.add(inner)
                ""
            }
        }
        text = text.replace("]", "").replace("[", "")
        text = Regex("\\bsticker_\\w+\\.png\\b", RegexOption.IGNORE_CASE).replace(text, "")
        for (desc in cleanedStickerDescs) {
            if (desc.length >= 2 && text.contains(desc)) {
                text = text.replace(desc, "")
            }
        }
        text = text.replace(Regex("(?<=[。！？])\\s*\\n+\\s*"), "")
        text = text.replace(Regex("(?<![。！？])\\s*\\n+\\s*"), "，")
        text = text.replace(Regex("，{2,}"), "，")
            .trim()
            .trimStart('，', ',', '.', '。', ' ')

        val sentences = text.split(Regex("(?<=[。！？])"))
        val deduped = mutableListOf<String>()
        for (sentence in sentences) {
            val trimmed = sentence.trim()
            if (trimmed.isEmpty()) continue
            val currentClean = trimmed.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
            var isDuplicate = false
            for (prev in deduped) {
                val prevClean = prev.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
                if (currentClean == prevClean ||
                    (currentClean.length >= 4 && prevClean.endsWith(currentClean)) ||
                    (prevClean.length >= 4 && currentClean.endsWith(prevClean))
                ) {
                    isDuplicate = true
                    break
                }
            }
            if (!isDuplicate) {
                deduped.add(sentence)
            }
        }
        var result = deduped.joinToString("").trim()

        for (len in result.length / 2 downTo 4) {
            val suffix = result.takeLast(len)
            val beforeSuffix = result.dropLast(len)
            if (beforeSuffix.contains(suffix)) {
                result = beforeSuffix
                break
            }
            val suffixCleaned = suffix.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
            val beforeSuffixCleaned = beforeSuffix.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
            if (suffixCleaned.length >= 4 && beforeSuffixCleaned.endsWith(suffixCleaned)) {
                result = beforeSuffix
                break
            }
        }

        return result
    }
}
