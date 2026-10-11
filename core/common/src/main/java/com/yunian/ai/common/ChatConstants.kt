package com.yunian.ai.common

object ChatConstants {

    const val CHAT_PAGE_SIZE = 80

    const val CHAT_LOAD_MORE_SIZE = 50

    const val MESSAGE_QUEUE_CAPACITY = 100

    const val MESSAGE_BATCH_WINDOW_MS = 2500L

    const val MESSAGE_BATCH_POLL_INTERVAL_MS = 100L

    const val MESSAGE_BATCH_MAX_SIZE = 10

    const val MESSAGE_BATCH_SPLIT_DELAY_MS = 300L

    const val MAX_AUTO_RESTARTS = 5

    const val CHAT_STICKER_CANDIDATES = 10

    const val CHAT_STICKER_NAME_MAX_LENGTH = 20

    const val COMPANION_PERSONALITY_MAX_LENGTH = 300

    const val COMPANION_SPEAKING_STYLE_MAX_LENGTH = 100

    const val COMPANION_BACKSTORY_MAX_LENGTH = 200

    const val SHORT_HISTORY_LIMIT = 10

    /**
     * AI 上下文单次拉取的消息条数上限。
     * 400 条配合滚动摘要（RollingSummaryManager）保证水位线之外的历史
     * 仍能被增量合并进持久化摘要，而不是被一次性丢弃。
     */
    const val MAX_AI_CONTEXT_FETCH = 400

    /**
     * 滚动摘要增量合并触发阈值：pending（未进摘要、也装不进上下文的）消息
     * 达到条数或估算 token 数任一阈值时，fire-and-forget 触发一次增量合并。
     */
    const val ROLLING_MERGE_MIN_MESSAGES = 24

    const val ROLLING_MERGE_MIN_TOKENS = 2500

    /**
     * 单次增量合并的上限：超过则先按段摘要再合并，避免单次 LLM 输入过长。
     */
    const val SEGMENT_MAX_MESSAGES = 48

    const val SEGMENT_MAX_TOKENS = 6000

    /**
     * 滚动摘要注入上下文的 token 预算上限。
     */
    const val SUMMARY_MAX_TOKENS = 1500

    /**
     * 陈旧防护水位线间隔（消息条数）：当前上下文里最旧一条消息的 id 超过
     * 「已覆盖水位线 + ROLLING_STALE_GAP」时，视为会话已重置（历史被清空重建），
     * 旧滚动摘要作废，回退到首启路径。
     */
    const val ROLLING_STALE_GAP = 600

    const val MAX_UI_MESSAGES = 200

    const val CONTEXT_CACHE_SIZE = 3

    const val MEMORY_CONTEXT_MAX_CHARS = 500

    const val USER_MESSAGE_PROMPT_MAX_CHARS = 2000

    const val GROUP_CHAT_AUTO_ROUNDS = 2

    /**
     * 单轮对话内 AI 自主工具循环的最大轮次。
     * 「上网找技能」这类任务需要 先用 web_fetch 查 GitHub API → 读 raw SKILL.md → skill_install，
     * 光试探就会消耗多轮；轮次太小会让模型来不及调用 skill_install 就被兜底文案截断。
     * 6 轮足以覆盖「搜索 → 确认 → 安装 → 收尾」的标准路径。
     */
    const val CHAT_TOOL_LOOP_MAX_ROUNDS = 6

    /**
     * Cordis Agent（Rust）单回合的 **API 调用轮数**上限。
     *
     * 为什么必须与「气泡数」分开（Issue #2 症状 2 的根因）：
     * 此前单聊把 `BubbleLoopRunner.MAX_BUBBLES`（本意＝连发**气泡条数**护栏，64）
     * 直接当作 `maxRounds` 传进 Rust 循环，于是「气泡数预算」被解释成「可以调 64 次 API」；
     * 而 Rust 循环唯一的停止条件是模型自己收手 → 一轮回复能拆出十几条气泡、内容逐轮漂移。
     *
     * 取值依据：正常陪伴对话 1~3 轮即可收尾；带工具（记忆/技能/通道发送）的回合
     * 6~8 轮足够；10 轮留出余量，同时远小于旧值 64，避免「为凑数继续输出」。
     * 气泡**条数**仍不受此值限制（每条气泡由 `emit_bubble`/`emit_segmented` 产出，
     * 其护栏仍是 [com.yunian.ai.network.bubble.BubbleLoopRunner.MAX_BUBBLES]）。
     */
    const val AGENT_MAX_TOOL_ROUNDS = 10

    const val GROUP_CHAT_BUBBLE_GAP_MS = 500L

    const val GROUP_CHAT_CONTEXT_WINDOW = 20

    const val GROUP_CHAT_MENTION_JUDGE_THRESHOLD = 0.8f

    const val GROUP_CHAT_MENTION_JUDGE_ENABLED = true

    const val GROUP_CHAT_MESSAGE_LIMIT = 50

    const val GROUP_CHAT_MENTION_CONTEXT_MAX_MESSAGES = 12

    const val GROUP_CHAT_MENTION_SUMMARY_MAX_LENGTH = 120

    const val GROUP_CHAT_HISTORY_LOOKBACK = 10

    const val GROUP_CHAT_REPLY_HISTORY_LOOKBACK = 8

    const val GROUP_CHAT_KEYWORD_HISTORY_LOOKBACK = 6

    const val GROUP_CHAT_MENTION_SNAPSHOT_LOOKBACK = 6

    const val GROUP_CHAT_SENTENCE_PREVIEW_MAX_LENGTH = 14

    const val GROUP_CHAT_DUPLICATE_HISTORY_LOOKBACK = 3

    const val GROUP_CHAT_DUPLICATE_PREVIEW_MAX_LENGTH = 20

    const val GROUP_CHAT_PERSONALITY_PREVIEW_MAX_LENGTH = 50

    const val LOG_MESSAGE_PREVIEW_MAX_LENGTH = 30

    const val LOG_LONG_MESSAGE_PREVIEW_MAX_LENGTH = 50

    const val LOG_ERROR_PREVIEW_MAX_LENGTH = 80

    const val SAFETY_ERROR_PREVIEW_MAX_LENGTH = 50

    const val AI_REPLY_BROADCAST_DELAY_MS = 100L

    const val MULTI_REPLY_BASE_DELAY_MS = 800L

    const val MULTI_REPLY_RANDOM_DELAY_MS = 1200L

    const val FOLLOW_UP_BASE_DELAY_MS = 2000L

    const val FOLLOW_UP_RANDOM_DELAY_MS = 3000L

    /**
     * 自动追问抑制阈值：主回复长度达到该值时视为「长叙述」，不再自动追问，
     * 避免 AI 在自己讲完一大段后还自问自答。
     */
    const val FOLLOW_UP_SUPPRESS_MIN_CHARS = 80

    const val LOAD_MORE_HISTORY_DELAY_MS = 200L

    const val OBSERVE_MESSAGES_RESTART_DELAY_MS = 500L

    const val SETTINGS_RESTORE_DELAY_MS = 500L

    const val REPETITION_MIN_TEXT_LENGTH = 4

    const val REPETITION_MIN_MATCH_LENGTH = 2

    const val REPETITION_MIN_SUB_MATCH_LENGTH = 4

    const val REPETITION_MIN_SENTENCES = 2

    const val REPETITION_MIN_COMPARE_LENGTH = 4

    const val CLEANED_MIN_LENGTH = 2

    const val CLEANED_FALLBACK_ORIGINAL_LENGTH = 5

    const val BRACKET_CLEAN_MAX_LENGTH = 50

    const val STICKER_RANDOM_MAX = 100

    const val STICKER_RANDOM_MIN = 1

    const val STICKER_DESC_MIN_LENGTH = 2

    const val STICKER_CLEAN_MIN_REMAINING_LENGTH = 2

    const val LEAKED_STICKER_MAX_LENGTH = 20

    const val PROACTIVE_FALLBACK_MIN_MINUTES = 15L

    const val PROACTIVE_FALLBACK_MAX_MINUTES = 60L

    const val PROACTIVE_USER_MIN_INTERVAL_MINUTES = 1

    const val PROACTIVE_USER_MAX_INTERVAL_MINUTES = 1440

    const val FOLLOW_UP_REMINDER_DEFAULT_INTERVAL_MINUTES = 5

    const val FOLLOW_UP_REMINDER_DEFAULT_MAX_TIMES = 3

    const val FOLLOW_UP_REMINDER_MIN_INTERVAL_MINUTES = 1

    const val FOLLOW_UP_REMINDER_MAX_INTERVAL_MINUTES = 120

    const val FOLLOW_UP_REMINDER_MAX_TIMES_LIMIT = 10

    const val FOLLOW_UP_REMINDER_MAX_AGE_HOURS = 24

    const val QQ_BOT_HEARTBEAT_FACTOR = 0.8

    const val QQ_BOT_HEARTBEAT_MIN_MS = 5000L

    const val QQ_BOT_RECONNECT_BACKOFF_BASE_MS = 2000L

    const val QQ_BOT_RECONNECT_MAX_DELAY_MS = 30000L

    const val WECHAT_SERVICE_POLL_TIMEOUT_MS = 20000L

    const val WECHAT_SERVICE_TIMEOUT_RETRY_DELAY_MS = 3000L

    const val WECHAT_SERVICE_CONNECTION_RETRY_DELAY_MS = 5000L

    const val WECHAT_SERVICE_ERROR_RETRY_DELAY_MS = 5000L

    const val GROUP_CHAT_REPLY_DELAY_0_MIN_MS = 100L
    const val GROUP_CHAT_REPLY_DELAY_0_MAX_MS = 400L

    const val GROUP_CHAT_REPLY_DELAY_1_MIN_MS = 300L
    const val GROUP_CHAT_REPLY_DELAY_1_MAX_MS = 700L

    const val GROUP_CHAT_REPLY_DELAY_2_MIN_MS = 500L
    const val GROUP_CHAT_REPLY_DELAY_2_MAX_MS = 900L

    const val GROUP_CHAT_REPLY_DELAY_DEFAULT_MIN_MS = 700L
    const val GROUP_CHAT_REPLY_DELAY_DEFAULT_MAX_MS = 1200L

    const val GROUP_CHAT_ROUND_GAP_MIN_MS = 800L
    const val GROUP_CHAT_ROUND_GAP_MAX_MS = 2000L

    const val GROUP_CHAT_TEXT_SEGMENT_DELAY_MIN_MS = 800L
    const val GROUP_CHAT_TEXT_SEGMENT_DELAY_MAX_MS = 1600L

    const val GROUP_CHAT_STICKER_DELAY_MIN_MS = 600L
    const val GROUP_CHAT_STICKER_DELAY_MAX_MS = 1200L

    const val PROACTIVE_TIME_THRESHOLD_MINUTES = 3

    const val PROACTIVE_SHORT_MESSAGE_LENGTH = 3

    const val PROACTIVE_CONTEXT_MESSAGE_COUNT = 8

    const val PROACTIVE_TOPIC_COUNT = 3

    const val PROACTIVE_TOPIC_MIN_MESSAGES = 2

    const val PROACTIVE_TIME_FLOW_THRESHOLD_MINUTES = 10

    const val PROACTIVE_GAP_REALTIME_MINUTES = 1

    const val PROACTIVE_GAP_RECENT_MINUTES = 5

    const val PROACTIVE_GAP_MEDIUM_MINUTES = 15

    const val PROACTIVE_GAP_LONG_MINUTES = 60

    const val POST_PROCESS_MAX_SENTENCES = 10

    const val POST_PROCESS_LONG_CUT_THRESHOLD = 360

    const val POST_PROCESS_CUT_CANDIDATE_LENGTH = 300

    const val POST_PROCESS_CUT_MIN_POSITION = 40

    const val ECHO_MIN_USER_LENGTH = 4

    const val ECHO_LENGTH_TOLERANCE = 3

    const val ECHO_MIN_REMAINING_LENGTH = 2

    const val REPEAT_NICKNAME_LOOKBACK_ROUNDS = 5

    const val REPEAT_NICKNAME_MIN_WORD_LENGTH = 2

    const val EXTRACT_DIRECT_REPLY_MIN_QUOTED_LENGTH = 2

    const val EXTRACT_DIRECT_REPLY_PARAGRAPH_THRESHOLD = 80

    const val EXTRACT_DIRECT_REPLY_MIN_PARAGRAPHS = 2

    const val BUILD_MESSAGES_DEFAULT_CONTEXT_LIMIT = 12

    const val DEFAULT_COMPRESSION_KEEP_RATIO = 0.5f

    const val DEFAULT_COMPRESSION_MIN_KEEP = 6

    const val LOCAL_STICKER_PROBABILITY_DEFAULT = 30

    const val SYSTEM_PROMPT_MAX_STICKERS = 50

    const val LOCAL_MIN_RESPONSE_LENGTH = 20

    const val PERSONA_MAX_SENTENCES = 12

    const val PERSONA_MIN_CHARS = 1

    const val PERSONA_MAX_CHARS = 300

    const val STICKER_PROBABILITY_HIGH = 80

    const val STICKER_PROBABILITY_MEDIUM = 50

    const val STICKER_PROBABILITY_LOW = 20

    const val CONVERSATION_REOPEN_GAP_MS = 2L * 60L * 60L * 1000L

    const val CONVERSATION_PHASE_LOOKBACK = 40

    const val ENV_ANCHOR_COOLDOWN_MS = 2L * 60L * 60L * 1000L

    const val ENV_ANCHOR_RECENT_LOOKBACK = 8
}
