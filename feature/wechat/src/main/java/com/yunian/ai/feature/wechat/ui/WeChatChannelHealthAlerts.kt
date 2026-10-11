package com.yunian.ai.feature.wechat.ui

import com.yunian.ai.domain.wechat.WeChatChannelHealthSnapshot
import com.yunian.ai.domain.wechat.WeChatFailureReason

/**
 * 通道健康卡的「失败信息」呈现逻辑（债务 D2 / G5）。
 *
 * ## 为什么单独抽一个文件
 *
 * 纯 JVM 单测跑不了 Compose 渲染，所以「该显示哪条文案、按什么优先级、什么时候不显示」
 * 这层判断必须与 Composable 分离才能被自动化验证。本文件**零 Compose / 零 Android 依赖**
 * （只用 core:domain 的数据类 + 常量），因此可以直接在 feature:wechat 的 junit 测试里跑。
 * [WeChatSettingsScreen] 的 ChannelHealthCard 只负责把 [ChannelHealthAlert] 画出来。
 *
 * ## 数据来源：不新增任何状态通道
 *
 * 失败文案**本来就存在**，只是从没被渲染过。本文件只读既有的
 * [WeChatChannelHealthSnapshot]，不新增 StateFlow、不新增 Runtime 字段：
 *
 * - 会话过期（iLink errcode=-14）：WeChatChannelRuntime.markSessionExpired 把
 *   「微信会话已过期（errcode=-14），请重新扫码登录」写进 lastError（前缀 `poll_auth: `），
 *   常量见 WeChatChannelRuntime.SESSION_EXPIRED_USER_MESSAGE；
 * - context_token 缺失 / 过期：WeChatOutboxCoordinator 在发送前拦截，把
 *   SEND_BLOCKED_NO_TOKEN / SEND_BLOCKED_EXPIRED 两条**用户可见文案**写进 outbox 行的
 *   lastError（前缀 `outbox_dead: `），由 WeChatOutboxCoordinator.recentFailures()
 *   → healthSnapshot(recentFailures = …) 透出；
 * - 轮询连续失败：WeChatChannelRuntime.onPollFailure 写入的 `<wireName>: <原始异常>`。
 *
 * ## 明确说明：有一个字段**没有**暴露（本次不改它）
 *
 * 「会话过期冷却剩余时间」（WeChatChannelRuntime.sessionExpiredUntilMs，1 小时）**不在**
 * 快照里——[WeChatChannelHealthSnapshot] 没有对应字段，公开访问器 isSessionExpired() 也只
 * 返回布尔。因此本文件**不显示倒计时**，只用 lastError 里的 errcode=-14 标记判定「现在正
 * 处在会话过期状态」。没有为此新增暴露，理由有三：
 *
 * 1. 任务硬性约束 2 要求不改 WeChatChannelRuntime 的语义；加字段虽不算改语义，但属于
 *    「新增状态通道」的邻域，能不碰就不碰；
 * 2. 倒计时对用户**没有可执行含义**：冷却到点也不会自愈，用户仍然必须重新扫码登录
 *    （onPollSuccess 才是解除条件，冷却只是防止无意义重试）；
 * 3. 用 lastError 判定有一个**已知且可接受的盲区**：冷却期内若出现一次成功轮询，
 *    onPollSuccess 会清空 lastError，此时卡片不再提示「会话过期」而冷却可能仍在。
 *    该盲区在实践中不可达——errcode=-14 意味着 iLink 会话已失效，冷却期内轮询不可能成功；
 *    真的成功了，也就意味着用户已经重新登录，不提示才是正确的。
 *
 * ## 设计取舍（任务要求写在 KDoc 里）
 *
 * 1. **只在有失败时多渲染**：alerts 为空（通道健康）时健康卡保持改动前的样子，一行都不多。
 *    理由：健康卡是「看一眼就走」的状态区，常态下加固定行数是纯噪音，而失败是少数态——
 *    把视觉预算全留给少数态，用户才会真的注意到它。
 * 2. **按「可操作性」排序，不按时间排序**：会话过期 → token 缺失/过期 → 轮询失败 →
 *    发送失败。理由：用户只能对前两类做点什么（重新扫码 / 让对方发一条消息），轮询与发送
 *    失败是**症状**，把它们排前面会把唯一的可操作提示挤到折叠线以下。
 * 3. **显示 recentFailures 的细节，但有上限**：最多 [MAX_SEND_FAILURE_ROWS] 条，超出时补
 *    一行「另有 N 条…」。理由：recentFailures 是「哪条消息没发出去」的唯一线索，不显示
 *    等于把「对方没收到」变成玄学；但它是**列表**，全量渲染会把卡片撑成一面墙，所以在
 *    信息量与高度之间取 3 条这个折中。
 * 4. **同一条失败去重，并且 token 类只渲染一次**：
 *    - outbox 明细里凡是命中 token 标记的行，**一律跳过**——它已经被提升成「该让对方发一条
 *      消息」那句可操作提示了，再按 `outbox_dead: …` 原文渲染一遍就是同一件事说两遍
 *      （用户会以为是两次故障，卡片也被撑高）；
 *    - 其余明细用「正文 + 细节」做去重键，而不是只按正文：不把「同样是 poll_timeout、
 *      但两次原因不同」的两条合并掉。
 * 5. **显示「多久之前」而不是绝对时刻**：轮询失败与发送失败都补一句 `(N 分钟前)`。
 *    理由：用户判断的是「现在还坏着吗」，绝对时刻需要他心算；且相对时间可以直接由
 *    lastErrorAtMs / WeChatOutboxFailure.updatedAtMs 推出，不需要新通道。nowMs 由调用方
 *    注入，方便单测。
 * 6. **文案保留技术原文**：会话过期的提示会带上原始 errcode=-14，发送失败会带上
 *    `outbox_dead: …` 原文。理由：这条信息在用户截图求助时是唯一能定位的线索；卡片本来
 *    就是技术状态区（同一张卡已经显示 consecutiveFailures 与 outbox 计数），刻意「净化」
 *    成纯自然语言反而丢掉了排障价值。
 */
object WeChatChannelHealthAlerts {

    /** 会话过期标记：iLink 官方 errcode=-14，以及中英文两种「会话已过期」写法。 */
    private val SESSION_EXPIRED_MARKERS = listOf(
        "errcode=-14",
        "会话已过期",
        "会话过期",
        "session expired",
    )

    /** context_token 缺失（对方从未发过消息）。 */
    private val NO_TOKEN_MARKERS = listOf(
        "context_token 缺失",
        "contexttoken 缺失",
        "missing latest context token",
    )

    /** context_token 过期（超过 24 小时）。 */
    private val EXPIRED_TOKEN_MARKERS = listOf(
        "context_token 已过期",
        "contexttoken 已过期",
    )

    /**
     * lastError 的格式是 `<wireName>: <detail>`（由 WeChatChannelRuntime 与
     * WeChatOutboxCoordinator 共同保证），这是两段之间的分隔符。
     */
    private const val DETAIL_SEPARATOR = ": "

    /** 卡片里最多渲染几条 outbox 发送失败——见设计取舍 3。 */
    const val MAX_SEND_FAILURE_ROWS = 3

    /** 单条失败文案的截断长度：够看清原因，又不至于把卡片撑破。 */
    const val MAX_MESSAGE_CHARS = 160

    /** 严重度，只影响渲染用的颜色，不参与排序。 */
    enum class AlertSeverity { ERROR, WARNING, INFO }

    /**
     * 一条要渲染的失败提示。
     *
     * @param headline 用户第一眼看到的那句（结论 / 该怎么办）
     * @param detail 补充细节（原始错误、发生时间）；为 null 时不渲染第二行
     * @param severity 颜色
     * @param actionRequired 是否**必须用户手动操作**才能恢复（目前只有会话过期与 token 类满足）
     */
    data class ChannelHealthAlert(
        val headline: String,
        val detail: String? = null,
        val severity: AlertSeverity = AlertSeverity.WARNING,
        val actionRequired: Boolean = false,
    )

    /**
     * 把健康快照翻译成要渲染的提示列表；**没有失败时返回空列表**（卡片保持改动前的样子）。
     *
     * @param nowMs 用于算「多久之前」，注入以便单测
     */
    fun fromSnapshot(
        health: WeChatChannelHealthSnapshot,
        nowMs: Long = System.currentTimeMillis(),
    ): List<ChannelHealthAlert> {
        val alerts = mutableListOf<ChannelHealthAlert>()

        val lastErrorDetail = health.lastError?.let(::detailOf)?.takeIf { it.isNotBlank() }
        val sessionExpired = lastErrorDetail != null && containsAny(lastErrorDetail, SESSION_EXPIRED_MARKERS)

        if (sessionExpired) {
            alerts += ChannelHealthAlert(
                headline = SESSION_EXPIRED_HEADLINE,
                detail = "errcode=-14" + ageSuffix(health.lastErrorAtMs, nowMs),
                severity = AlertSeverity.ERROR,
                actionRequired = true,
            )
        }

        val tokenAlert = tokenAlertOf(health, lastErrorDetail, nowMs)
        if (tokenAlert != null) alerts += tokenAlert

        pollFailureAlert(health, lastErrorDetail, sessionExpired, tokenAlert, nowMs)?.let { alerts += it }

        alerts += sendFailureAlerts(health, alerts, nowMs)

        return alerts
    }

    /**
     * context_token 类失败：优先看 lastError（它是「当前」状态），没有命中时再从
     * [WeChatChannelHealthSnapshot.recentFailures] 里找最近一条。
     *
     * 两条正文与 WeChatOutboxCoordinator.SEND_BLOCKED_NO_TOKEN / SEND_BLOCKED_EXPIRED 同义，
     * 不在 UI 侧重写成另一套说法，避免两处漂移。
     */
    private fun tokenAlertOf(
        health: WeChatChannelHealthSnapshot,
        lastErrorDetail: String?,
        nowMs: Long,
    ): ChannelHealthAlert? {
        if (lastErrorDetail != null) {
            if (containsAny(lastErrorDetail, NO_TOKEN_MARKERS)) {
                return ChannelHealthAlert(
                    headline = NO_TOKEN_HEADLINE,
                    detail = TOKEN_NO_TOKEN_CAUSE + ageSuffix(health.lastErrorAtMs, nowMs),
                    severity = AlertSeverity.ERROR,
                    actionRequired = true,
                )
            }
            if (containsAny(lastErrorDetail, EXPIRED_TOKEN_MARKERS)) {
                return ChannelHealthAlert(
                    headline = EXPIRED_TOKEN_HEADLINE,
                    detail = TOKEN_EXPIRED_CAUSE + ageSuffix(health.lastErrorAtMs, nowMs),
                    severity = AlertSeverity.ERROR,
                    actionRequired = true,
                )
            }
        }
        val failure = health.recentFailures.firstOrNull { matchesTokenMarkers(it.lastError) } ?: return null
        val noToken = matchesNoTokenMarkers(failure.lastError)
        return ChannelHealthAlert(
            headline = if (noToken) NO_TOKEN_HEADLINE else EXPIRED_TOKEN_HEADLINE,
            detail = (if (noToken) TOKEN_NO_TOKEN_CAUSE else TOKEN_EXPIRED_CAUSE) +
                ageSuffix(failure.updatedAtMs, nowMs),
            severity = AlertSeverity.ERROR,
            actionRequired = true,
        )
    }

    /**
     * 轮询失败：consecutiveFailures > 0 且 lastError 不是会话过期、也不是 token 类
     * （那两类已单独渲染，重复渲染会让用户以为是三次故障）。
     *
     * 只保留 lastError 里已经截断过的正文，不追加堆栈——见设计取舍 6。
     */
    private fun pollFailureAlert(
        health: WeChatChannelHealthSnapshot,
        lastErrorDetail: String?,
        sessionExpired: Boolean,
        tokenAlert: ChannelHealthAlert?,
        nowMs: Long,
    ): ChannelHealthAlert? {
        if (sessionExpired || health.consecutiveFailures <= 0) return null
        if (lastErrorDetail == null) return null
        if (tokenAlert != null && matchesTokenMarkers(lastErrorDetail)) return null
        val reason = WeChatFailureReason.fromPollMessage(lastErrorDetail)
        return ChannelHealthAlert(
            headline = "轮询连续失败 " + health.consecutiveFailures + " 次（" + reasonLabel(reason) + "）",
            detail = lastErrorDetail.take(MAX_MESSAGE_CHARS) + ageSuffix(health.lastErrorAtMs, nowMs),
            severity = AlertSeverity.WARNING,
        )
    }

    /**
     * outbox 发送失败明细——见设计取舍 3（条数上限）与 4（与上文去重）。
     *
     * recentFailures 已由 DAO 按 updatedAtMs 倒序返回，这里保持原序不再重排。
     */
    private fun sendFailureAlerts(
        health: WeChatChannelHealthSnapshot,
        existing: List<ChannelHealthAlert>,
        nowMs: Long,
    ): List<ChannelHealthAlert> {
        if (health.recentFailures.isEmpty()) return emptyList()
        val seen = existing.map { dedupeKey(it.headline, it.detail) }.toMutableSet()
        val rows = mutableListOf<ChannelHealthAlert>()
        var hidden = 0
        for (failure in health.recentFailures) {
            // 已被提升成 token 类提示（或与会话过期同一件事）的行不再重复渲染——见设计取舍 4。
            if (matchesTokenMarkers(failure.lastError)) continue
            val detail = failure.lastError?.let(::detailOf)?.takeIf { it.isNotBlank() }
            val detailLine = detail?.take(MAX_MESSAGE_CHARS)?.let { it + ageSuffix(failure.updatedAtMs, nowMs) }
            if (!seen.add(dedupeKey(SEND_FAILED_HEADLINE, detailLine))) continue
            if (rows.size >= MAX_SEND_FAILURE_ROWS) {
                hidden++
                continue
            }
            rows += ChannelHealthAlert(
                headline = SEND_FAILED_HEADLINE,
                detail = detailLine,
                severity = AlertSeverity.WARNING,
            )
        }
        if (hidden > 0) {
            rows += ChannelHealthAlert(
                headline = "另有 " + hidden + " 条发送失败未显示",
                severity = AlertSeverity.INFO,
            )
        }
        return rows
    }

    /**
     * 从 `<wireName>: <detail>` 里取出 `<detail>`。
     *
     * 只在**第一个** `": "` 处切分：两段格式由 WeChatChannelRuntime /
     * WeChatOutboxCoordinator 保证，detail 自身可能再含冒号。没有分隔符时原样返回
     * （防御性：格式若变化，宁可显示原文也不要显示空白）。
     */
    fun detailOf(lastError: String): String =
        lastError.substringAfter(DETAIL_SEPARATOR, lastError).trim()

    /**
     * 除 [shown] 里已经渲染的那几条之外，还有几条 outbox 失败**被上限挡掉**（0 = 全部已渲染）。
     *
     * 与 [sendFailureAlerts] 用**同一套**去重键、截断规则与 token 跳过规则，所以 UI 上
     * 「另有 N 条」的 N 与实际隐藏条数恒等；把这段判断放在这里而不是 Composable 里，
     * 正是为了让它可单测。
     *
     * [shown] 应为同一快照和同一 nowMs 生成的完整提示列表。已展示项和重复项均不计数；
     * 剩余唯一项就是被上限挡掉的条数，不能再减一次展示上限。
     */
    fun hiddenSendFailureCount(
        health: WeChatChannelHealthSnapshot,
        shown: List<ChannelHealthAlert>,
        nowMs: Long = System.currentTimeMillis(),
    ): Int {
        if (health.recentFailures.isEmpty()) return 0
        val seen = shown.map { dedupeKey(it.headline, it.detail) }.toMutableSet()
        var hidden = 0
        for (failure in health.recentFailures) {
            if (matchesTokenMarkers(failure.lastError)) continue
            val detail = failure.lastError?.let(::detailOf)?.takeIf { it.isNotBlank() }
            val detailLine = detail?.take(MAX_MESSAGE_CHARS)?.let { it + ageSuffix(failure.updatedAtMs, nowMs) }
            // 已展示或已计数的重复项跳过；仅首次出现的未展示项计入隐藏数。
            if (!seen.add(dedupeKey(SEND_FAILED_HEADLINE, detailLine))) continue
            hidden++
        }
        return hidden
    }

    /** WeChatOutboxFailure.lastError 是否属于 context_token 类失败。 */
    fun matchesTokenMarkers(lastError: String?): Boolean =
        matchesNoTokenMarkers(lastError) || matchesExpiredTokenMarkers(lastError)

    private fun matchesNoTokenMarkers(lastError: String?): Boolean =
        lastError != null && containsAny(lastError, NO_TOKEN_MARKERS)

    private fun matchesExpiredTokenMarkers(lastError: String?): Boolean =
        lastError != null && containsAny(lastError, EXPIRED_TOKEN_MARKERS)

    private fun containsAny(text: String, markers: List<String>): Boolean {
        val lower = text.lowercase()
        return markers.any { lower.contains(it.lowercase()) }
    }

    /** 去重键——见设计取舍 4。用 \u0000 分隔，避免正文与细节拼串后产生歧义碰撞。 */
    private fun dedupeKey(headline: String, detail: String?): String =
        headline + "\u0000" + detail.orEmpty()

    /**
     * 「多久之前」的渲染片段（含前置分隔符）。atMs <= 0（从未记录）或 nowMs <= atMs
     * （时钟回拨 / 刚发生）返回空串，此时调用方不渲染这一段——显示「0 秒前」或负数
     * 都比不显示更糟。
     */
    private fun ageSuffix(atMs: Long, nowMs: Long): String {
        val label = formatAge(atMs, nowMs) ?: return ""
        return " · " + label
    }

    /** 见 [ageSuffix]。公开以便单测直接覆盖边界（0 / 时钟回拨 / 各档位）。 */
    fun formatAge(atMs: Long, nowMs: Long): String? {
        if (atMs <= 0L || nowMs <= atMs) return null
        val seconds = (nowMs - atMs) / 1000L
        return when {
            seconds < 60L -> "刚刚"
            seconds < 3600L -> (seconds / 60L).toString() + " 分钟前"
            seconds < 86_400L -> (seconds / 3600L).toString() + " 小时前"
            else -> (seconds / 86_400L).toString() + " 天前"
        }
    }

    /** 失败原因的中文标签，只用于轮询失败那一行的括注。 */
    private fun reasonLabel(reason: WeChatFailureReason): String = when (reason) {
        WeChatFailureReason.POLL_TIMEOUT -> "轮询超时"
        WeChatFailureReason.POLL_CONNECTION -> "网络连接失败"
        WeChatFailureReason.POLL_AUTH -> "鉴权失败"
        WeChatFailureReason.POLL_UNKNOWN -> "原因未知"
        WeChatFailureReason.OUTBOX_SEND -> "发送失败"
        WeChatFailureReason.OUTBOX_DEAD -> "发送已放弃"
        WeChatFailureReason.MAPPING_MISSING -> "缺少用户映射"
        WeChatFailureReason.MAPPING_INVALID -> "用户映射无效"
        WeChatFailureReason.DIALOGUE_FAILED -> "AI 回复失败"
        WeChatFailureReason.TRANSPORT_REBUILD -> "通道重建中"
    }

    /** 会话过期的用户可见正文——与 WeChatChannelRuntime.SESSION_EXPIRED_USER_MESSAGE 同义。 */
    const val SESSION_EXPIRED_HEADLINE = "微信会话已过期（errcode=-14），请重新扫码登录"

    /** 与 WeChatOutboxCoordinator.SEND_BLOCKED_NO_TOKEN 同义。 */
    const val NO_TOKEN_HEADLINE =
        "context_token 缺失：iLink 协议只能回复对方先发来的消息，请让对方先发一条消息"

    /** 与 WeChatOutboxCoordinator.SEND_BLOCKED_EXPIRED 同义。 */
    const val EXPIRED_TOKEN_HEADLINE =
        "context_token 已过期（会话超过 24 小时）：请让对方重新发送一条消息后再试"

    const val SEND_FAILED_HEADLINE = "微信消息发送失败"

    private const val TOKEN_NO_TOKEN_CAUSE =
        "原因：本会话没有可用的会话凭证（context_token）。iLink 只能回复对方先发来的消息——" +
            "若对方已给机器人发过消息仍报此项，说明服务端未下发凭证，此时重新绑定无效，请导出日志反馈"
    private const val TOKEN_EXPIRED_CAUSE = "原因：context_token 超过 24 小时未刷新"
}
