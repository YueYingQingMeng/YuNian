package com.yunian.ai.wechat.outbox

import com.yunian.ai.common.SecureLog
import com.yunian.ai.database.dao.WeChatOutboxDao
import com.yunian.ai.database.model.WeChatOutboxEntity
import com.yunian.ai.domain.wechat.WeChatContentKind
import com.yunian.ai.domain.wechat.WeChatDeliveryStatus
import com.yunian.ai.domain.wechat.WeChatFailureReason
import com.yunian.ai.domain.wechat.WeChatOutboundRequest
import com.yunian.ai.domain.wechat.WeChatOutboxFailure
import com.yunian.ai.wechat.ilink.IlinkSessionExpiredException
import com.yunian.ai.wechat.ilink.IlinkSessionStore
import com.yunian.ai.wechat.map.WeChatOutboundSegmenter
import com.yunian.ai.wechat.transport.WeChatTransportPort
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.File
import java.util.UUID

class WeChatOutboxCoordinator(
    private val dao: WeChatOutboxDao,
    private val transport: WeChatTransportPort,
    private val sessionStore: IlinkSessionStore? = null,
    private val segmentGapMs: Long = DEFAULT_SEGMENT_GAP_MS,
    private val maxRetry: Int = DEFAULT_MAX_RETRY,
    private val nowMs: () -> Long = { System.currentTimeMillis() },
    private val managedMediaCacheDir: File? = null,
    private val staleSendingRecoveryEnabled: Boolean = true,
    private val staleSendingRecoveryLimit: Int = DEFAULT_STALE_RECOVERY_LIMIT,
    /**
     * 僵尸行恢复的外部观察者：(已重置回待发的行数, 已判死的行数)。
     *
     * core:wechat 是纯库模块、拿不到 BuildConfig，而 SecureLog.i/w 在正式包里是空操作；
     * 由调用方（feature:wechat，有 BuildConfig）注入正式包可见的记录通道。
     */
    private val staleSendingRecoveryObserver: ((recovered: Int, dead: Int) -> Unit)? = null,
) {
    private val drainMutex = Mutex()

    /** 每个进程只做一次 SENDING 僵尸恢复：drain 被轮询/Worker/回复链路高频调用，不能每次都扫。 */
    @Volatile
    private var staleSendingRecoveryDone = false

    /** 每个进程只做一次「凭证判死行」复活（同理：drain 高频调用）。 */
    @Volatile
    private var tokenBlockedRevivalDone = false

    suspend fun enqueue(
        request: WeChatOutboundRequest,
        wechatUserId: String = request.wechatUserId.orEmpty(),
        rootId: String = UUID.randomUUID().toString(),
    ): String {
        require(wechatUserId.isNotBlank()) { "wechatUserId blank" }
        val created = nowMs()
        val entities = buildEntities(request, wechatUserId, rootId, created)
        if (entities.isNotEmpty()) {
            dao.insertAll(entities)
            SecureLog.i(
                TAG,
                "enqueue rootId=${rootId.take(8)} segs=${entities.size} kind=${entities.firstOrNull()?.kind} user=${maskUser(wechatUserId)}",
            )
        }
        return rootId
    }

    suspend fun drain(limit: Int = DEFAULT_DRAIN_LIMIT): Int = drainMutex.withLock {
        // 先恢复上一次进程留下的 SENDING 僵尸行（详见 WeChatOutboxRecovery）：
        // 恢复失败不能连累正常 drain，因此整体 runCatching。
        runCatching { recoverStaleSendingOnce() }
            .onFailure { SecureLog.w(TAG, "stale_sending recovery failed: ${it.message}") }
        // 再复活历史「因缺 context_token 被判死」的行（本进程一次）：老版本的判死永不可逆，
        // 用户即使后来正常收到凭证，那批回复也永远不会再发出去（Issue #2 关联故障）。
        runCatching { reviveTokenBlockedDeadRowsOnce() }
            .onFailure { SecureLog.w(TAG, "token-blocked revival failed: ${it.message}") }

        val ready = dao.listReady(nowMs = nowMs(), limit = limit)
        var sent = 0
        val deliveries = ready.map { it.rootId }.distinct().map { rootId ->
            dao.listOpenByRootId(rootId).ifEmpty { ready.filter { it.rootId == rootId } }
        }
        for ((index, items) in deliveries.withIndex()) {
            if (index > 0 && segmentGapMs > 0) {
                delay(segmentGapMs)
            }
            val delivered = if (
                items.all { WeChatContentKind.fromWireType(it.kind) == WeChatContentKind.TEXT } &&
                items.all { it.nextAttemptAtMs <= nowMs() }
            ) {
                dispatchTextSegments(items.sortedBy { it.segmentIndex })
            } else {
                items.filter { it.nextAttemptAtMs <= nowMs() }.count { dispatchOne(it) }
            }
            sent += delivered
        }

        val cutoff = nowMs() - SENT_RETENTION_MS
        dao.deleteSentBefore(cutoff)
        dao.deleteDeadBefore(maxRetry = maxRetry, cutoffMs = cutoff)
        cleanupManagedCache(dao.listMediaLocalPaths().toSet(), cutoff)
        if (sent > 0) {
            SecureLog.i(TAG, "drain sent=$sent ready=${ready.size}")
        } else if (ready.isNotEmpty()) {
            // 「明明排了队却一条都没发出去」是用户报「无法同步」的典型现象，
            // 必须留 I 级证据：正式包 Log.d 会被 proguard 剥离。
            val sample = recentFailures(DEFAULT_RECENT_FAILURE_LIMIT)
                .joinToString(" | ") { "${it.kind}:${it.lastError?.take(80)}" }
            SecureLog.i(
                TAG,
                "drain blocked ready=${ready.size} sent=0 recentFailures=$sample",
            )
        }
        sent
    }

    /**
     * 把上一次进程残留的 SENDING 行恢复成 PENDING。每个进程只跑一次，由 [drain] 串行调用。
     *
     * 触发时机即「启动时可恢复」：drain 由 WeChatPollingService 主循环、WeChatPollingWorker、
     * WeChatAiReplyWorker 以及回复链路共同驱动，进程起来后第一条 drain 就会执行本恢复。
     *
     * @return 被重置回 PENDING 的行数（0 表示没有僵尸行，或本进程已经恢复过）
     */
    suspend fun recoverStaleSending(): Int {
        if (!staleSendingRecoveryEnabled) return 0
        val now = nowMs()
        val candidates = WeChatOutboxRecovery.readStaleSending(
            dao = dao,
            nowMs = now,
            limit = staleSendingRecoveryLimit,
        )
        if (candidates.isEmpty()) return 0
        val plan = WeChatOutboxRecovery.plan(candidates, nowMs = now, maxRetry = maxRetry)
        val recovered = WeChatOutboxRecovery.applyPlan(dao, plan, nowMs = now)
        WeChatOutboxRecovery.log(plan, nowMs = now)
        staleSendingRecoveryObserver?.invoke(plan.recovered, plan.dead)
        return recovered
    }

    private suspend fun recoverStaleSendingOnce() {
        if (!staleSendingRecoveryEnabled || staleSendingRecoveryDone) return
        recoverStaleSending()
        staleSendingRecoveryDone = true
    }

    /**
     * 复活历史上「因缺 context_token 被判死」的行：状态回到 PENDING、立即到期、失败计数清零。
     *
     * 为什么必须做：旧实现把「凭证缺失/过期」当 non-retryable 直接判死
     * （FAILED + `NEVER_RETRY_AT_MS`），而凭证完全可能在下一条入站消息时重新落库——
     * 判死之后这些回复**永远不会再发**，用户看到的就是「一直在发消息，AI 就是同步不到微信」。
     * 修复本身（[updateFailure] 走挂起）只对**新**失败生效，因此这里对存量行做一次性迁移。
     *
     * 每个进程只做一次（由 [drain] 串行调用），扫描上限 [TOKEN_REVIVAL_SCAN_LIMIT]。
     *
     * @return 被复活的行数（0 = 没有历史判死行，或本进程已复活过）
     */
    suspend fun reviveTokenBlockedDeadRows(limit: Int = TOKEN_REVIVAL_SCAN_LIMIT): Int {
        val dead = dao.listRecentFailed(limit).filter { isContextTokenWaitMessage(it.lastError) }
        if (dead.isEmpty()) return 0
        val now = nowMs()
        for (row in dead) {
            dao.updateAttempt(
                id = row.id,
                status = WeChatDeliveryStatus.PENDING.name,
                retryCount = 0,
                nextAttemptAtMs = 0L, // 立即到期：凭证若已存在，本轮 drain 就会发出去
                lastError = "$WAITING_CONTEXT_TOKEN: revived_from_dead: ${row.lastError.orEmpty().take(120)}",
                updatedAtMs = now,
            )
        }
        SecureLog.i(TAG, "revived token-blocked dead rows count=${dead.size}")
        staleSendingRecoveryObserver?.invoke(dead.size, 0)
        return dead.size
    }

    private suspend fun reviveTokenBlockedDeadRowsOnce() {
        if (tokenBlockedRevivalDone) return
        reviveTokenBlockedDeadRows()
        tokenBlockedRevivalDone = true
    }

    private suspend fun dispatchTextSegments(items: List<WeChatOutboxEntity>): Int {
        var sent = 0
        for ((index, item) in items.withIndex()) {
            if (index > 0 && segmentGapMs > 0) {
                delay(segmentGapMs)
            }
            if (dispatchOne(item)) {
                sent++
            }
        }
        return sent
    }

    internal fun cleanupManagedCache(referencedPaths: Set<String>, cutoffMs: Long): Int {
        val cacheDir = managedMediaCacheDir?.takeIf { it.exists() && it.isDirectory } ?: return 0
        val canonicalDir = runCatching { cacheDir.canonicalFile }.getOrNull() ?: return 0
        val canonicalReferences = referencedPaths.mapNotNullTo(mutableSetOf()) { path ->
            runCatching { File(path).canonicalPath }.getOrNull()
        }
        return canonicalDir.listFiles().orEmpty().count { file ->
            file.isFile &&
                file.lastModified() < cutoffMs &&
                file.canonicalFile.parentFile == canonicalDir &&
                file.canonicalPath !in canonicalReferences &&
                file.delete()
        }
    }

    suspend fun openCount(): Int = dao.countOpen()

    suspend fun countByStatus(status: WeChatDeliveryStatus): Int =
        dao.countByStatus(status.name)

    suspend fun recentFailures(limit: Int = DEFAULT_RECENT_FAILURE_LIMIT): List<WeChatOutboxFailure> {
        return dao.listRecentFailed(limit).map { entity ->
            WeChatOutboxFailure(
                id = entity.id,
                wechatUserId = entity.wechatUserId,
                kind = WeChatContentKind.fromWireType(entity.kind).name,
                retryCount = entity.retryCount,
                lastError = entity.lastError,
                updatedAtMs = entity.updatedAtMs,
            )
        }
    }

    fun buildEntities(
        request: WeChatOutboundRequest,
        wechatUserId: String,
        rootId: String,
        createdAtMs: Long,
    ): List<WeChatOutboxEntity> {
        val segments = WeChatOutboundSegmenter.expand(request, wechatUserId, rootId)
        return segments.map { seg ->
            WeChatOutboxEntity(
                id = seg.outboxId,
                rootId = rootId,
                companionId = request.companionId,
                wechatUserId = wechatUserId,
                kind = seg.kind.wireType,
                text = seg.text,
                mediaLocalPath = seg.media?.localPath,
                mediaFileName = seg.media?.fileName,
                mediaDescription = seg.media?.description,
                segmentIndex = seg.segmentIndex,
                segmentCount = seg.segmentCount,
                sourceMessageId = request.sourceMessageId,
                status = WeChatDeliveryStatus.PENDING.name,
                retryCount = 0,
                nextAttemptAtMs = 0L,
                lastError = null,
                createdAtMs = createdAtMs,
                updatedAtMs = createdAtMs,
            )
        }
    }

    private suspend fun dispatchOne(item: WeChatOutboxEntity): Boolean {
        // updatedAtMs 就是 SENDING 租约的起点，必须与恢复逻辑用同一个时钟（nowMs），
        // 否则租约判断会依赖 Room 默认的 System.currentTimeMillis()，无法测试也不可推导。
        dao.updateStatus(
            id = item.id,
            status = WeChatDeliveryStatus.SENDING.name,
            updatedAtMs = nowMs(),
        )
        val result = runCatching {
            val contextToken = resolveContextToken(item.wechatUserId)
            // iLink 协议只能回复：无 token 或 token 过期时发送会被服务端静默丢弃（ret=0 但不投递）。
            // sessionStore 存在（生产环境）时发送前拦截并判死；未注入时保持旧行为交由 transport/SDK 处理。
            if (sessionStore != null) {
                if (contextToken.isNullOrBlank()) error(SEND_BLOCKED_NO_TOKEN)
                ensureContextTokenFresh(item.wechatUserId, contextToken)
            }
            when (WeChatContentKind.fromWireType(item.kind)) {
                WeChatContentKind.TEXT -> {
                    val text = item.text?.takeIf { it.isNotBlank() }
                        ?: error("empty text segment")
                    transport.sendText(item.wechatUserId, text, contextToken).getOrThrow()
                }
                WeChatContentKind.IMAGE -> {
                    val path = item.mediaLocalPath ?: error("image path missing")
                    val bytes = File(path).takeIf { it.exists() }?.readBytes()
                        ?: error("image file missing: $path")
                    transport.sendImage(
                        toUserId = item.wechatUserId,
                        imageBytes = bytes,
                        fileName = item.mediaFileName ?: "image.png",
                        description = item.mediaDescription,
                        contextToken = contextToken,
                    ).getOrThrow()
                }
                else -> error("unsupported outbox kind=${item.kind}")
            }
        }
        return if (result.isSuccess) {
            dao.updateStatus(
                id = item.id,
                status = WeChatDeliveryStatus.SENT.name,
                updatedAtMs = nowMs(),
            )
            true
        } else {
            updateFailure(item, result.exceptionOrNull())
            false
        }
    }

    private suspend fun ensureContextTokenFresh(wechatUserId: String, contextToken: String) {
        val store = sessionStore ?: return
        val account = store.getSessionAccount() ?: return
        val savedAt = store.getContextTokenSavedAt(account.accountId, wechatUserId) ?: return
        val ageMs = nowMs() - savedAt
        if (ageMs > CONTEXT_TOKEN_MAX_AGE_MS) {
            SecureLog.w(TAG, "context token expired age=${ageMs / 1000}s user=${maskUser(wechatUserId)}")
            error(SEND_BLOCKED_EXPIRED)
        }
    }

    private suspend fun resolveContextToken(wechatUserId: String): String? =
        sessionStore?.getSessionAccount()?.let { account ->
            sessionStore.getContextToken(account.accountId, wechatUserId)
        }

    private suspend fun updateFailure(item: WeChatOutboxEntity, error: Throwable?) {
        val raw = error?.message ?: "send failed"

        // ── 「等待对方下一条消息」≠ 失败（G4 修正）──
        // iLink 协议只能回复对方先发来的消息：没有 context_token 时**任何重试都发不出去**，
        // 但对方下一条消息一到，凭证就会重新落库、本条立刻可发。
        // 旧实现把它当 non-retryable 判死（FAILED + NEVER_RETRY_AT_MS）→ 永不复活，
        // 用户看到的是「一直在发消息，AI 就是同步不到微信」，且健康卡把原因推给用户
        // （「对方尚未给机器人发过消息」），把排查引向无用的反复重绑。
        // 现改为**挂起**：保留 PENDING、不计失败次数、退避后自动重试。
        if (isContextTokenWait(error)) {
            dao.updateAttempt(
                id = item.id,
                status = WeChatDeliveryStatus.PENDING.name,
                retryCount = item.retryCount, // 不计入失败次数：这是等待，不是失败
                nextAttemptAtMs = nowMs() + CONTEXT_TOKEN_WAIT_BACKOFF_MS,
                lastError = "$WAITING_CONTEXT_TOKEN: ${raw.take(160)}",
                updatedAtMs = nowMs(),
            )
            SecureLog.i(
                TAG,
                "waiting token id=${item.id.take(8)} user=${maskUser(item.wechatUserId)} " +
                    "retry=${item.retryCount} backoff=${CONTEXT_TOKEN_WAIT_BACKOFF_MS}ms err=$raw",
            )
            return
        }

        val nextRetry = item.retryCount + 1
        val nonRetryable = isNonRetryable(error)
        val reason = if (nextRetry >= maxRetry || nonRetryable) {
            WeChatFailureReason.OUTBOX_DEAD
        } else {
            WeChatFailureReason.OUTBOX_SEND
        }
        val err = "${reason.wireName}: ${raw.take(160)}"
        if (nextRetry >= maxRetry || nonRetryable) {
            dao.updateAttempt(
                id = item.id,
                status = WeChatDeliveryStatus.FAILED.name,
                retryCount = nextRetry,
                nextAttemptAtMs = WeChatOutboxRecovery.NEVER_RETRY_AT_MS,
                lastError = err,
                updatedAtMs = nowMs(),
            )
            SecureLog.w(
                TAG,
                "dead id=${item.id.take(8)} retry=$nextRetry user=${maskUser(item.wechatUserId)} err=$err",
            )
        } else {
            val backoff = (1L shl nextRetry.coerceAtMost(6)) * 1000L
            dao.updateAttempt(
                id = item.id,
                status = WeChatDeliveryStatus.FAILED.name,
                retryCount = nextRetry,
                nextAttemptAtMs = nowMs() + backoff.coerceAtMost(60_000L),
                lastError = err,
                updatedAtMs = nowMs(),
            )
            SecureLog.w(
                TAG,
                "retry id=${item.id.take(8)} retry=$nextRetry backoff=${backoff.coerceAtMost(60_000L)}ms err=$err",
            )
        }
    }

    /**
     * 这次失败是否只是「还没有会话凭证」（可等待，不判死）。
     *
     * 命中两条发送前拦截文案（[SEND_BLOCKED_NO_TOKEN] / [SEND_BLOCKED_EXPIRED]）
     * 或 SDK 的 missing token 报错时成立。
     */
    private fun isContextTokenWait(error: Throwable?): Boolean =
        isContextTokenWaitMessage(error?.message)

    private fun isNonRetryable(error: Throwable?): Boolean {
        // iLink 会话过期（errcode=-14）：重新扫码前重试必然失败，直接判死
        if (error is IlinkSessionExpiredException) return true
        val message = (error?.message ?: return false).lowercase()
        return NON_RETRYABLE_MARKERS.any { message.contains(it) }
    }

    companion object {
        private const val TAG = "WeChatOutbox"
        const val DEFAULT_SEGMENT_GAP_MS = 0L
        const val DEFAULT_MAX_RETRY = 5
        const val DEFAULT_DRAIN_LIMIT = 20
        const val DEFAULT_RECENT_FAILURE_LIMIT = 5
        const val DEFAULT_STALE_RECOVERY_LIMIT = 50

        /**
         * SENDING 行的租约时长：超过它仍未落库就认定「发送它的进程已经死了」，重置回 PENDING。
         *
         * 取值依据（必须远大于任何一次单条发送的最坏耗时，否则会误判仍在发送中的行）：
         * - 文本：一次 sendmessage，IlinkHttpApi.API_TIMEOUT_MS = 15s（read 超时 40s）；
         * - 图片：getuploadurl 15s + CDN POST（connect 10s / write 30s / read 30s）≈ 最坏 ~70s；
         * - 进程被冻结（Doze/后台限制）时发送协程可能被整体挂起，给足余量避免把「还在发」当僵尸。
         * 5 分钟 ≈ 文本发送预算的 20 倍、图片预算的 4 倍。
         *
         * 租约越长，重复投递窗口越小，但僵尸消息滞留越久；这里选 5 分钟偏向「不重复」，
         * 因为僵尸行本来就已经丢了（迟到 5 分钟重发远好过永久丢失）。
         */
        const val SENDING_LEASE_MS = 5L * 60 * 1000

        /** iLink 官方协议：用户超过 24 小时未发消息，context_token 过期，无法回复 */
        const val CONTEXT_TOKEN_MAX_AGE_MS = 24L * 60 * 60 * 1000

        const val SEND_BLOCKED_NO_TOKEN =
            "context_token 缺失：iLink 协议只能回复对方先发来的消息，请让对方先发一条消息"
        const val SEND_BLOCKED_EXPIRED =
            "context_token 已过期（会话超过 24 小时）：请让对方重新发送一条消息后再试"

        /**
         * 「等待对方下一条消息」的挂起退避。
         *
         * 该状态下**不发网络请求**（发送前拦截），每次重试只是一次本地凭证查询，
         * 因此退避取 1 分钟：对方发来消息后最迟 1 分钟内自动补发，代价可忽略。
         */
        const val CONTEXT_TOKEN_WAIT_BACKOFF_MS = 60_000L

        /** 挂起行的 lastError 前缀（健康卡/日志据此区分「等待」与「失败」）。 */
        const val WAITING_CONTEXT_TOKEN = "waiting_context_token"

        /** 复活历史判死行的单次扫描上限（够覆盖一次会话的积压，且不会一次写太多）。 */
        const val TOKEN_REVIVAL_SCAN_LIMIT = 50

        /**
         * 该错误是否只是「还没有会话凭证」——可等待（不判死），凭证一到即可发送。
         *
         * 命中两条发送前拦截文案或 SDK 的 missing token 报错时成立；供 [updateFailure]
         * 与历史行复活（[WeChatOutboxCoordinator.reviveTokenBlockedDeadRows]）共用。
         */
        fun isContextTokenWaitMessage(message: String?): Boolean {
            val lower = message?.lowercase() ?: return false
            return TOKEN_WAIT_MARKERS.any { lower.contains(it) }
        }

        private const val SENT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000

        /** 凭证等待标记（统一小写匹配）。 */
        private val TOKEN_WAIT_MARKERS = listOf(
            "context_token 缺失",
            "contexttoken 缺失",
            "missing latest context token",
            "context_token 已过期",
            "contexttoken 已过期",
        )

        // 统一小写匹配（isNonRetryable 会 lowercase）。
        // 注意：context_token 缺失/过期**不在**此列——它们走「挂起等待」而非判死
        // （见 updateFailure 的 G4 修正与 TOKEN_WAIT_MARKERS）。
        private val NON_RETRYABLE_MARKERS = listOf(
            "未登录",
            "errcode=-14",
        )

        private fun maskUser(userId: String): String {
            if (userId.length <= 6) return "***"
            return userId.take(3) + "***" + userId.takeLast(2)
        }
    }
}
