package com.yunian.ai.wechat

import com.yunian.ai.domain.wechat.WeChatContentKind
import com.yunian.ai.domain.wechat.WeChatMediaRef
import com.yunian.ai.domain.wechat.WeChatOutboundRequest
import com.yunian.ai.wechat.ilink.IlinkAccount
import com.yunian.ai.wechat.ilink.IlinkSessionStore
import com.yunian.ai.wechat.outbox.WeChatOutboxCoordinator
import com.yunian.ai.wechat.outbox.WeChatOutboxRecovery
import com.yunian.ai.wechat.transport.WeChatTransportPort
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File

class WeChatOutboxCoordinatorTest {

    @get:Rule
    val temporaryFolder = TemporaryFolder()

    private val coordinator = WeChatOutboxCoordinator(
        dao = FakeOutboxDao(),
        transport = object : WeChatTransportPort {
            override suspend fun sendText(toUserId: String, text: String, contextToken: String?) =
                Result.success(Unit)

            override suspend fun sendImage(
                toUserId: String,
                imageBytes: ByteArray,
                fileName: String,
                description: String?,
                contextToken: String?,
            ) = Result.success(Unit)
        },
    )

    @Test
    fun buildEntities_simpleTextSegments() {
        val entities = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 9L, text = "你好。世界！"),
            wechatUserId = "u1",
            rootId = "r1",
            createdAtMs = 1000L,
        )
        assertEquals(2, entities.size)
        assertEquals("r1#0", entities[0].id)
        assertEquals("r1#1", entities[1].id)
        assertEquals(WeChatContentKind.TEXT.wireType, entities[0].kind)
        assertEquals(0, entities[0].segmentIndex)
        assertEquals(2, entities[0].segmentCount)
        assertEquals(9L, entities[0].companionId)
        assertEquals("PENDING", entities[0].status)
    }

    @Test
    fun buildEntities_imageSingle() {
        val entities = coordinator.buildEntities(
            request = WeChatOutboundRequest(
                companionId = 1L,
                media = WeChatMediaRef(kind = WeChatContentKind.IMAGE, localPath = "/a.png"),
            ),
            wechatUserId = "u2",
            rootId = "img",
            createdAtMs = 1L,
        )
        assertEquals(1, entities.size)
        assertEquals(WeChatContentKind.IMAGE.wireType, entities.single().kind)
        assertEquals("/a.png", entities.single().mediaLocalPath)
    }

    @Test
    fun buildEntities_blankEmpty() {
        val entities = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "  "),
            wechatUserId = "u3",
            rootId = "e",
            createdAtMs = 1L,
        )
        assertTrue(entities.isEmpty())
    }

    @Test
    fun drain_resolvesLatestContextTokenFromSessionStore() = runBlocking {
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello", contextToken = "stale-token"),
            wechatUserId = "user-1",
            rootId = "secure",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        var sentToken: String? = null
        val secureCoordinator = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sentToken = contextToken
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            sessionStore = FakeSessionStore("latest-token"),
            segmentGapMs = 0L,
        )

        assertEquals(1, secureCoordinator.drain())
        assertEquals("latest-token", sentToken)
        assertEquals("SENT", dao.lastStatus)
    }

    @Test
    fun drain_sendsEveryTextSegmentInOrder() = runBlocking {
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "第一句。第二句！第三句？"),
            wechatUserId = "user-1",
            rootId = "multi",
            createdAtMs = 1L,
        )
        val sentTexts = mutableListOf<String>()
        val multiSegmentCoordinator = WeChatOutboxCoordinator(
            dao = FakeOutboxDao(ready),
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sentTexts += text
                    return Result.success(Unit)
                }

                override suspend fun sendTextSegments(
                    toUserId: String,
                    segments: List<String>,
                    contextToken: String?,
                ): Result<Unit> {
                    error("batch transport must not be used")
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            segmentGapMs = 0L,
        )

        assertEquals(3, multiSegmentCoordinator.drain(limit = 1))
        assertEquals(listOf("第一句。", "第二句！", "第三句？"), sentTexts)
    }

    @Test
    fun cleanupManagedCache_deletesOnlyExpiredUnreferencedFiles() {
        val cacheDir = temporaryFolder.newFolder("stickers")
        val referenced = File(cacheDir, "referenced.png").apply { writeText("keep") }
        val expired = File(cacheDir, "expired.png").apply { writeText("delete") }
        val recent = File(cacheDir, "recent.png").apply { writeText("keep") }
        referenced.setLastModified(100L)
        expired.setLastModified(100L)
        recent.setLastModified(2_000L)
        val cacheCoordinator = WeChatOutboxCoordinator(
            dao = FakeOutboxDao(),
            transport = SuccessfulTransport,
            managedMediaCacheDir = cacheDir,
        )

        val deleted = cacheCoordinator.cleanupManagedCache(
            referencedPaths = setOf(referenced.absolutePath),
            cutoffMs = 1_000L,
        )

        assertEquals(1, deleted)
        assertTrue(referenced.exists())
        assertTrue(recent.exists())
        assertTrue(!expired.exists())
    }

    @Test
    fun drain_failsFastWithoutRetryWhenContextTokenMissing() = runBlocking {
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "no-token",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        val sendAttempts = mutableListOf<String?>()
        val guardedCoordinator = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sendAttempts += contextToken
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            sessionStore = FakeSessionStore(contextToken = null),
        )

        assertEquals(0, guardedCoordinator.drain())
        assertTrue(sendAttempts.isEmpty())
        // G4 修正：凭证缺失＝「等待对方下一条消息」，**不再判死**——保持 PENDING（健康卡显示
        // 「待发」而非「失败」），凭证落库后自动补发；更不得计入失败次数。
        assertEquals("PENDING", dao.lastAttemptStatus)
        assertTrue(dao.lastAttemptError!!.contains("waiting_context_token"))
        assertTrue(dao.lastAttemptError!!.contains("context_token 缺失"))
    }

    @Test
    fun drain_failsFastWithoutRetryWhenContextTokenExpired() = runBlocking {
        val now = 10_000_000_000L
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "expired-token",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        val sendAttempts = mutableListOf<String?>()
        val guardedCoordinator = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sendAttempts += contextToken
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            sessionStore = FakeSessionStore(
                contextToken = "stale-token",
                savedAtMs = now - (WeChatOutboxCoordinator.CONTEXT_TOKEN_MAX_AGE_MS + 1),
            ),
            nowMs = { now },
        )

        assertEquals(0, guardedCoordinator.drain())
        assertTrue(sendAttempts.isEmpty())
        // 同上：过期同样只是「等对方刷新凭证」，不判死
        assertEquals("PENDING", dao.lastAttemptStatus)
        assertTrue(dao.lastAttemptError!!.contains("waiting_context_token"))
        assertTrue(dao.lastAttemptError!!.contains("context_token 已过期"))
    }

    /** 存量迁移：老版本判死的「凭证缺失」行必须被复活为可发送（否则用户永远补不回来）。 */
    @Test
    fun reviveTokenBlockedDeadRows_reactivates_dead_token_rows_only() = runBlocking {
        val dead = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "dead-token",
            createdAtMs = 1L,
        ).map {
            it.copy(
                status = "FAILED",
                lastError = "outbox_dead: " + WeChatOutboxCoordinator.SEND_BLOCKED_NO_TOKEN,
                nextAttemptAtMs = WeChatOutboxRecovery.NEVER_RETRY_AT_MS,
            )
        }
        val otherDead = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hi"),
            wechatUserId = "user-1",
            rootId = "dead-session",
            createdAtMs = 1L,
        ).map {
            it.copy(
                status = "FAILED",
                lastError = "outbox_dead: 微信会话已过期（errcode=-14），请重新扫码登录",
                nextAttemptAtMs = WeChatOutboxRecovery.NEVER_RETRY_AT_MS,
            )
        }
        val dao = FakeOutboxDao(dead + otherDead)
        val c = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?) =
                    Result.success(Unit)

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            sessionStore = FakeSessionStore(contextToken = null),
        )

        assertEquals(1, c.reviveTokenBlockedDeadRows())
        val revived = dao.attempts.last { it.status == "PENDING" }
        assertEquals(0, revived.retryCount)
        assertEquals(0L, revived.nextAttemptAtMs)
        assertTrue(revived.lastError!!.contains("revived_from_dead"))
        assertTrue(revived.lastError!!.contains("context_token 缺失"))
        // 会话过期（errcode=-14）不在复活范围：重扫前重试必然失败，不该被改写
        assertTrue(
            "会话过期行不得被复活",
            dao.attempts.none { it.status == "PENDING" && it.lastError.orEmpty().contains("errcode=-14") },
        )
    }

    @Test
    fun drain_sendsWhenContextTokenFresh() = runBlocking {
        val now = 10_000_000_000L
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "fresh-token",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        val freshCoordinator = WeChatOutboxCoordinator(
            dao = dao,
            transport = SuccessfulTransport,
            sessionStore = FakeSessionStore(contextToken = "fresh-token", savedAtMs = now),
            nowMs = { now },
        )

        assertEquals(1, freshCoordinator.drain())
        assertEquals("SENT", dao.lastStatus)
    }

    @Test
    fun drain_deadLettersImmediatelyWhenSessionExpired() = runBlocking {
        val now = 10_000_000_000L
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello", contextToken = "tok"),
            wechatUserId = "user-1",
            rootId = "session-expired",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        var sendAttempts = 0
        val expiredCoordinator = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(
                    toUserId: String,
                    text: String,
                    contextToken: String?,
                ): Result<Unit> {
                    sendAttempts++
                    throw com.yunian.ai.wechat.ilink.IlinkSessionExpiredException()
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            sessionStore = FakeSessionStore(contextToken = "tok", savedAtMs = now),
            nowMs = { now },
        )

        // 首次发送即判死：不进入重试退避，errcode=-14 保留在 lastError
        assertEquals(0, expiredCoordinator.drain())
        assertEquals(1, sendAttempts)
        assertEquals("FAILED", dao.lastAttemptStatus)
        assertTrue(dao.lastAttemptError!!.contains("outbox_dead"))
        assertTrue(dao.lastAttemptError!!.contains("errcode=-14"))
    }

    @Test
    fun drain_doesNotResendFreshSendingRow() = runBlocking {
        // 另一个进程/协程正在发送：updatedAtMs 很新，不能当僵尸。
        val now = 10_000_000_000L
        val inFlight = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "inflight",
            createdAtMs = 1L,
        ).map { it.copy(status = "SENDING", updatedAtMs = now - 1_000L) }
        val dao = FakeOutboxDao(inFlight)
        var sends = 0
        val recovering = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sends++
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            nowMs = { now },
        )

        // 旧实现：SENDING 不在 listReady 里，既不会被恢复也不会被重发（静默卡死）。
        // 新实现：未超租约 → 明确不动，避免把「还在发」误判成僵尸导致重复投递。
        assertEquals(0, recovering.drain())
        assertEquals(0, sends)
        assertTrue(dao.attempts.isEmpty())
    }

    @Test
    fun drain_recoversStaleSendingRowAndSendsIt() = runBlocking {
        var now = 10_000_000_000L
        val stale = now - WeChatOutboxCoordinator.SENDING_LEASE_MS - 1
        val zombie = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "zombie",
            createdAtMs = 1L,
        ).map { it.copy(status = "SENDING", updatedAtMs = stale) }
        val dao = FakeOutboxDao(zombie)
        val sentTexts = mutableListOf<String>()
        val recovering = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sentTexts += text
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            nowMs = { now },
        )

        // 旧实现：这条永久停在 SENDING（listReady 只取 PENDING/FAILED），永远发不出去。
        // 新实现：超租约 → 重置为 PENDING（retry+1、退避 2s）。
        assertEquals(1, recovering.recoverStaleSending())
        val recovery = dao.attempts.single()
        assertEquals("PENDING", recovery.status)
        assertEquals(1, recovery.retryCount)
        assertEquals(now + 2_000L, recovery.nextAttemptAtMs)
        assertTrue(recovery.lastError!!.contains(WeChatOutboxRecovery.RECOVERED_MARKER))

        // 退避未到期 → 本次 drain 不发；时间推过退避后必须补发出去。
        assertEquals(0, recovering.drain())
        assertTrue(sentTexts.isEmpty())
        now += 2_001L
        assertEquals(1, recovering.drain())
        assertEquals(listOf("hello"), sentTexts)
        assertEquals("SENT", dao.lastStatus)
    }

    @Test
    fun drain_recoversStaleSendingOnlyOncePerProcess() = runBlocking {
        val now = 10_000_000_000L
        val zombie = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "zombie2",
            createdAtMs = 1L,
        ).map {
            it.copy(
                status = "SENDING",
                updatedAtMs = now - WeChatOutboxCoordinator.SENDING_LEASE_MS - 1,
            )
        }
        val dao = FakeOutboxDao(zombie)
        val recovering = WeChatOutboxCoordinator(
            dao = dao,
            transport = SuccessfulTransport,
            nowMs = { now },
        )

        // drain 被轮询/Worker/回复链路高频调用；恢复扫描每个进程只做一次，
        // 否则每轮 drain 都多一次全表扫描。
        recovering.drain()
        val attemptsAfterFirst = dao.attempts.size
        recovering.drain()
        recovering.drain()

        assertEquals(1, attemptsAfterFirst)
        assertEquals(1, dao.attempts.size)
    }

    @Test
    fun drain_doesNotReviveZombieWhenRetryBudgetExhausted() = runBlocking {
        val now = 10_000_000_000L
        val zombie = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "zombie3",
            createdAtMs = 1L,
        ).map {
            it.copy(
                status = "SENDING",
                retryCount = 4,
                updatedAtMs = now - WeChatOutboxCoordinator.SENDING_LEASE_MS - 1,
            )
        }
        val dao = FakeOutboxDao(zombie)
        var sends = 0
        val recovering = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    sends++
                    return Result.success(Unit)
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            nowMs = { now },
        )

        // 反复崩溃不能无限复活同一条消息：预算用尽 → 只改写 lastError，
        // 交回既有 deleteDeadBefore(retryCount >= maxRetry) 回收，绝不重发。
        assertEquals(0, recovering.recoverStaleSending())
        val dead = dao.attempts.single()
        assertEquals("FAILED", dead.status)
        assertEquals(4, dead.retryCount)
        assertEquals(WeChatOutboxRecovery.NEVER_RETRY_AT_MS, dead.nextAttemptAtMs)
        assertTrue(dead.lastError!!.contains(WeChatOutboxRecovery.DEAD_MARKER))
        // 行本身除 status/lastError 外不得被改：retryCount 必须还是 4，
        // 否则会越过既有判死回收路径（deleteDeadBefore: retryCount >= maxRetry）。
        assertEquals(4, dao.row("zombie3#0")!!.retryCount)
        assertEquals("FAILED", dao.row("zombie3#0")!!.status)
        // 判死必须同时写上「永不重试」哨兵，否则 nextAttemptAtMs 还停在入队时的 0，
        // 下一次 drain 会立刻把它捞出来重发，等于绕过判死。
        assertEquals(WeChatOutboxRecovery.NEVER_RETRY_AT_MS, dao.row("zombie3#0")!!.nextAttemptAtMs)

        // 之后 drain 只按既有重试链路处理（本用例 transport 必失败 → 本轮不投递成功），
        // 关键是 recovery 不再把它当僵尸复活：attempts 里不能再出现 outbox_recovered 前缀。
        // 用真实墙钟把时间推过哨兵值（Long.MAX_VALUE/4 ≈ 2.3e18，只有真实 now 才会超过它），
        // 否则 nextAttemptAtMs 的哨兵语义测不出来。
        val wallClockNow = System.currentTimeMillis()
        val atWallClock = WeChatOutboxCoordinator(
            dao = dao,
            transport = SuccessfulTransport,
            nowMs = { wallClockNow },
        )
        assertEquals("drain must not deliver a dead-lettered row", 0, atWallClock.drain())
        assertTrue(
            "recovery must not resurrect an exhausted row: " + dao.attempts.joinToString(" | ") {
                it.kind + ":" + it.status + ":" + it.retryCount + ":" + it.lastError
            },
            dao.attempts.none {
                it.lastError != null &&
                    it.lastError.startsWith(WeChatOutboxRecovery.RECOVERED_MARKER + ":")
            },
        )
    }

    @Test
    fun killDuringSend_leavesSendingEvidenceThatRecoveryCanFind() = runBlocking {
        var now = 10_000_000_000L
        val ready = coordinator.buildEntities(
            request = WeChatOutboundRequest(companionId = 1L, text = "hello"),
            wechatUserId = "user-1",
            rootId = "killed",
            createdAtMs = 1L,
        )
        val dao = FakeOutboxDao(ready)
        val dying = WeChatOutboxCoordinator(
            dao = dao,
            transport = object : WeChatTransportPort {
                override suspend fun sendText(toUserId: String, text: String, contextToken: String?): Result<Unit> {
                    // 模拟「发送过程中进程被杀」：发送调用直接终止，
                    // dispatchOne 之后的 updateFailure/updateStatus 永远不会执行。
                    throw IllegalStateException("simulated kill mid-send")
                }

                override suspend fun sendImage(
                    toUserId: String,
                    imageBytes: ByteArray,
                    fileName: String,
                    description: String?,
                    contextToken: String?,
                ) = Result.success(Unit)
            },
            nowMs = { now },
        )

        runCatching { dying.drain() }

        // 证据：SENDING 先落库，且 updatedAtMs 就是「开始发送」的时刻（租约的起点）。
        val writes = dao.attempts.joinToString(" | ") {
            it.kind + ":" + it.status + ":" + it.retryCount + ":" + it.lastError
        }
        val stuckIndex = dao.attempts.indexOfFirst { it.status == "SENDING" }
        assertTrue("no SENDING write found in: " + writes, stuckIndex >= 0)
        val stuck = dao.attempts[stuckIndex]
        assertEquals("killed#0", stuck.id)
        // 租约起点必须走注入时钟：旧实现用 Room 的 System.currentTimeMillis() 默认值，
        // 时间戳与恢复逻辑的 nowMs 不是同一个时钟，租约无法推导也无法测试。
        assertEquals("SENDING write must carry the lease start: " + writes, now, stuck.updatedAtMs)
        // 这个模拟里异常被 dispatchOne 捕获并落成 FAILED；真实进程被杀时不会有这一步，
        // 行会停在上面那条 SENDING 上（由下面的恢复兜住）。
        assertEquals("writes=" + writes, "FAILED", dao.row("killed#0")!!.status)

        // 进程重启后（新的 coordinator、租约已过）必须能把超租约的 SENDING 捡回来。
        now += WeChatOutboxCoordinator.SENDING_LEASE_MS + 1
        val restarted = WeChatOutboxCoordinator(
            dao = dao,
            transport = SuccessfulTransport,
            nowMs = { now },
        )
        // 把行重新置成「进程被杀」后的真实形态：停在 SENDING、租约已过。
        dao.forceRow("killed#0") {
            it.copy(status = "SENDING", updatedAtMs = now - WeChatOutboxCoordinator.SENDING_LEASE_MS - 1)
        }
        assertEquals(1, restarted.recoverStaleSending())
        assertEquals("PENDING", dao.row("killed#0")!!.status)
        assertTrue(
            dao.attempts.last().lastError!!.contains(WeChatOutboxRecovery.RECOVERED_MARKER),
        )
    }
}

private object SuccessfulTransport : WeChatTransportPort {
    override suspend fun sendText(toUserId: String, text: String, contextToken: String?) =
        Result.success(Unit)

    override suspend fun sendImage(
        toUserId: String,
        imageBytes: ByteArray,
        fileName: String,
        description: String?,
        contextToken: String?,
    ) = Result.success(Unit)
}

private class FakeOutboxDao(
    private val ready: List<com.yunian.ai.database.model.WeChatOutboxEntity> = emptyList(),
) : com.yunian.ai.database.dao.WeChatOutboxDao {
    var lastStatus: String? = null
    var lastAttemptStatus: String? = null
    var lastAttemptError: String? = null

    /** 所有写过的状态/尝试，按发生顺序；用于验证「先写 SENDING 再发送」的时序。 */
    data class Write(
        val kind: String,
        val id: String,
        val status: String,
        val retryCount: Int,
        val nextAttemptAtMs: Long,
        val lastError: String?,
        val updatedAtMs: Long,
    )

    val attempts = mutableListOf<Write>()

    override suspend fun insertAll(items: List<com.yunian.ai.database.model.WeChatOutboxEntity>) = Unit
    override suspend fun insert(item: com.yunian.ai.database.model.WeChatOutboxEntity) = Unit
    /**
     * 可变行存储：仿 Room 的真实行为（写入会改状态），否则「恢复后不再重复恢复」
     * 这类时序断言在假 DAO 上恒为真，等于没测。
     */
    private val store = ready.toMutableList()

    override suspend fun listReady(nowMs: Long, limit: Int) = store
        .filter { (it.status == "PENDING" || it.status == "FAILED") && it.nextAttemptAtMs <= nowMs }
        .take(limit)

    override suspend fun listOpenByRootId(rootId: String) = store
        .filter { it.rootId == rootId && (it.status == "PENDING" || it.status == "FAILED") }
        .sortedBy { it.segmentIndex }

    override suspend fun listStaleSending(
        staleBeforeMs: Long,
        limit: Int,
    ): List<com.yunian.ai.database.model.WeChatOutboxEntity> = store
        .filter { it.status == "SENDING" && it.updatedAtMs <= staleBeforeMs }
        .take(limit)

    override suspend fun updateAttempt(
        id: String,
        status: String,
        retryCount: Int,
        nextAttemptAtMs: Long,
        lastError: String?,
        updatedAtMs: Long,
    ) {
        lastAttemptStatus = status
        lastAttemptError = lastError
        attempts += Write("attempt", id, status, retryCount, nextAttemptAtMs, lastError, updatedAtMs)
        replace(id) {
            it.copy(
                status = status,
                retryCount = retryCount,
                nextAttemptAtMs = nextAttemptAtMs,
                lastError = lastError,
                updatedAtMs = updatedAtMs,
            )
        }
    }

    override suspend fun updateStatus(id: String, status: String, lastError: String?, updatedAtMs: Long) {
        lastStatus = status
        attempts += Write("status", id, status, -1, 0L, lastError, updatedAtMs)
        replace(id) { it.copy(status = status, lastError = lastError, updatedAtMs = updatedAtMs) }
    }

    /** 读取某行的当前状态（断言「行本身没被改」用，避免依赖写入记录的哨兵值）。 */
    fun row(id: String) = store.firstOrNull { it.id == id }

    /** 直接改写行状态，用于把「进程被杀」的真实形态摆出来。 */
    fun forceRow(
        id: String,
        transform: (com.yunian.ai.database.model.WeChatOutboxEntity) -> com.yunian.ai.database.model.WeChatOutboxEntity,
    ) = replace(id, transform)

    private fun replace(
        id: String,
        transform: (com.yunian.ai.database.model.WeChatOutboxEntity) -> com.yunian.ai.database.model.WeChatOutboxEntity,
    ) {
        val index = store.indexOfFirst { it.id == id }
        if (index >= 0) store[index] = transform(store[index])
    }

    override suspend fun countOpen(): Int = 0
    override suspend fun countByStatus(status: String): Int = 0
    override suspend fun listRecentFailed(limit: Int) = store
        .filter { it.status == "FAILED" }
        .sortedByDescending { it.updatedAtMs }
        .take(limit)
    override suspend fun deleteSentBefore(cutoffMs: Long): Int = 0
    override suspend fun deleteDeadBefore(maxRetry: Int, cutoffMs: Long): Int = 0
    override suspend fun listMediaLocalPaths(): List<String> = emptyList()
}

private class FakeSessionStore(
    private val contextToken: String?,
    private val savedAtMs: Long? = null,
) : IlinkSessionStore {
    override suspend fun getSessionAccount() = IlinkAccount("bot", "bot-id", "user-id", accountId = "account-1")
    override suspend fun saveSessionAccount(account: IlinkAccount) = Unit
    override suspend fun clearSessionAccount() = Unit
    override suspend fun getCursor() = ""
    override suspend fun saveCursor(cursor: String) = Unit
    override suspend fun getContextToken(accountId: String, userId: String) = contextToken
    override suspend fun getContextTokenSavedAt(accountId: String, userId: String) = savedAtMs
    override suspend fun saveContextToken(accountId: String, userId: String, token: String) = Unit
    override suspend fun getContextTokens(accountId: String) = emptyMap<String, String>()
}
