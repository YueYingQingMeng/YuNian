package com.yunian.ai.feature.groupchat

import android.app.Application
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.CreationExtras
import com.yunian.ai.database.AppDatabase
import com.yunian.ai.domain.wechat.WeChatProactiveSync
import com.yunian.ai.common.ChatConstants
import com.yunian.ai.common.MessageBodyState
import com.yunian.ai.common.ScriptTurnStripper
import com.yunian.ai.database.model.ChatGroup
import com.yunian.ai.database.model.CompanionEntity
import com.yunian.ai.database.model.GroupMessage
import com.yunian.ai.database.model.Message
import com.yunian.ai.common.StickerManager
import com.yunian.ai.common.StickerInfo
import com.yunian.ai.common.text.BubbleTextSplitter
import com.yunian.ai.common.text.DedupGuard
import com.yunian.ai.database.repository.ChatGroupRepository
import com.yunian.ai.database.repository.CompanionRepository
import com.yunian.ai.database.repository.GroupMessageRepository
import com.yunian.ai.database.repository.MessageWriteCoordinator
import com.yunian.ai.feature.groupchat.R
import com.yunian.ai.feature.groupchat.mention.MentionEnhancer
import com.yunian.ai.feature.groupchat.mention.MentionMessageSnapshot
import com.yunian.ai.feature.groupchat.mention.MentionNormalizer
import com.yunian.ai.feature.groupchat.mention.MentionParser
import com.yunian.ai.domain.AiChatMessage
import com.yunian.ai.domain.MemoryProvider
import com.yunian.ai.domain.ServiceRegistry
import com.yunian.ai.domain.ToolRegistry
import com.yunian.ai.database.repository.ApiConfigRepository
import com.yunian.ai.database.repository.UserRepository
import com.yunian.ai.database.model.ApiProvider
import com.yunian.ai.common.DeviceIdProvider
import com.yunian.ai.common.RemoteKeyProvider
import com.yunian.ai.common.SecureLog
import com.yunian.ai.agent.AgentConfirmGuard
import com.yunian.ai.agent.AgentFacade
import com.yunian.ai.agent.AgentTurnReplyText
import com.yunian.ai.agent.host.AgentToolHost
import com.yunian.ai.agent.uniffi.AgentTurnRequest
import com.yunian.ai.agent.uniffi.PromptOrchestratorOptions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random
import com.yunian.ai.common.concurrent.DuplicateSendGuard

class GroupChatViewModel(
    application: Application,
    private val groupId: Long
) : AndroidViewModel(application) {

    private val applicationScope = com.yunian.ai.common.ApplicationScopeProvider.scope
    private var sendMessageJob: Job? = null

    private val database = AppDatabase.getDatabase(application)
    private val groupMessageRepository = ServiceRegistry.getOrThrow(GroupMessageRepository::class.java)
    private val messageWriter = ServiceRegistry.getOrThrow(MessageWriteCoordinator::class.java)
    private val chatGroupRepository = ChatGroupRepository(database.chatGroupDao())
    private val companionRepository = CompanionRepository(database.companionDao())

    private val userRepository = ServiceRegistry.getOrThrow(UserRepository::class.java)
    private val apiConfigRepository = ServiceRegistry.getOrThrow(ApiConfigRepository::class.java)

    /**
     * 群聊单次 Agent 回合的 **API 调用轮数**上限（Rust 多轮 emit_bubble 循环上界）。
     *
     * 与单聊同源：此前群聊用 16、单聊误用 `BubbleLoopRunner.MAX_BUBBLES`(=64 条气泡护栏)，
     * 都是把「气泡条数」当成「轮数」——连续多轮纯气泡会让内容逐轮漂移
     * （Issue #2 症状 2）。现统一取 [ChatConstants.AGENT_MAX_TOOL_ROUNDS]。
     */
    private val maxToolRounds: UInt = ChatConstants.AGENT_MAX_TOOL_ROUNDS.toUInt()

    /** 确认门守卫（Commerce 工具在群聊回合上 fail-closed 自动拒绝 + 有限次重跑，与通道侧同一实现）。 */
    private val agentConfirmGuard = AgentConfirmGuard()

    private val memoryProvider: MemoryProvider by lazy {
        ServiceRegistry.getOrThrow(MemoryProvider::class.java).also { it.initialize() }
    }

    private fun GroupMessage.toAiChatMessage() = com.yunian.ai.domain.AiChatMessage(
        isFromUser = companionId == -1L, content = content, timestamp = timestamp,
        companionId = companionId
    )

    /** 角色 ID → 名称 映射 → JSON（AgentTurnRequest.companionNameMapJson）。 */
    private fun serializeNameMap(map: Map<Long, String>): String {
        val obj = org.json.JSONObject()
        for ((id, name) in map) obj.put(id.toString(), name)
        return obj.toString()
    }

    // ── 上帝类拆分第二轮：分页/正文状态机外提 ────────────────────────────────
    // 闭包分析（全量读码，非 grep 抽样）：该簇只读写自己的 5 个状态流 +
    // groupMessageRepository + groupId + 协程作用域，与用户资料/群数据/成员表/
    // 发送循环/记忆/AI/保活零交叉，故整体外提至 GroupChatPager，行为逐行等价。
    private val pager = GroupChatPager(
        scope = viewModelScope,
        groupId = groupId,
        repository = groupMessageRepository
    )
    val messages: StateFlow<List<GroupMessage>> get() = pager.messages
    val messageMetadata: StateFlow<List<Message>> get() = pager.messageMetadata
    val messageBodies: StateFlow<Map<Long, MessageBodyState<GroupMessage>>> get() = pager.messageBodies
    val hasMore: StateFlow<Boolean> get() = pager.hasMore
    val isLoadingMore: StateFlow<Boolean> get() = pager.isLoadingMore

    fun loadVisibleMessageBodies(messageIds: Set<Long>) = pager.loadVisibleMessageBodies(messageIds)

    fun retryMessageBody(messageId: Long) = pager.retryMessageBody(messageId)

    fun loadMoreMessages() = pager.loadMoreMessages()

    private val _userName = MutableStateFlow("我")
    val userName: StateFlow<String> = _userName.asStateFlow()
    private val _userAvatar = MutableStateFlow<String?>(null)
    val userAvatar: StateFlow<String?> = _userAvatar.asStateFlow()

    init {
        loadUserProfile()
        pager.start()
    }

    private var avatarUnsubscribe: (() -> Unit)? = null
    private var nicknameUnsubscribe: (() -> Unit)? = null

    private fun loadUserProfile() {
        val provider = com.yunian.ai.domain.ServiceRegistry.get(com.yunian.ai.domain.UserProfileProvider::class.java)
        _userName.value = provider?.getNickname() ?: "我"
        _userAvatar.value = provider?.getAvatar()
        avatarUnsubscribe?.invoke()
        nicknameUnsubscribe?.invoke()
        avatarUnsubscribe = provider?.observeAvatar { _userAvatar.value = it }
        nicknameUnsubscribe = provider?.observeNickname { _userName.value = it }
    }

    private val _groupData = MutableStateFlow<ChatGroup?>(null)
    val groupData: StateFlow<ChatGroup?> = _groupData.asStateFlow()

    private val _allCompanions = MutableStateFlow<List<CompanionEntity>>(emptyList())
    val allCompanions: StateFlow<List<CompanionEntity>> = _allCompanions.asStateFlow()

    private val _isLoading = MutableStateFlow(false)
    private val isLoadingLock = java.util.concurrent.atomic.AtomicBoolean(false)

    private val _isRegenerating = MutableStateFlow(false)
    val isRegenerating: StateFlow<Boolean> = _isRegenerating.asStateFlow()
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()

    init {
        loadGroupData()
        loadAllCompanions()
    }

    private fun loadGroupData() {
        viewModelScope.launch {
            try {
                _groupData.value = chatGroupRepository.getGroupById(groupId)
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "loadGroupData failed", e)
            }
        }
    }

    private fun loadAllCompanions() {
        viewModelScope.launch {
            try {
                _allCompanions.value = companionRepository.getAllCompanions().first()
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "loadAllCompanions failed", e)
            }
        }
    }

    // 防连击：同内容 2 秒窗口内重复提交静默忽略（第一条已发出，双击/回车连按误触）
    private val duplicateSendGuard = DuplicateSendGuard()

    fun sendMessage(content: String) {
        if (duplicateSendGuard.shouldReject(content)) return
        if (com.yunian.ai.common.BanManager.isBanned(getApplication())) return

        // P1 收尾：输入安全校验是 CPU 较重的同步逻辑（约 100+ 正则 + 语义预处理），
        // 原先在 Compose 回调（主线程）同步执行会造成输入/首帧抖动。现整体移入 applicationScope(IO)，
        // 并严格保持原顺序语义：校验输入 → 通过后再申请门控 → 再取消旧 job → 启动新一轮。
        val previousJob = sendMessageJob
        val job = applicationScope.launch {
            // 输入安全校验在 try 之外，且 applicationScope 无 CoroutineExceptionHandler：
            // 一旦这里抛出就会沿线程默认未捕获处理器直接闪退，故显式兜底并中止本轮。
            val inputCheck = runCatching { com.yunian.ai.common.ContentFilter.checkInput(content) }
                .getOrElse { e ->
                    Log.e("GroupChatViewModel", "checkInput failed", e)
                    return@launch
                }
            if (inputCheck.isViolating) {
                android.util.Log.w("GroupChatViewModel", "Input blocked by safety filter: ${inputCheck.reason}")
                com.yunian.ai.common.BanManager.recordViolation(getApplication(), inputCheck.level)
                return@launch
            }

            // 门控在协程内、校验之后申请：原子 CAS 保证快速连点发送时只有一个能通过并启动生成。
            if (!isLoadingLock.compareAndSet(false, true)) {
                Log.w("GroupChatViewModel", "sendMessage ignored: already processing")
                return@launch
            }
            previousJob?.cancel()
            try {
                _isLoading.value = true

                val group = _groupData.value
                    ?: throw IllegalStateException("Group data is null")
                val activeCompanionIds = group.getCompanionIdList()
                val activeCompanions = _allCompanions.value.filter { activeCompanionIds.contains(it.id) }

                val normalizedContent = MentionNormalizer.normalizeImplicitMentions(content, activeCompanions)

                val userMessage = GroupMessage(
                    groupId = groupId,
                    companionId = -1L,
                    content = normalizedContent,
                    timestamp = System.currentTimeMillis()
                )
                messageWriter.enqueueGroup(userMessage)

                val userMentionedIds = MentionParser.extractMentionedCharacterIds(normalizedContent, activeCompanions)
                Log.d("GroupChatMention", "用户提及角色IDs: $userMentionedIds, 原文: $content → 标准化: $normalizedContent")

                runMultiRoundDispatch(activeCompanions, userMentionedIds)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "sendMessage failed", e)
            } finally {
                _isLoading.value = false
                isLoadingLock.set(false)
            }
        }
        sendMessageJob = job
    }

    /**
     * 顺序释放器：保证成员按优先级顺序依次落库。
     *
     * 并行生成完成顺序取决于模型响应速度，直接"完成即落库"会导致消息乱序出现
     * （优先级低但响应快的角色先冒泡）。生成仍然完全并行，只是提交段排队：
     * 成员 i 等到 turn==i 才写消息，写完（或早退/失败）后放行 i+1。
     * advance 用 CAS 校验期望轮次，早退路径的兜底推进不会越位。
     */
    private class OrderedGate {
        private val turn = java.util.concurrent.atomic.AtomicInteger(0)

        suspend fun await(myTurn: Int) {
            // 等到"至少轮到自己"：成员若已在内部 advance 过（turn > myTurn），此处立即放行不死等；
            // delay 可被取消，无需额外 ensureActive
            while (turn.get() < myTurn) {
                delay(60)
            }
        }

        fun advance(expectedTurn: Int) {
            turn.compareAndSet(expectedTurn, expectedTurn + 1)
        }
    }

    private suspend fun runMultiRoundDispatch(
        activeCompanions: List<CompanionEntity>,
        userMentionedIds: Set<Long>
    ) {
        val maxRounds = if (activeCompanions.size == 1) 1 else ChatConstants.GROUP_CHAT_AUTO_ROUNDS
        for (round in 1..maxRounds) {
            // 本轮已说内容集合：仅在同一轮次内做跨角色查重。
            // 不能跨轮共享——中文短句在近似匹配下极易误判，历史轮次的误命中
            // 会让后续轮次全员被跳过，群聊彻底沉默。
            val roundContents = ConcurrentHashMap<String, Boolean>()
            Log.d("GroupChatM", "=== 第 $round / ${ChatConstants.GROUP_CHAT_AUTO_ROUNDS} 轮开始 ===")

            val roundMembers = if (round == 1 && userMentionedIds.isNotEmpty()) {
                activeCompanions.filter { userMentionedIds.contains(it.id) }.also {
                    Log.d("GroupChatM", "第1轮过滤: 仅被@的角色发言 → ${it.map { c -> c.name }}")
                }
            } else {
                activeCompanions.also {
                    if (round > 1) Log.d("GroupChatM", "第${round}轮: 全员可发言")
                }
            }

            if (roundMembers.isEmpty()) {
                Log.d("GroupChatM", "第${round}轮无成员，跳过")
                continue
            }

            val baseHistorySnapshot = getRecentHistorySnapshot()
            val repliedIds = ConcurrentHashMap<Long, Boolean>()

            val prioritizedMembers = if (round == 1) {
                roundMembers.map { companion ->
                    companion to calculateSpeakingPriority(companion, baseHistorySnapshot, userMentionedIds)
                }.sortedByDescending { it.second }
                    .map { it.first }
            } else {
                roundMembers.shuffled()
            }

            Log.d("GroupChatM", "第${round}轮发言顺序: ${prioritizedMembers.map { it.name }}")

            withContext(Dispatchers.IO) {
                val releaseGate = OrderedGate()
                // 本轮是否有成员成功落库 + 被跨角色查重跳过的成员（用于静默轮兜底）
                val committedAny = java.util.concurrent.atomic.AtomicBoolean(false)
                val skippedByDedup = java.util.concurrent.CopyOnWriteArrayList<CompanionEntity>()
                prioritizedMembers.mapIndexed { index, companion ->
                    async {
                        try {
                            val baseDelay = when (index) {
                                0 -> Random.nextLong(100L, 400L)
                                1 -> Random.nextLong(300L, 700L)
                                2 -> Random.nextLong(500L, 900L)
                                else -> Random.nextLong(700L, 1200L)
                            }
                            delay(baseDelay)
                            processCompanionReply(
                                companion = companion,
                                activeCompanions = activeCompanions,
                                baseHistorySnapshot = baseHistorySnapshot,
                                repliedIds = repliedIds,
                                round = round,
                                roundContents = roundContents,
                                releaseGate = releaseGate,
                                myTurn = index,
                                committedAny = committedAny,
                                skippedByDedup = skippedByDedup
                            )
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            Log.e("GroupChatM", "[${companion.name}] 第${round}轮失败", e)
                        } finally {
                            // 无论成功/早退/失败，轮到自己就放行下一位，避免顺序释放死等
                            releaseGate.await(index)
                            releaseGate.advance(index)
                        }
                    }
                }.awaitAll()

                // 静默轮兜底：本轮全员被跨角色查重跳过时，放行第一个被跳过的成员再试一次，
                // 避免查重误判导致群聊整轮沉默、无任何提示。
                if (!committedAny.get() && skippedByDedup.isNotEmpty()) {
                    val fallbackCompanion = skippedByDedup.first()
                    Log.d("GroupChatM", "本轮全员查重跳过，兜底放行 ${fallbackCompanion.name}")
                    runCatching {
                        processCompanionReply(
                            companion = fallbackCompanion,
                            activeCompanions = activeCompanions,
                            baseHistorySnapshot = baseHistorySnapshot,
                            repliedIds = repliedIds,
                            round = round,
                            roundContents = roundContents,
                            // awaitAll 后串行单跑，无需再排队
                            releaseGate = null,
                            skipDedupCheck = true
                        )
                    }.onFailure { e ->
                        Log.e("GroupChatM", "[${fallbackCompanion.name}] 第${round}轮兜底放行失败", e)
                    }
                }
            }

            if (round < ChatConstants.GROUP_CHAT_AUTO_ROUNDS && activeCompanions.size >= 2) {
                delay(Random.nextLong(800, 2000))
            } else if (round >= ChatConstants.GROUP_CHAT_AUTO_ROUNDS && activeCompanions.size == 1) {
                break
            }
        }
    }

    private suspend fun getRecentHistorySnapshot(): List<GroupMessage> {
        return withContext(Dispatchers.IO) {
            groupMessageRepository.getMessagesForGroup(groupId).first().takeLast(ChatConstants.GROUP_CHAT_CONTEXT_WINDOW)
        }
    }

    private suspend fun buildRecentContextSnapshots(history: List<GroupMessage>): List<MentionMessageSnapshot> {
        val companionMap = _allCompanions.value.associateBy { it.id }
        val currentUserName = userRepository.userName.first()
        return history.takeLast(6).map { msg ->
            MentionMessageSnapshot(
                content = msg.content,
                isUser = msg.companionId == -1L,
                speakerName = if (msg.companionId == -1L) currentUserName
                else companionMap[msg.companionId]?.name ?: "未知"
            )
        }
    }

    private suspend fun processCompanionReply(
        companion: CompanionEntity,
        activeCompanions: List<CompanionEntity>,
        baseHistorySnapshot: List<GroupMessage>,
        repliedIds: ConcurrentHashMap<Long, Boolean>,
        round: Int,
        roundContents: ConcurrentHashMap<String, Boolean> = ConcurrentHashMap(),
        releaseGate: OrderedGate? = null,
        myTurn: Int = 0,
        committedAny: java.util.concurrent.atomic.AtomicBoolean? = null,
        skippedByDedup: MutableList<CompanionEntity>? = null,
        skipDedupCheck: Boolean = false
    ) {
        // 生成阶段（并行）：合并本轮先手已落库的新消息，后手能"接力"回应先手
        val mergedSnapshot = mergeWithFreshMessages(baseHistorySnapshot)
        val isolatedHistory = buildIsolatedHistorySnapshot(
            baseHistorySnapshot = mergedSnapshot,
            currentCompanionId = companion.id
        )

        val bubbles = generateIsolatedAiReplyBubbles(
            companion = companion,
            allActiveCompanions = activeCompanions,
            historySnapshot = isolatedHistory,
            excludeCompanionIds = emptySet()
        )
        val firstContent = bubbles.firstOrNull()?.trim() ?: return
        val aiContent = firstContent.replace(Regex("\n{2,}"), "\n")

        // 自查重：不要复读自己刚说过的话。
        // 口径与单聊/跨角色统一为 DedupGuard.isDuplicate（P1-5）：精确相等或 ≥10 字双向包含，
        // 旧的「>5 字子串包含」会把「哈哈哈哈」类短句误杀。
        val recentRepliesFromThisChar = isolatedHistory
            .filter { it.companionId == companion.id }
            .takeLast(3)
            .map { normalizeForDedup(it.content) }
        val normalizedNew = normalizeForDedup(aiContent)
        val isSelfDuplicate = DedupGuard.isDuplicate(normalizedNew, recentRepliesFromThisChar)
        if (isSelfDuplicate) {
            // 静默轮兜底（skipDedupCheck=true）落库前同样过这道自查重：命中即放弃本轮（P1-3）。
            if (skipDedupCheck) {
                Log.w("GroupChatM", "[${companion.name}] 兜底放行命中自查重，放弃本轮")
            } else {
                Log.d("GroupChatM", "[${companion.name}] 跳过重复回复")
            }
            return
        }

        // 跨角色查重：别的角色刚说过一样的话就不抢话
        // （skipDedupCheck = true 时跳过查重与登记，供静默轮兜底放行使用）
        if (!skipDedupCheck) {
            if (isCrossDuplicate(normalizedNew, roundContents)) {
                Log.d("GroupChatM", "[${companion.name}] 跳过跨角色重复回复")
                skippedByDedup?.add(companion)
                return
            }
        }

        val enhancedContent = MentionEnhancer.enhanceMentionsForAssistantReply(
            content = aiContent,
            speakerName = companion.name,
            members = activeCompanions,
            aiService = null,
            judgeEnabled = ChatConstants.GROUP_CHAT_MENTION_JUDGE_ENABLED,
            judgeThreshold = ChatConstants.GROUP_CHAT_MENTION_JUDGE_THRESHOLD,
            recentContext = buildRecentContextSnapshots(isolatedHistory)
        )
        repliedIds[companion.id] = true
        // 生成完成即登记（早于提交）：并行的后手生成时就能看到，避免撞车
        // （skipDedupCheck 时跳过登记，兜底放行的回复不算作"抢话"内容）
        if (!skipDedupCheck) {
            roundContents[normalizedNew] = true
        }

        val outputCheck = com.yunian.ai.common.ContentFilter.checkOutputSafety(enhancedContent)
        val safeContent = if (!outputCheck.isSafe) {
            android.util.Log.w("GroupChatViewModel", "AI output blocked by safety filter: ${outputCheck.reason}")
            com.yunian.ai.common.BanManager.recordViolation(getApplication(), outputCheck.level)
            "抱歉，我无法回应这个话题。"
        } else {
            enhancedContent
        }

        // 提交阶段（按优先级顺序串行落库）：等轮到自己再冒泡，消息时序与发言优先级一致
        releaseGate?.await(myTurn)
        try {
            sendSplitAiMessages(safeContent, groupId, companion.id)
            // 首条消息成功落库，标记本轮已有成员成功发言（静默轮兜底的判断依据）
            committedAny?.set(true)

            bubbles.drop(1).forEach { bubble ->
                val content = bubble.trim()
                if (content.isBlank()) return@forEach
                delay(Random.nextLong(600L, 1600L))
                val bubbleCheck = com.yunian.ai.common.ContentFilter.checkOutputSafety(content)
                val safeBubble = if (!bubbleCheck.isSafe) {
                    android.util.Log.w("GroupChatViewModel", "AI bubble blocked by safety filter: ${bubbleCheck.reason}")
                    "抱歉，我无法回应这个话题。"
                } else {
                    content
                }
                sendSplitAiMessages(safeBubble, groupId, companion.id)
            }
        } finally {
            // 消息写完立即放行下一位，记忆提取不阻塞别人冒泡
            releaseGate?.advance(myTurn)
        }

        val lastUserMsg = mergedSnapshot.lastOrNull { it.companionId == -1L }?.content ?: ""
        val allContent = (listOf(safeContent) + bubbles.drop(1)).joinToString("\n")
        if (lastUserMsg.isNotBlank() && allContent.isNotBlank()) {
            memoryProvider.extractAndSaveFromConversation(
                userInput = lastUserMsg,
                aiResponse = allContent,
                companionId = companion.id,
                groupId = groupId
            )
        }

        Log.d("GroupChatM", "[${companion.name}] 第${round}轮回复完成 (${allContent.length}字, ${bubbles.size}气泡)")
    }

    /**
     * 生成前把本轮先手已落库的新消息合并进快照。
     *
     * 并行生成时各成员的完成顺序不定：先手写入的新气泡（消息 + 表情包）会先落库，
     * 后手生成前重读一次，就能"听到"先手的话再回应，而不是平行世界各说各的。
     * DB 读取失败时退回基础快照，不影响可用性。
     */
    private suspend fun mergeWithFreshMessages(base: List<GroupMessage>): List<GroupMessage> {
        val fresh = withContext(Dispatchers.IO) {
            runCatching {
                groupMessageRepository.getMessagesForGroup(groupId).first()
                    .takeLast(ChatConstants.GROUP_CHAT_CONTEXT_WINDOW)
            }.getOrDefault(emptyList())
        }
        if (fresh.isEmpty()) return base
        return (base + fresh)
            .distinctBy { it.id }
            .sortedWith(compareBy({ it.timestamp }, { it.id }))
            .takeLast(ChatConstants.GROUP_CHAT_CONTEXT_WINDOW)
    }

    /** 查重规范化：委托 [DedupGuard.normalize]（剥 @、空白、标点、引号括号后小写，取前 40 字），与单聊统一口径。 */
    private fun normalizeForDedup(text: String): String = DedupGuard.normalize(text)

    /**
     * 与本轮其他角色的回复比对，近似重复则不发言。
     * 规则：精确相等，或「较短串长度 >= 10 且较长串包含较短串」——
     * 阈值收紧到 10，避免「哈哈哈哈哈哈」类中文短句被误判为重复而全员禁言。
     */
    private fun isCrossDuplicate(normalized: String, roundContents: ConcurrentHashMap<String, Boolean>): Boolean {
        if (normalized.isEmpty() || roundContents.isEmpty()) return false
        return roundContents.keys.any { existing ->
            existing == normalized ||
                (existing.length >= 10 && existing.length <= normalized.length && normalized.contains(existing)) ||
                (normalized.length >= 10 && normalized.length <= existing.length && existing.contains(normalized))
        }
    }

    /**
     * 构建当前角色的隔离历史：自己的消息与用户消息原样保留，
     * 其他角色的消息统一加「[其他群友] 」前缀（匿名化，防止人设串台的同时保留群聊互动感）。
     *
     * 注意：此前这里只保留"已回复过"的其他角色消息，且 generateIsolatedAiReplyBubbles
     * 会再过滤一次，双重过滤导致成员几乎看不到彼此，群聊变得各说各话；
     * 现在统一保留（匿名前缀 + 身份锁定提示词共同兜底）。
     */
    private fun buildIsolatedHistorySnapshot(
        baseHistorySnapshot: List<GroupMessage>,
        currentCompanionId: Long
    ): List<GroupMessage> {
        val enhanced = baseHistorySnapshot.map { msg ->
            if (msg.companionId != -1L && msg.companionId != currentCompanionId) {
                msg.copy(content = "[其他群友] ${msg.content}")
            } else {
                msg
            }
        }

        return enhanced.takeLast(ChatConstants.GROUP_CHAT_CONTEXT_WINDOW)
    }

    private fun calculateSpeakingPriority(
        companion: CompanionEntity,
        historySnapshot: List<GroupMessage>,
        userMentionedIds: Set<Long>
    ): Double {
        var priority = 0.0

        if (userMentionedIds.contains(companion.id)) {
            priority += 100.0
        }

        val mentionedInHistory = historySnapshot
            .takeLast(10)
            .count { it.content.contains("@${companion.name}") && it.companionId != companion.id }
        priority += mentionedInHistory * 30.0

        val lastReplyFromThisChar = historySnapshot
            .filter { it.companionId == companion.id }
            .lastOrNull()
        if (lastReplyFromThisChar == null) {
            priority += 20.0
        } else {
            val timeSinceLastReply = System.currentTimeMillis() - lastReplyFromThisChar.timestamp
            val minutesSince = timeSinceLastReply / (1000 * 60)
            if (minutesSince > 5) {
                priority += 15.0
            }
        }

        val recentRepliesCount = historySnapshot
            .takeLast(8)
            .count { it.companionId == companion.id }
        if (recentRepliesCount == 0) {
            priority += 10.0
        } else if (recentRepliesCount >= 3) {
            priority -= 15.0
        }

        val lastUserMsg = historySnapshot.lastOrNull { it.companionId == -1L }
        if (lastUserMsg != null) {
            val userMsgContent = lastUserMsg.content.lowercase()
            val keywords = listOf(
                companion.name.lowercase(),
                *(companion.personality?.take(50)?.lowercase()?.split(" ", "，", "。", "、")?.toTypedArray() ?: emptyArray<String>())
            )
            val relevanceScore = keywords.count { keyword ->
                keyword.isNotBlank() && userMsgContent.contains(keyword)
            }
            priority += relevanceScore * 5.0
        }

        priority += Random.nextDouble(0.0, 10.0)

        return priority
    }

    private fun extractPersonalityTraits(companion: CompanionEntity): String {
        val traits = mutableListOf<String>()

        val personality = companion.personality.lowercase()
        when {
            personality.contains("活泼") || personality.contains("开朗") || personality.contains("外向") -> {
                traits.add("你很活跃，喜欢主动说话，话比较多")
                traits.add("经常发表情包和语气词")
            }
            personality.contains("内向") || personality.contains("安静") || personality.contains("文静") -> {
                traits.add("你比较安静，不太爱主动发言")
                traits.add("说话简短，但每句都有意义")
                traits.add("只在真正感兴趣的话题上才会多说")
            }
            personality.contains("傲娇") || personality.contains("嘴硬") -> {
                traits.add("你嘴硬心软，表面不在乎其实很在意")
                traits.add("喜欢说反话，用「哼」、「才不是」之类的词")
            }
            personality.contains("温柔") || personality.contains("体贴") -> {
                traits.add("你说话很温柔，经常关心别人")
                traits.add("用词委婉，带「呀」、「呢」、「啦」等语气词")
            }
            personality.contains("毒舌") || personality.contains("犀利") -> {
                traits.add("你说话直接，偶尔会吐槽")
                traits.add("但吐槽都是善意的，其实是关系好的表现")
            }
        }

        companion.speakingStyle?.let { style ->
            when {
                style.contains("可爱") -> traits.add("你的语气很萌，喜欢用叠词")
                style.contains("成熟") -> traits.add("你说话比较稳重，不会太幼稚")
                style.contains("搞笑") -> traits.add("你幽默风趣，喜欢开玩笑")
                style.contains("正经") -> traits.add("你做事认真，说话也比较严肃")
            }
        }

        return if (traits.isNotEmpty()) traits.joinToString("\n") else ""
    }

    private fun detectGroupAtmosphere(
        historySnapshot: List<GroupMessage>,
        userName: String
    ): String {
        if (historySnapshot.isEmpty()) return "刚开始聊天，气氛还比较生疏"

        val recentMessages = historySnapshot.takeLast(10)
        val totalMessages = recentMessages.size

        val laughCount = recentMessages.count { msg ->
            msg.content.contains(Regex("[哈h][哈h]+|哈哈哈|hhhh|笑死|笑死我了"))
        }
        val emojiCount = recentMessages.count { msg ->
            msg.content.contains(Regex("[😂🤣😄😆🥰😘💕❤️👍🎉]"))
        }
        val questionCount = recentMessages.count { msg ->
            msg.content.contains(Regex("[？?]"))
        }

        val hasHeatedDiscussion = recentMessages.any { msg ->
            msg.content.length > 50 && (msg.content.contains("！！") || msg.content.contains("!!"))
        }

        return when {
            laughCount >= totalMessages * 0.5 -> "大家都在哈哈大笑，气氛很欢乐"
            emojiCount >= totalMessages * 0.4 -> "大家都在发表情包，气氛轻松愉快"
            questionCount >= totalMessages * 0.3 -> "大家在讨论问题，气氛比较认真"
            hasHeatedDiscussion -> "讨论得很激烈，有人很激动"
            totalMessages <= 3 -> "刚开始聊，还在热身阶段"
            else -> "正常聊天氛围，大家聊得挺开心"
        }
    }

    private fun buildEmotionalContext(
        historySnapshot: List<GroupMessage>,
        companion: CompanionEntity
    ): String {
        val contexts = mutableListOf<String>()

        val mentionedMe = historySnapshot
            .takeLast(6)
            .filter { it.content.contains("@${companion.name}") && it.companionId != companion.id }

        if (mentionedMe.isNotEmpty()) {
            val mentioners = mentionedMe.map { msg ->
                val speaker = if (msg.companionId == -1L) "用户"
                else _allCompanions.value.find { it.id == msg.companionId }?.name ?: "某人"
                speaker
            }.distinct()
            contexts.add("最近${mentioners.joinToString("、")}@了你，他们可能在等你回应")
        }

        val lastUserMsg = historySnapshot.lastOrNull { it.companionId == -1L }
        if (lastUserMsg != null) {
            val userEmotion = when {
                lastUserMsg.content.contains(Regex("[哈h][哈h]+|哈哈哈")) -> "用户看起来很开心"
                lastUserMsg.content.contains(Regex("[呜呜|难过|伤心|😢😭]")) -> "用户好像有点难过"
                lastUserMsg.content.contains(Regex("[生气|愤怒|😡😤]")) -> "用户似乎生气了"
                lastUserMsg.content.contains(Regex("[？?]{2,}|疑惑|不懂]")) -> "用户可能有些困惑"
                else -> null
            }
            userEmotion?.let { contexts.add(it) }
        }

        val myLastMsg = historySnapshot.lastOrNull { it.companionId == companion.id }
        if (myLastMsg != null) {
            val timeSince = System.currentTimeMillis() - myLastMsg.timestamp
            val minutesAgo = timeSince / (1000 * 60)
            if (minutesAgo > 10) {
                contexts.add("你已经${minutesAgo}分钟没说话了，可以冒个泡")
            }
        }

        return contexts.joinToString("\n")
    }

    private suspend fun buildIsolatedSystemPrompt(
        companion: CompanionEntity,
        allActiveCompanions: List<CompanionEntity>,
        historySnapshot: List<GroupMessage>,
        excludeCompanionIds: Set<Long> = emptySet()
    ): String {
        val otherMembers = allActiveCompanions.filter { it.id != companion.id }
        val groupContextBlock = MentionParser.buildGroupContextBlock(companion.name, otherMembers)

        val otherMembersNames = otherMembers.map { it.name }.joinToString("、")
        val currentUserName = userRepository.userName.first()

        val recentSnapshots = buildRecentContextSnapshots(historySnapshot)
        val mentionContextBlock = MentionParser.buildMentionContext(companion.name, recentSnapshots, currentUserName)

        val sessionId = "session_${companion.id}_${System.currentTimeMillis()}"
        val personalitySeed = companion.personality.hashCode() + companion.name.hashCode()

        val personalityTraits = extractPersonalityTraits(companion)
        val groupAtmosphere = detectGroupAtmosphere(historySnapshot, currentUserName)
        val emotionalContext = buildEmotionalContext(historySnapshot, companion)
        val memoryContext = getGroupMemoryContext(historySnapshot)

        val stickerManager = StickerManager.getInstance(getApplication())
        val availableStickers = stickerManager.getAllRules()
        val stickerHint = if (availableStickers.isNotEmpty()) {
            val stickerNames = availableStickers.take(10).joinToString("、") { it.description }
            "\n=== 表情包功能 ===\n你可以发送表情包！在回复中用 [表情包描述] 格式插入表情包。\n可用的表情包：$stickerNames\n示例：哈哈 [开心] 或者 哼 [委屈]\n表情包算作一条消息，不要和其他文字混在一起。"
        } else {
            ""
        }

        val baseSystemPrompt = buildString {
            appendLine("=== 角色身份锁定 ===")
            appendLine("【会话ID】$sessionId")
            appendLine("【你的名字】${companion.name}（唯一，不可更改）")
            appendLine("【身份确认】你是${companion.name}，你必须始终保持这个身份和性格。")
            appendLine()
            appendLine("=== 群聊场景 ===")
            appendLine("这是一个真实的微信群聊天，大家在一起开心聊天。")
            appendLine("当前群聊氛围：$groupAtmosphere")
            appendLine("群里的人：你（${companion.name}）、$otherMembersNames、$currentUserName（群主）。")
            if (emotionalContext.isNotBlank()) {
                appendLine()
                appendLine("=== 当前情绪感知 ===")
                appendLine(emotionalContext)
            }
            appendLine()
            appendLine("=== 你的性格特征（必须严格遵守） ===")
            val persona = companion.personality.trim()
            if (persona.length >= 20) {
                appendLine(persona)
            } else {
                appendLine("核心性格：$persona")
                companion.age?.let { appendLine("年龄：${it}岁") }
                companion.speakingStyle?.let { appendLine("说话风格：${it}") }
                companion.backstory?.let { appendLine("背景故事：${it}") }
            }
            if (personalityTraits.isNotBlank()) {
                appendLine()
                appendLine("=== 你的说话特点 ===")
                appendLine(personalityTraits)
            }
            appendLine()
            appendLine("=== 说话规则 ===")
            appendLine("1. 你只能以${companion.name}的身份说话，保持自己的性格和口吻一致")
            appendLine("2. 像真人一样自然聊天：口语化、语气词、表情符号、网络用语")
            appendLine("3. **积极互动**：主动接话题、回应别人、发表情、分享想法")
            appendLine("4. **善用@功能**：想让人回答问题时@他，接别人话茬时也可以@，被@了要优先回")
            appendLine("5. 每次回复 = 一条气泡：一句话说完并收尾（用 。！？～… 结尾），像真人发微信，最长一两句")
            appendLine("6. 群聊是碎片化的，不要写长段落；如果还有同一话题的话没说完，会由连发机制继续追加气泡，不用在一条里塞完")
            appendLine("7. 不知道说什么就简单回应一句或发表情包")
            appendLine("8. 绝对禁止：思考过程、内心独白、分析总结、括号说明、AI式回复")
            appendLine("9. 可以模仿真人的说话习惯（如口头禅），但保持自己的人设不变")
            appendLine("10. 上下文里「[其他群友]」开头的是别人刚说的话：自然接话互动；如果别人已经说了你想说的内容，就换个角度、吐槽附和或聊新话题，绝不重复别人已说过的话")
            appendLine("11. 你自己之前说过的话（你自己发过的那些历史消息）也别重复：不要换种说法再讲一遍；想接着同一话题就补充新信息，没新信息就换个角度、简短附和或收住，还没说完就交给连发机制接着说")
            appendLine()
            appendLine("=== 回复示例（必须遵守长度） ===")
            appendLine("❌ 错误（太长）：噗，林梓涵你这说的什么呀😂脚踏两只船可不是什么好比喻呢")
            appendLine("✅ 正确（短句）：噗你这说的啥呀😂")
            appendLine("✅ 正确（短句）：脚踏两只船可不是好比喻~")
            appendLine()
            appendLine(groupContextBlock)
            if (mentionContextBlock.isNotBlank()) {
                appendLine()
                appendLine(mentionContextBlock)
            }
            if (memoryContext.isNotBlank()) {
                appendLine()
                appendLine("=== 群聊相关记忆 ===")
                appendLine(memoryContext)
            }
            appendLine()
            appendLine("=== 互动提醒 ===")
            appendLine("你是${companion.name}，你有独特的性格。在群里要活跃一点，多跟大家互动！")
            appendLine("看到感兴趣的话题就插嘴，有人@你就赶紧回，没事也能闲聊几句~")
            appendLine()
            appendLine("=== ⚠️ 格式警告 ===")
            appendLine("直接输出你的话，不要加「${companion.name}:」前缀！不要加任何前缀！")
            appendLine("错误示例：❌ ${companion.name}: 今天天气真好")
            appendLine("正确示例：✅ 今天天气真好")
            if (stickerHint.isNotBlank()) {
                appendLine()
                appendLine(stickerHint)
            }
        }

        // 回合纪律（硬性红线）：群聊自带 system_prompt，Rust 侧编排器会被跳过
        // （agent.rs：system_prompt 非空即不注入 L3 常驻片段），故此处显式补上禁令，
        // 防止模型续写对话脚本、替用户说话（与主聊天 TURN_DISCIPLINE_NOTE 等价）。
        val turnDiscipline = """
            【对话纪律（硬性红线）】
            1. 禁止替用户说话：一条回复只说你自己（${companion.name}）这一方，绝不模拟用户语气或替用户作答。
            2. 禁止自问自答：不自己提问又替用户回答。
            3. 禁止输出「用户：」「你：」等回合标记，禁止以任何分隔符（如 ｜）续写对话脚本。
            4. 只输出你自己这一方的回复；引用用户时用转述（如"你刚说想我了"），不要伪造用户回合。
        """.trimIndent()
        return baseSystemPrompt + "\n\n" + turnDiscipline
    }

    private suspend fun generateIsolatedAiReplyBubbles(
        companion: CompanionEntity,
        allActiveCompanions: List<CompanionEntity>,
        historySnapshot: List<GroupMessage>,
        excludeCompanionIds: Set<Long> = emptySet()
    ): List<String> {
        val systemPrompt = buildIsolatedSystemPrompt(companion, allActiveCompanions, historySnapshot, excludeCompanionIds)
        val companionNameMap = allActiveCompanions.associate { it.id to it.name }
        val filteredSnapshot = if (excludeCompanionIds.isEmpty()) {
            historySnapshot
        } else {
            historySnapshot.filter { it.companionId == -1L || !excludeCompanionIds.contains(it.companionId) }
        }

        val lastUserQuery = filteredSnapshot.lastOrNull { it.companionId == -1L }?.content
            ?: filteredSnapshot.lastOrNull()?.content
            ?: ""

        // 群聊回合走 Rust Cordis Agent：气泡/表情包由 emit_bubble / send_sticker 事件产出，
        // 多轮循环预算 16 轮（替代旧的 Kotlin BubbleLoopRunner 跟进气泡）。
        val memoryTools = AgentFacade.memoryToolDefinitions(getApplication())
        val memoryNames = memoryTools.map { it.name }.toSet()
        val skillTools = AgentFacade.skillToolDefinitions()
        val availableTools = ToolRegistry.availableTools(includeAppLocal = true).map { it.name }
            .toMutableList()
            .apply {
                addAll(memoryNames)
                addAll(skillTools.map { it.name })
                addAll(listOf("emit_segmented", "send_sticker", "emit_bubble"))
            }
        val orchestrationOptions = PromptOrchestratorOptions(
            memoryLimit = 5u,
            skillLimit = 3u,
            includeMemorySkill = true,
            includeSafetyNote = true,
            availableTools = availableTools,
            deviceId = DeviceIdProvider.getDeviceId(getApplication()),
            timezone = java.util.TimeZone.getDefault().id,
            sessionId = null,
            ownerName = null,
            companionNameMapJson = serializeNameMap(companionNameMap),
            workingMemoryLimit = 200u,
        )
        // 工具装配的**唯一折叠点**（授权按伴侣 × 工具命中，**不含通道维度**）：
        // companionId 必须取**本回合实际回复的伴侣**（本函数的 companion 参数，= 上层的发言人，
        // 也是下面 runTurn / recordDispatchLog 用的同一个 id）——授权是按伴侣命中的，
        // 取群 id 或其它伴侣 id 都会静默读不到决定。
        // 无决定 / 存储未注册 / 读取失败一律 fail-closed 维持既有类别（与单聊、通道侧同一实现）。
        val globalTools = AgentFacade.toolDefinitionsFor(
            companionId = companion.id,
            tools = ToolRegistry.availableTools(includeAppLocal = true),
        )
        val tools = buildList {
            addAll(memoryTools)
            addAll(skillTools)
            addAll(globalTools.filter { it.name !in memoryNames })
        }
        val turnRequest = AgentTurnRequest(
            groupId = groupId,
            historyJson = serializeHistoryJson(filteredSnapshot.map { it.toAiChatMessage() }),
            tools = tools,
            maxRounds = maxToolRounds,
            toolChoice = "auto",
            stickerProbability = 0u,
            image = null,
            systemPrompt = systemPrompt,
            companionNameMapJson = serializeNameMap(companionNameMap),
        )

        val appContext = getApplication<Application>()
        val activeApi = runCatching { apiConfigRepository.getActiveEnabledConfig() }.getOrNull()
        val partnerSession = runCatching { RemoteKeyProvider.getPartnerSession(appContext) }.getOrNull()
        val isPartner = activeApi?.provider == ApiProvider.PARTNER
        AgentFacade.syncRuntimeConfig(
            appContext,
            AgentFacade.buildSettingsJson(role = "GIRLFRIEND"),
            com.yunian.ai.agent.sticker.StickerPreferenceFacade.availableTagsWithFallback(appContext),
            AgentFacade.buildCredentialsJson(
                sessionToken = if (isPartner) partnerSession?.token else null,
                clientId = if (isPartner) partnerSession?.clientId else null,
                apiKey = activeApi?.apiKey?.takeIf { it.isNotBlank() },
            ),
        )

        // App 内群聊是本机会话：显式开启本机敏感工具（个人资料）的执行权限。
        val toolHost = AgentToolHost(appContext, allowAppLocalTools = true)
        val dispatchStartedAt = System.currentTimeMillis()
        val agentResult = withContext(Dispatchers.IO) {
            // Q4（R18）：群聊同样按「本回合发言人」实时合并世界书 ——
            // 专属书 ∪ 绑定的全局书，合成单本 ST JSON 后幂等覆盖写入 Rust 单槽。
            runCatching {
                com.yunian.ai.agent.worldbook.WorldbookRepository(appContext)
                    .syncActiveToRuntime(companion.id)
            }.onFailure {
                SecureLog.w("GroupChatViewModel", "worldbook sync failed: ${it.message}")
            }
            // Commerce 类工具的确认门在群聊里同样没有可确认的界面：与通道侧共用
            // [AgentConfirmGuard] 的 fail-closed 自动拒绝 + 有限次重跑。此前这里直接把
            // confirm_pending 的结果交给上层，回合静默结束、气泡为空 —— 用户被晾在死路上。
            agentConfirmGuard.drive(
                tag = "GroupChatViewModel",
                initial = AgentFacade.runTurn(turnRequest, appContext, companion.id, toolHost),
                rerunFailureMessage = "runTurn after auto-reject failed, companion=${companion.id}",
                runTurn = { AgentFacade.runTurn(turnRequest, appContext, companion.id, toolHost) },
                rejectTool = { name, args -> AgentFacade.rejectTool(appContext, name, args) },
            )
        }

        AgentFacade.recordDispatchLog(
            context = appContext, companionId = companion.id, groupId = groupId,
            sessionId = null, dispatchId = "group_${groupId}_${dispatchStartedAt}",
            provider = activeApi?.provider?.name ?: "", model = activeApi?.model ?: "",
            startedAtMs = dispatchStartedAt, completedAtMs = System.currentTimeMillis(),
            roundsUsed = agentResult.roundsUsed.toInt(), finishedReason = agentResult.finishedReason,
            error = agentResult.error, toolNames = tools.map { it.name },
            toolCalls = toolHost.collectedToolCalls(), events = agentResult.events,
            querySummary = lastUserQuery,
        )
        AgentFacade.recordTurnAudit(
            context = appContext, companionId = companion.id, groupId = groupId,
            sessionId = null, options = orchestrationOptions, query = lastUserQuery,
            roundsUsed = agentResult.roundsUsed.toInt(), toolNames = tools.map { it.name },
        )

        val bubbles = mutableListOf<String>()
        for (event in agentResult.events) {
            if (event.kind == "bubble" && event.text.isNotBlank()) {
                bubbles.add(event.text.trim().replace(Regex("\\n{2,}"), "\n"))
            }
        }

        // 兜底：模型只产出 finalText（未走 emit_bubble）时补一条，避免群聊静默。
        val closing = agentResult.finalText.trim().replace(Regex("\\n{2,}"), "\n")
        if (closing.isNotBlank() && bubbles.none { it == closing }) {
            bubbles.add(closing)
        }

        // U1：确认门重跑到上限（3 次）后模型仍坚持调用被拒工具时，Rust 回合既没有 bubble 事件、
        // 也没有 finalText —— 上面两段都补不出内容，群聊首条取空即直接 return（User 被晾在死路上，
        // 表现为空白/无回复）。通道侧由 [AgentTurnReplyText.resolve] 兜出一条含「确认」的可见文案，
        // 这里复用同一份实现，保证两侧文案逐字一致；**仅在** confirm_pending 终局且确实没有任何
        // 可见内容时补一条，正常的 completed 路径完全不受影响。
        if (bubbles.isEmpty() &&
            agentResult.finishedReason == AgentConfirmGuard.FINISHED_CONFIRM_PENDING
        ) {
            AgentTurnReplyText.resolve(agentResult.events, agentResult.finalText, agentResult.finishedReason)
                ?.takeIf { it.isNotBlank() }
                ?.let { bubbles.add(it.trim().replace(Regex("\\n{2,}"), "\n")) }
        }
        return bubbles
    }

    /** 领域历史 → OpenAI messages JSON（AgentTurnRequest.historyJson）。 */
    private fun serializeHistoryJson(history: List<AiChatMessage>): String {
        val arr = org.json.JSONArray()
        for (msg in history) {
            val m = org.json.JSONObject().apply {
                put("role", if (msg.isFromUser) "user" else "assistant")
                put("content", msg.content)
            }
            arr.put(m)
        }
        return arr.toString()
    }

    private suspend fun getGroupMemoryContext(historySnapshot: List<GroupMessage>): String {
        val query = historySnapshot.lastOrNull { it.companionId == -1L }?.content
            ?: historySnapshot.lastOrNull()?.content
            ?: ""
        if (query.isBlank()) return ""
        return memoryProvider.getMemoryContext(
            companionId = null,
            groupId = groupId,
            query = query,
            limit = 5
        ).take(500)
    }

    fun triggerAiSpeak(companionId: Long) {
        applicationScope.launch {
            try {
                val group = _groupData.value
                    ?: throw IllegalStateException("Group data is null")
                val companion = _allCompanions.value.find { it.id == companionId }
                    ?: throw IllegalStateException("Companion not found: $companionId")
                val activeCompanionIds = group.getCompanionIdList()
                val activeCompanions = _allCompanions.value.filter { activeCompanionIds.contains(it.id) }

                val bubbles = generateIsolatedAiReplyBubbles(
                    companion = companion,
                    allActiveCompanions = activeCompanions,
                    // 与主路径一致：先过隔离前缀处理，其他角色匿名化为「[其他群友]」
                    historySnapshot = buildIsolatedHistorySnapshot(getRecentHistorySnapshot(), companion.id)
                )
                bubbles.forEachIndexed { index, content ->
                    val trimmed = content.trim()
                    if (trimmed.isBlank()) return@forEachIndexed
                    if (index > 0) delay(Random.nextLong(600L, 1600L))
                    val outputCheck = com.yunian.ai.common.ContentFilter.checkOutputSafety(trimmed)
                    val safeContent = if (!outputCheck.isSafe) {
                        android.util.Log.w("GroupChatViewModel", "triggerAiSpeak output blocked: ${outputCheck.reason}")
                        com.yunian.ai.common.BanManager.recordViolation(getApplication(), outputCheck.level)
                        "抱歉，我无法回应这个话题。"
                    } else trimmed
                    sendSplitAiMessages(safeContent, groupId, companion.id)
                }
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "triggerAiSpeak failed", e)
            }
        }
    }

    fun recallMessage(message: GroupMessage) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                groupMessageRepository.deleteMessage(message)
                Log.d("GroupChatViewModel", "消息已撤回: id=${message.id}")
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "recallMessage failed", e)
            }
        }
    }

    fun regenerateMessage(targetMessage: GroupMessage) {

        if (!isLoadingLock.compareAndSet(false, true)) {
            Log.w("GroupChatViewModel", "regenerateMessage ignored: already processing")
            return
        }
        sendMessageJob?.cancel()
        sendMessageJob = applicationScope.launch(Dispatchers.IO) {
            try {
                val targetCompanionId = targetMessage.companionId
                if (targetCompanionId <= 0) throw IllegalStateException("Invalid target companion ID: $targetCompanionId")
                _isRegenerating.value = true
                groupMessageRepository.deleteMessage(targetMessage)
                val history = groupMessageRepository.getMessagesForGroup(groupId, 100).first().reversed()
                val lastUserMsg = history.lastOrNull { it.companionId == -1L }
                    ?: throw IllegalStateException("No user message found in history")
                val companion = _allCompanions.value.find { it.id == targetCompanionId }
                    ?: throw IllegalStateException("Companion not found: $targetCompanionId")

                // 成员过滤与 sendMessage 主路径一致：只有群成员进入上下文，
                // 防止未入群的 companion 以真实身份出现在提示词里
                val activeCompanionIds = _groupData.value?.getCompanionIdList() ?: emptyList()
                val activeCompanions = if (activeCompanionIds.isEmpty()) {
                    _allCompanions.value
                } else {
                    _allCompanions.value.filter { activeCompanionIds.contains(it.id) }
                }

                val bubbles = generateIsolatedAiReplyBubbles(
                    companion = companion,
                    allActiveCompanions = activeCompanions,
                    // 与主路径一致：先过隔离前缀处理，其他角色匿名化为「[其他群友]」
                    historySnapshot = buildIsolatedHistorySnapshot(history, companion.id)
                )
                bubbles.forEachIndexed { index, content ->
                    val trimmed = content.trim()
                    if (trimmed.isBlank()) return@forEachIndexed
                    if (index > 0) delay(Random.nextLong(600L, 1600L))
                    val outputCheck = com.yunian.ai.common.ContentFilter.checkOutputSafety(trimmed)
                    val safeReply = if (!outputCheck.isSafe) {
                        android.util.Log.w("GroupChatViewModel", "regenerate output blocked: ${outputCheck.reason}")
                        com.yunian.ai.common.BanManager.recordViolation(getApplication(), outputCheck.level)
                        "抱歉，我无法回应这个话题。"
                    } else trimmed
                    sendSplitAiMessages(safeReply, groupId, targetCompanionId)
                }
                Log.d("GroupChatM", "[${companion.name}] 重新生成回复成功 (${bubbles.size}气泡)")
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "regenerateMessage failed", e)
            } finally {
                _isRegenerating.value = false

                isLoadingLock.set(false)
            }
        }
    }

    fun deleteGroup() {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val group = _groupData.value
                    ?: throw IllegalStateException("Group data is null")
                chatGroupRepository.deleteGroup(group)
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "deleteGroup failed", e)
            }
        }
    }

    fun updateGroupAvatar(avatarUrl: String) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val group = _groupData.value
                    ?: throw IllegalStateException("Group data is null")
                val updatedGroup = group.copy(avatarUrl = avatarUrl)
                chatGroupRepository.updateGroup(updatedGroup)
                _groupData.value = updatedGroup
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "updateGroupAvatar failed", e)
            }
        }
    }

    fun updateGroupName(name: String) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val group = _groupData.value
                    ?: throw IllegalStateException("Group data is null")
                val updatedGroup = group.copy(name = name)
                chatGroupRepository.updateGroup(updatedGroup)
                _groupData.value = updatedGroup
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "updateGroupName failed", e)
            }
        }
    }

    private fun broadcastWeChatMessage(companionId: Long, messageId: Long) {
        WeChatProactiveSync.enqueue(companionId, messageId)
        Log.d("GroupChatViewModel", "Enqueue WeChat proactive message, companionId=$companionId, messageId=$messageId")
    }

    private fun cleanAiReply(raw: String, companionName: String? = null): String {
        var text = com.yunian.ai.network.ResponsePostProcessor.stripThinkingContent(raw)
        val rolePrefixRegex = Regex("(?m)^\\s*\\[(?:角色\\d+|[^\\[\\]]+?)\\]\\s*")
        text = rolePrefixRegex.replace(text, "")
        val encRegex = Regex("(?m)^enc:\\S+$")
        text = encRegex.replace(text, "")

        // 角色串线防线：剥离模型续写的「用户回合」脚本。
        // 群聊此前只剥行首名字前缀、不截断内容——模型编造的用户话剥掉前缀后仍以 AI 口吻落库。
        // 必须在下面的名字前缀剥离之前执行，避免「用户：xxx」的标记先被剥掉而漏截。
        text = ScriptTurnStripper.strip(text, aiName = companionName)

        val allNames = _allCompanions.value.map { it.name }.toSet() + listOfNotNull("用户", companionName)
        val namePattern = allNames.joinToString("|") { Regex.escape(it) }
        if (namePattern.isNotBlank()) {
            val namePrefixPattern = Regex("(?m)^($namePattern)[：: ]\\s*")
            text = namePrefixPattern.replace(text, "")
        }

        return text.trim().replace(Regex("\\n{2,}"), "\n")
    }

    private suspend fun sendSplitAiMessages(
        rawContent: String,
        groupId: Long,
        companionId: Long
    ) {
        val companionName = _allCompanions.value.find { it.id == companionId }?.name
        val cleaned = cleanAiReply(rawContent, companionName)

        val stickerManager = StickerManager.getInstance(getApplication())
        val systemTags = setOf("语音", "图片", "视频", "文件", "位置", "红包", "转账")
        val stickerRegex = Regex("\\[([^\\]]+)\\]")
        // 先剥系统标签（P1-7）：[图片]/[文件] 等系统标签不应出现在文本气泡里。
        // 旧实现仅在抽取贴纸时跳过它们，但子串游标仍基于原文——夹在中间的系统标签会漏进文本段。
        // 剥离后 systemTags 在 systemTagFree 中已不存在，下游游标问题自然消失。
        val systemTagFree = stickerRegex.replace(cleaned) { m ->
            if (m.groupValues[1].trim() in systemTags) "" else m.value
        }
        val matches = stickerRegex.findAll(systemTagFree)

        val textSegments = mutableListOf<String>()
        val stickerNames = mutableListOf<String>()

        var lastIndex = 0
        for (match in matches) {
            val description = match.groupValues[1].trim()
            if (description !in systemTags) {
                val beforeText = systemTagFree.substring(lastIndex, match.range.first).trim()
                if (beforeText.isNotBlank()) {
                    textSegments.add(beforeText)
                }
                stickerNames.add(description)
                lastIndex = match.range.last + 1
            }
        }
        val remainingText = systemTagFree.substring(lastIndex).trim()
        if (remainingText.isNotBlank()) {
            textSegments.add(remainingText)
        }

        if (stickerNames.isEmpty() && textSegments.isEmpty()) {
            return
        }

        if (stickerNames.isEmpty() && textSegments.size <= 1) {
            // 按 AI 自己敲的换行拆分：单行即一条；成员在回复里敲了回车就逐条发出（群聊同样尊重 AI 的分条意图）
            val textBubbles = BubbleTextSplitter.splitByParagraphs(systemTagFree)
            textBubbles.forEachIndexed { index, bubbleText ->
                if (index > 0) delay(Random.nextLong(600L, 1600L))
                if (bubbleText.isBlank()) return@forEachIndexed
                val msg = GroupMessage(groupId = groupId, companionId = companionId, content = bubbleText, timestamp = System.currentTimeMillis())
                val msgId = messageWriter.enqueueGroup(msg)
                broadcastWeChatMessage(companionId, msgId)
            }
        } else {

            val orderedItems = mutableListOf<Either<String, String>>()
            var cursor = 0
            for (match in stickerRegex.findAll(systemTagFree)) {
                val description = match.groupValues[1].trim()
                if (description !in systemTags) {
                    val beforeText = systemTagFree.substring(cursor, match.range.first).trim()
                    if (beforeText.isNotBlank()) {
                        orderedItems.add(Either.Left(beforeText))
                    }
                    orderedItems.add(Either.Right(description))
                    cursor = match.range.last + 1
                }
            }
            val remaining = systemTagFree.substring(cursor).trim()
            if (remaining.isNotBlank()) {
                orderedItems.add(Either.Left(remaining))
            }

            val textCount = orderedItems.count { it is Either.Left }
            val stickerCount = orderedItems.count { it is Either.Right }
            repeat(stickerNames.size - stickerCount) { i ->
                orderedItems.add(Either.Right(stickerNames[stickerCount + i]))
            }
            repeat(textSegments.size - textCount) { i ->
                orderedItems.add(Either.Left(textSegments[textCount + i]))
            }

            // 文本段再按 AI 自己敲的换行展开（保持与贴纸的交错顺序：文本按序拆行、贴纸位置不变）
            val expandedItems = orderedItems.flatMap { item ->
                when (item) {
                    is Either.Left -> BubbleTextSplitter.splitByParagraphs(item.value)
                        .filter { it.isNotBlank() }
                        .map { Either.Left(it) }
                    is Either.Right -> listOf(item)
                }
            }

            Log.d("GroupChatViewModel", "AI回复按原文顺序发送 ${expandedItems.size} 项 (text=${textSegments.size}, sticker=${stickerNames.size})")
            // P2-C1：本批查重小窗（思路同 CompanionMessageWorker.sendMessage）——左向文本气泡
            // enqueue 前与本批已发内容比对，命中复读即跳过，避免同一批里整行复读。
            val batchSentNorms = mutableListOf<String>()
            var segmentIndex = 0
            for (item in expandedItems) {
                delay(Random.nextLong(600L, 1600L))
                when (item) {
                    is Either.Left -> {
                        val norm = DedupGuard.normalize(item.value)
                        if (norm.isNotEmpty() && DedupGuard.isDuplicate(norm, batchSentNorms)) {
                            Log.w("GroupChatViewModel", "dedup hit in batch, skip bubble: ${item.value.take(30)}")
                        } else {
                            if (norm.isNotEmpty()) batchSentNorms.add(norm)
                            val msg = GroupMessage(groupId = groupId, companionId = companionId, content = item.value, timestamp = System.currentTimeMillis())
                            val msgId = messageWriter.enqueueGroup(msg)
                            broadcastWeChatMessage(companionId, msgId)
                        }
                    }
                    is Either.Right -> {
                        sendStickerMessage(groupId, companionId, item.value)
                    }
                }
                segmentIndex++
                Log.d("GroupChatViewModel", "发送 $segmentIndex/${expandedItems.size}")
            }
        }
    }

    private sealed class Either<out L, out R> {
        data class Left<L>(val value: L) : Either<L, Nothing>()
        data class Right<R>(val value: R) : Either<Nothing, R>()
    }

    private suspend fun sendStickerMessage(
        groupId: Long,
        companionId: Long,
        stickerDescription: String
    ) {
        try {
            val stickerManager = StickerManager.getInstance(getApplication())
            var sticker = stickerManager.findStickerByDescriptionExact(stickerDescription)
            if (sticker == null) {
                sticker = stickerManager.findStickerByDescription(stickerDescription)
            }
            if (sticker != null) {
                val stickerId = sticker.fileName ?: sticker.name
                val stickerMessage = GroupMessage(
                    groupId = groupId,
                    companionId = companionId,
                    content = "[$stickerId]",
                    timestamp = System.currentTimeMillis()
                )
                val msgId = messageWriter.enqueueGroup(stickerMessage)
                broadcastWeChatMessage(companionId, msgId)
                Log.d("GroupChatViewModel", "群聊表情包已发送: [$stickerId]")
            } else {
                Log.w("GroupChatViewModel", "表情包未找到: [$stickerDescription]，发送为文本")
                val fallbackMsg = GroupMessage(
                    groupId = groupId,
                    companionId = companionId,
                    content = "[$stickerDescription]",
                    timestamp = System.currentTimeMillis()
                )
                messageWriter.enqueueGroup(fallbackMsg)
            }
        } catch (e: Exception) {
            Log.e("GroupChatViewModel", "sendStickerMessage failed", e)
        }
    }

    fun sendUserSticker(sticker: StickerInfo) {
        applicationScope.launch {
            try {
                val stickerId = sticker.fileName ?: sticker.name
                val stickerMessage = GroupMessage(
                    groupId = groupId,
                    companionId = -1L,
                    content = "[$stickerId]",
                    timestamp = System.currentTimeMillis()
                )
                messageWriter.enqueueGroup(stickerMessage)
                Log.d("GroupChatViewModel", "用户在群聊发送了表情包: [$stickerId]")
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "sendUserSticker failed", e)
            }
        }
    }

    fun sendImageMessage(imagePath: String) {
        applicationScope.launch {
            try {
                val imageMessage = GroupMessage(
                    groupId = groupId,
                    companionId = -1L,
                    content = "[图片] $imagePath",
                    timestamp = System.currentTimeMillis()
                )
                messageWriter.enqueueGroup(imageMessage)
                Log.d("GroupChatViewModel", "用户在群聊发送了图片: $imagePath")
            } catch (e: Exception) {
                Log.e("GroupChatViewModel", "sendImageMessage failed", e)
            }
        }
    }

    override fun onCleared() {
        avatarUnsubscribe?.invoke()
        avatarUnsubscribe = null
        nicknameUnsubscribe?.invoke()
        nicknameUnsubscribe = null
        super.onCleared()

        sendMessageJob?.cancel()
        _isLoading.value = false
        isLoadingLock.set(false)
        _isRegenerating.value = false
    }
}

class GroupChatViewModelFactory(
    private val application: Application,
    private val groupId: Long
) : ViewModelProvider.Factory {
    override fun <T : androidx.lifecycle.ViewModel> create(modelClass: Class<T>, extras: CreationExtras): T {
        @Suppress("UNCHECKED_CAST")
        return GroupChatViewModel(application, groupId) as T
    }
}
