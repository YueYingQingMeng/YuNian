package com.yunian.ai.feature.chat.ui.viewmodel

import android.app.Application
import android.os.SystemClock
import com.yunian.ai.common.AppSettingsStore
import com.yunian.ai.common.ApplicationScopeProvider
import com.yunian.ai.common.BanManager
import com.yunian.ai.common.ChatConstants
import com.yunian.ai.common.CompanionRole
import com.yunian.ai.common.ContentFilter
import com.yunian.ai.common.EnvAnchorCooldown
import com.yunian.ai.common.EnvAnchorStore
import com.yunian.ai.common.RolePromptProvider
import com.yunian.ai.common.SecureLog
import com.yunian.ai.common.StickerManager
import com.yunian.ai.common.TimeoutBudgets
import com.yunian.ai.common.perf.PerfBoost
import com.yunian.ai.domain.wechat.WeChatProactiveSync
import com.yunian.ai.database.model.ApiProvider
import com.yunian.ai.database.model.ChatMessage
import com.yunian.ai.database.model.CompanionEntity
import com.yunian.ai.database.model.MessageType
import com.yunian.ai.database.repository.ApiConfigRepository
import com.yunian.ai.database.repository.ChatRepository
import com.yunian.ai.database.repository.CompanionRepository
import com.yunian.ai.database.repository.MessageWriteCoordinator
import com.yunian.ai.database.repository.UserRepository
import com.yunian.ai.domain.AiChatMessage
import com.yunian.ai.domain.AiCompanionInfo
import com.yunian.ai.domain.AiMessageRole
import com.yunian.ai.domain.AiMessageType
import com.yunian.ai.domain.AiOperationalMessages
import com.yunian.ai.domain.AiServiceProvider
import com.yunian.ai.domain.imagegen.ImageGenService
import com.yunian.ai.domain.MemoryProvider
import com.yunian.ai.domain.ServiceRegistry
import com.yunian.ai.domain.ToolRegistry
import com.yunian.ai.feature.chat.R
import com.yunian.ai.feature.chat.data.ChatContextResolver
import com.yunian.ai.feature.chat.data.ChatDetailSettingsStore
import com.yunian.ai.feature.chat.plugin.ChatToolLifecycleItem
import com.yunian.ai.feature.chat.plugin.ChatToolLifecyclePlugin
import com.yunian.ai.feature.chat.plugin.ChatToolLifecycleProjection
import com.yunian.ai.feature.chat.plugin.ChatToolLifecycleStatus
import com.yunian.ai.feature.chat.timeline.EventCommitRules
import com.yunian.ai.feature.chat.timeline.PendingTurnStreamApplier
import com.yunian.ai.feature.chat.timeline.StreamingReasoningMessagePipeline
import com.yunian.ai.feature.chat.voice.ChatTtsController
import com.yunian.ai.feature.chat.voice.ChatTtsState
import com.yunian.ai.network.ChatTypingState
import com.yunian.ai.network.stream.NonStreamingAssistantStreamAdapter
import com.yunian.ai.network.tts.ChatTtsConfig
import com.yunian.ai.network.tts.ChatTtsMode
import com.yunian.ai.network.tts.TtsService
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONArray
import org.json.JSONObject
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import com.yunian.ai.common.concurrent.DuplicateSendGuard

class ChatGenerationManager private constructor(
    private val application: Application,
    private val companionId: Long
) {
    companion object {
        private val instances = ConcurrentHashMap<Long, ChatGenerationManager>()
        private val questionRegex = Regex("[?？]|吗|呢|什么|怎么|为什么|多少|哪|谁|几|是不是|有没有|能不能|会不会|要不要|好不好")

        private const val IDLE_DISPOSE_MS = 60_000L

        fun get(application: Application, companionId: Long): ChatGenerationManager =
            instances.getOrPut(companionId) { ChatGenerationManager(application, companionId) }

        fun acquire(application: Application, companionId: Long): ChatGenerationManager {
            while (true) {
                val manager = get(application, companionId)
                if (manager.tryAcquire()) return manager
                instances.remove(companionId, manager)
            }
        }

        fun release(companionId: Long) {
            instances[companionId]?.onReleased()
        }
    }

    private val refCount = AtomicInteger(0)
    @Volatile private var disposed = false
    private var disposeJob: Job? = null

    private val scope = ApplicationScopeProvider.scope
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        ChatDebugLog.log("[ChatGeneration] uncaught: ${throwable.javaClass.simpleName}: ${throwable.message}")
        SecureLog.e("ChatGenerationManager", "Uncaught generation exception", throwable)
    }
    private val apiConfigRepository = ServiceRegistry.getOrThrow(ApiConfigRepository::class.java)
    private val chatRepository = ServiceRegistry.getOrThrow(ChatRepository::class.java)
    private val messageWriter = ServiceRegistry.getOrThrow(MessageWriteCoordinator::class.java)
    private val companionRepository = ServiceRegistry.getOrThrow(CompanionRepository::class.java)
    private val contextResolver = ChatContextResolver(chatRepository)
    private val chatDetailSettingsStore = ChatDetailSettingsStore(application)
    private val appSettingsStore = AppSettingsStore(application)
    private val envAnchorStore by lazy { EnvAnchorStore(application) }
    private val stickerManager by lazy { StickerManager.getInstance(application) }
    private val memoryProvider: MemoryProvider by lazy {
        ServiceRegistry.getOrThrow(MemoryProvider::class.java).also { it.initialize() }
    }
    private val userRepository = ServiceRegistry.get(UserRepository::class.java)
    private val aiService = ServiceRegistry.get(AiServiceProvider::class.java)
        ?: throw IllegalStateException("AiServiceProvider not registered in ServiceRegistry")
    private val _chatTtsConfig = MutableStateFlow(ChatTtsConfig.fromSharedPreferences(application))
    val chatTtsConfig: StateFlow<ChatTtsConfig> = _chatTtsConfig.asStateFlow()
    @Volatile private var callActive = false
    private val _ttsState = MutableStateFlow(ChatTtsState.IDLE)
    private val ttsControllerDelegate = lazy {
        ChatTtsController(
            context = application.applicationContext,
            ttsService = TtsService.getInstance(application),
            scope = scope,
            configProvider = { _chatTtsConfig.value },
            callActiveProvider = { callActive }
        ).also { controller ->
            scope.launch { controller.state.collect { _ttsState.value = it } }
        }
    }
    private val ttsController by ttsControllerDelegate
    val ttsState: StateFlow<ChatTtsState> = _ttsState.asStateFlow()
    private val typingState = ChatTypingState()
    private val activeRequests = AtomicInteger(0)
    private val messageQueue = Channel<String>(capacity = 100)
    private val turnState = ChatTurnState()
    @Volatile private var latestCompanionInfo: AiCompanionInfo? = null
    private var messageConsumerJob: Job? = null
    private var replacementJob: Job? = null

    /**
     * 单轮生成的"是否已产出回复（已落库）"信号。
     *
     * 每轮生成都新建一个（不复用上一轮的值），默认 `false`。
     * 由 [AiResponseFinalizer.deliverResponse] 在某段回复确实写入数据库后置位（见 `onCommitted`）。
     * 消费循环在"新消息打断在飞生成"时据此决定旧 batch 的去留：
     * - 已落库 → 旧批消息已被回答过，丢弃以免重复回答；
     * - 未落库 → 旧 job 被干净取消、没有产生任何回复，保留原合并重发行为以免丢消息。
     */
    private class TurnCommitSignal {
        private val committed = AtomicBoolean(false)
        val isCommitted: Boolean get() = committed.get()
        fun markCommitted() { committed.set(true) }
    }

    private val _events = MutableSharedFlow<ChatUiEvent>(extraBufferCapacity = 8)
    val events: SharedFlow<ChatUiEvent> = _events.asSharedFlow()

    private val _confirmationRequest = MutableStateFlow<ToolConfirmationRequest?>(null)

    val confirmationRequest: StateFlow<ToolConfirmationRequest?> = _confirmationRequest.asStateFlow()
    private val confirmationChannel = Channel<Boolean>(capacity = 1)

    private suspend fun requestToolConfirmation(toolName: String, argumentsJson: String): Boolean {
        val tool = ToolRegistry.get(toolName)
        val summary = tool?.summarizeArguments(argumentsJson) ?: argumentsJson.take(120)
        val request = ToolConfirmationRequest(
            id = System.currentTimeMillis(),
            toolName = toolName,
            summary = summary,
            argumentsJson = argumentsJson
        )
        typingState.stopTyping()
        _confirmationRequest.value = request

        while (confirmationChannel.tryReceive().isSuccess) { }
        return try {
            confirmationChannel.receive()
        } finally {
            if (_confirmationRequest.value?.id == request.id) {
                _confirmationRequest.value = null
            }
            if (activeRequests.get() > 0) typingState.startTyping()
        }
    }

    fun respondToConfirmation(id: Long, confirmed: Boolean) {
        val current = _confirmationRequest.value
        if (current?.id != id) return
        _confirmationRequest.value = null
        confirmationChannel.trySend(confirmed)
    }

    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()

    private val _isRegenerating = MutableStateFlow(false)
    val isRegenerating: StateFlow<Boolean> = _isRegenerating.asStateFlow()

    private val _queueDepth = MutableStateFlow(0)
    val queueDepth: StateFlow<Int> = _queueDepth.asStateFlow()

    /** 当前轮次的工具调用活动（OpenMinis 风格过程气泡数据源），turn 结束清空 */
    private val _toolActivity = MutableStateFlow<List<ToolActivity>>(emptyList())
    val toolActivity: StateFlow<List<ToolActivity>> = _toolActivity.asStateFlow()

    val isTyping: StateFlow<Boolean> = typingState.isTyping
    val typingText: StateFlow<String> = typingState.typingText

    val pipeline = MessagePipelineRunner { level -> BanManager.recordViolation(application, level) }

    private val streamApplier = PendingTurnStreamApplier()

    private val responseFinalizer by lazy { AiResponseFinalizer(
        companionId = companionId,
        chatRepository = chatRepository,
        messageWriter = messageWriter,
        memoryProvider = memoryProvider,
        stickerManager = stickerManager,
        chatDetailSettingsStore = chatDetailSettingsStore,
        appSettingsStore = appSettingsStore,
        contextResolver = contextResolver,
        aiService = aiService,
        applicationApiScope = scope,
        turnState = turnState,
        chatTtsController = ttsController,
        application = application,
        questionRegex = questionRegex,
    ).apply {
        companionInfoProvider = { latestCompanionInfo }
    } }

    // 防连击：同内容 2 秒窗口内重复提交静默忽略（第一条已发出，双击/回车连按误触）
    private val duplicateSendGuard = DuplicateSendGuard()

    fun sendText(content: String) {
        if (duplicateSendGuard.shouldReject(content)) return
        startMessageConsumer()
        // 新一轮对话开始：清掉上一轮的工具过程气泡（常驻展示一轮）
        _toolActivity.value = emptyList()
        val userMessage = ChatMessage(
            companionId = companionId,
            content = content,
            isFromUser = true,
            timestamp = System.currentTimeMillis()
        )
        scope.launch(Dispatchers.IO + exceptionHandler) {
            val userMessageId = messageWriter.enqueueChat(userMessage)
            broadcastWeChatMessage(userMessageId)

        if (apiConfigRepository.getActiveEnabledConfig() == null && !hasCompanionBoundConfig()) {
            _events.tryEmit(ChatUiEvent.Error("请先配置API：我 → API设置 → 添加密钥"))
        }
            val result = messageQueue.trySend(content)
            if (result.isSuccess) {
                _queueDepth.update { it + 1 }

                if (!typingState.isTyping.value) {
                    typingState.startTyping()
                }
            } else {
                _events.tryEmit(ChatUiEvent.Error("消息队列已满，请稍后再试"))
                SecureLog.w("ChatGenerationManager", "Message queue full, dropped: ${content.take(20)}...")
            }
        }
    }

    fun sendImage(imagePath: String) {
        replaceActiveGeneration("Image message superseded active generation") {
            val userMessageId = messageWriter.enqueueChat(
                ChatMessage(
                    companionId = companionId,

                    content = "[图片]",
                    isFromUser = true,
                    timestamp = System.currentTimeMillis(),
                    type = MessageType.IMAGE,
                    linkString = imagePath
                )
            )
            broadcastWeChatMessage(userMessageId)
            val history = contextResolver.getHistoryForAi(companionId)
            val settings = chatDetailSettingsStore.getSettings(companionId)
            turnState.sendMessageJob = startAiResponse(
                history = history,
                stickerProbability = settings.stickerProbability,
                userContentForMemory = "[图片]",
                imagePath = imagePath,
                ntpTimeEnabled = settings.ntpTimeEnabled
            )
            turnState.sendMessageJob?.join()
        }
    }

    fun regenerate(targetMessage: ChatMessage) {
        replaceActiveGeneration("Regenerate superseded active generation") {
            _isRegenerating.value = true
            try {
                chatRepository.deleteMessage(targetMessage)
                // 一并清理被替换那一轮的工具调用卡片（其 turnId 与该条助手消息一致）
                targetMessage.turnId?.let { chatRepository.deleteToolActivitiesForTurn(companionId, it) }
                val history = contextResolver.getHistoryForAi(companionId)
                val settings = chatDetailSettingsStore.getSettings(companionId)
                turnState.sendMessageJob = startAiResponse(
                    history = history,
                    stickerProbability = settings.stickerProbability,
                    userContentForMemory = "",
                    ntpTimeEnabled = settings.ntpTimeEnabled
                )
                turnState.sendMessageJob?.join()
            } finally {
                _isRegenerating.value = false
            }
        }
    }

    fun setCallActive(active: Boolean) {
        callActive = active
        if (active) ttsController.stop()
    }

    fun setTtsMode(mode: ChatTtsMode) {

        val normalized = when (mode) {
            ChatTtsMode.VOICE_BAR -> ChatTtsMode.VOICE_BAR
            else -> ChatTtsMode.SILENT
        }
        updateTtsConfig(_chatTtsConfig.value.copy(mode = normalized))
    }

    fun updateTtsConfig(config: ChatTtsConfig) {
        val normalized = config.copy(
            mode = when (config.mode) {
                ChatTtsMode.VOICE_BAR -> ChatTtsMode.VOICE_BAR
                else -> ChatTtsMode.SILENT
            }
        )
        _chatTtsConfig.value = normalized
        ChatTtsConfig.saveToSharedPreferences(application, normalized)
        if (normalized.mode == ChatTtsMode.SILENT) ttsController.stop()
    }

    fun stopTts() = ttsController.stop()

    private fun tryAcquire(): Boolean {
        synchronized(this) {
            if (disposed) return false
            refCount.incrementAndGet()
            cancelPendingDisposeLocked()
            return true
        }
    }

    private fun onReleased() {
        val shouldSchedule: Boolean
        synchronized(this) {
            if (disposed) return
            val remaining = refCount.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
            shouldSchedule = remaining == 0
        }
        if (shouldSchedule) {
            scheduleIdleDispose()
        }
    }

    private fun cancelPendingDisposeLocked() {
        disposeJob?.cancel()
        disposeJob = null
    }

    private fun scheduleIdleDispose() {
        synchronized(this) {
            if (disposed || refCount.get() > 0) return
            cancelPendingDisposeLocked()
            disposeJob = scope.launch(Dispatchers.IO + exceptionHandler) {
                delay(IDLE_DISPOSE_MS)
                disposeIfIdle()
            }
        }
    }

    private fun isBusy(): Boolean {
        return activeRequests.get() > 0 ||
            _isLoading.value ||
            _isRegenerating.value ||
            _queueDepth.value > 0 ||
            turnState.sendMessageJob?.isActive == true ||
            replacementJob?.isActive == true
    }

    private fun disposeIfIdle() {
        val shouldReschedule: Boolean
        synchronized(this) {
            if (disposed) return
            if (refCount.get() > 0 || isBusy()) {

                shouldReschedule = refCount.get() == 0
            } else {
                disposed = true
                cancelPendingDisposeLocked()
                runCatching { stopTts() }
                runCatching { messageQueue.close() }
                messageConsumerJob?.cancel()
                messageConsumerJob = null
                replacementJob?.cancel()
                replacementJob = null
                turnState.cancelSendJob()
                turnState.sendMessageJob = null
                contextResolver.clearCache(companionId)
                instances.remove(companionId, this)
                SecureLog.i("ChatGenerationManager", "Disposed idle manager for companion=$companionId")
                shouldReschedule = false
            }
        }
        if (shouldReschedule) {
            scheduleIdleDispose()
        }
    }

    suspend fun synthesizeForVoiceBar(text: String): String? =
        ttsController.synthesizeOnly(text)?.path

    private fun replaceActiveGeneration(reason: String, block: suspend () -> Unit) {
        replacementJob?.cancel(CancellationException(reason))
        turnState.sendMessageJob?.takeIf { it.isActive }?.cancel(CancellationException(reason))
        turnState.reset()
        replacementJob = scope.launch(Dispatchers.IO + exceptionHandler) {
            try {
                block()
            } catch (_: CancellationException) {
                Unit
            } catch (exception: Exception) {
                SecureLog.e("ChatGenerationManager", reason, exception)
                _events.tryEmit(ChatUiEvent.Error(exception.message?.removePrefix("[TOAST]") ?: "发送失败"))
            }
        }
    }

    private fun startMessageConsumer() {
        if (messageConsumerJob?.isActive == true) return
        messageConsumerJob = scope.launch(Dispatchers.IO + exceptionHandler) {

            val pending = mutableListOf<String>()

            var waitForMergeWindow = true
            while (true) {
                if (pending.isEmpty()) {
                    val first = messageQueue.receiveCatching()
                    if (first.isClosed) break
                    if (first.exceptionOrNull() != null) continue
                    pending.add(first.getOrThrow())
                    _queueDepth.update { maxOf(0, it - 1) }
                    waitForMergeWindow = true
                }

                if (waitForMergeWindow) {

                    drainQueueWithTimeout(pending, ChatConstants.MESSAGE_BATCH_WINDOW_MS)
                } else {
                    delay(ChatConstants.MESSAGE_BATCH_SPLIT_DELAY_MS)
                    drainTryReceive(pending)
                }

                val batchSize = pending.size.coerceAtMost(ChatConstants.MESSAGE_BATCH_MAX_SIZE)
                if (batchSize <= 0) continue
                val batch = pending.take(batchSize)

                cancelActiveGenerationForMerge()

                try {
                    // 每轮生成独立的"已落库"信号，用于打断时判定旧批去留（默认未落库）
                    val commitSignal = TurnCommitSignal()
                    val job = startSendMessage(batch, commitSignal)
                    if (job == null) {

                        typingState.stopTyping()
                        repeat(batchSize) { if (pending.isNotEmpty()) pending.removeAt(0) }
                        waitForMergeWindow = pending.isEmpty()
                        continue
                    }

                    val interrupted = awaitGenerationOrNewMessage(job, pending)
                    if (interrupted) {
                        job.cancel(
                            CancellationException("New message batch started, cancelling stale batch")
                        )
                        runCatching { job.join() }
                        if (commitSignal.isCommitted) {
                            // 旧 job 已把回复落库：这批旧消息已经被回答过，
                            // 必须从 pending 移除，否则下一轮会把它们连同新消息再次作答
                            // → 用户看到"发完一句、很快又发下一句，前一句被回两遍"。
                            repeat(batchSize) { if (pending.isNotEmpty()) pending.removeAt(0) }
                            ChatDebugLog.log(
                                "[ChatGeneration] Interrupted in-flight AI already committed; " +
                                    "dropped $batchSize answered message(s), keeping ${pending.size} new message(s)"
                            )
                        } else {
                            // 旧 job 被干净取消、未产出任何回复：保持原合并重发行为，不丢消息。
                            ChatDebugLog.log(
                                "[ChatGeneration] Interrupted in-flight AI before commit; " +
                                    "merging ${pending.size} pending user message(s)"
                            )
                        }

                        waitForMergeWindow = true
                        continue
                    }

                    repeat(batchSize) { if (pending.isNotEmpty()) pending.removeAt(0) }
                    waitForMergeWindow = false
                } catch (cancelled: CancellationException) {
                    currentCoroutineContext().ensureActive()
                    ChatDebugLog.log(
                        "[ChatGeneration] Child generation cancelled; queue consumer remains active: ${cancelled.message}"
                    )
                    waitForMergeWindow = true
                } catch (e: Exception) {
                    SecureLog.e("ChatGenerationManager", "doSendMessage failed", e)
                    typingState.stopTyping()
                    _events.tryEmit(
                        ChatUiEvent.Error("消息发送失败: ${e.message?.take(50) ?: "未知错误"}")
                    )

                    repeat(batchSize) { if (pending.isNotEmpty()) pending.removeAt(0) }
                    waitForMergeWindow = pending.isEmpty()
                }
            }
        }
    }

    private fun drainTryReceive(pending: MutableList<String>) {
        while (pending.size < ChatConstants.MESSAGE_BATCH_MAX_SIZE) {
            val extra = messageQueue.tryReceive()
            if (extra.isClosed || extra.isFailure) break
            pending.add(extra.getOrThrow())
            _queueDepth.update { maxOf(0, it - 1) }
        }
    }

    private suspend fun drainQueueWithTimeout(pending: MutableList<String>, windowMs: Long) {
        val deadline = System.currentTimeMillis() + windowMs
        drainTryReceive(pending)
        while (pending.size < ChatConstants.MESSAGE_BATCH_MAX_SIZE) {
            val remaining = deadline - System.currentTimeMillis()
            if (remaining <= 0L) break
            val waitMs = minOf(remaining, ChatConstants.MESSAGE_BATCH_POLL_INTERVAL_MS)
            val result = withTimeoutOrNull(waitMs) { messageQueue.receiveCatching() } ?: continue
            if (result.isClosed) return
            if (result.isFailure) continue
            pending.add(result.getOrThrow())
            _queueDepth.update { maxOf(0, it - 1) }
            drainTryReceive(pending)
        }
    }

    private suspend fun awaitGenerationOrNewMessage(
        job: Job,
        pending: MutableList<String>,
    ): Boolean {
        while (job.isActive) {
            val result = withTimeoutOrNull(ChatConstants.MESSAGE_BATCH_POLL_INTERVAL_MS) {
                messageQueue.receiveCatching()
            }
            if (result == null) continue
            if (result.isClosed) {

                job.join()
                return false
            }
            if (result.isFailure) continue
            pending.add(result.getOrThrow())
            _queueDepth.update { maxOf(0, it - 1) }
            drainTryReceive(pending)
            return true
        }

        runCatching { job.join() }
        return false
    }

    private fun cancelActiveGenerationForMerge() {
        val active = turnState.sendMessageJob?.takeIf { it.isActive } ?: return
        active.cancel(CancellationException("New message batch started, cancelling stale batch"))

    }

    /**
     * 该角色是否绑定了可用的专属 API 配置（apiConfigId 有效且配置存在/有 Key）。
     * 与 AiService.resolveConfig(companionId) 的回退判定保持一致：
     * 无全局配置但角色绑定了专属配置时，不应误报「请先配置API」。
     */
    private suspend fun hasCompanionBoundConfig(): Boolean {
        return try {
            val boundId = companionRepository.getCompanionById(companionId)?.apiConfigId
                ?: return false
            if (boundId <= 0L) return false
            val bound = apiConfigRepository.getConfigById(boundId) ?: return false
            bound.apiKey.isNotBlank() || bound.provider == ApiProvider.PARTNER
        } catch (e: Exception) {
            SecureLog.w("ChatGenerationManager", "hasCompanionBoundConfig failed: ${e.message}")
            false
        }
    }

    /**
     * 领域历史 → OpenAI messages JSON（`AgentTurnRequest.historyJson`）。
     *
     * Rust 侧将其作为 messages 基座（system 状态内嵌 + 会话工具结果轮追加），
     * 角色显式映射；TOOL 消息带 `name`，供 gateway 端补 `[工具调用结果]` 前缀。
     */
    private fun serializeHistoryJson(history: List<AiChatMessage>): String {
        val arr = JSONArray()
        for (msg in history) {
            val role = when (msg.role) {
                AiMessageRole.SYSTEM -> "system"
                AiMessageRole.TOOL -> "tool"
                AiMessageRole.USER -> "user"
                AiMessageRole.ASSISTANT -> "assistant"
                null -> if (msg.isFromUser) "user" else "assistant"
            }
            val m = JSONObject().apply {
                put("role", role)
                put("content", msg.content)
            }
            if (role == "tool" && !msg.toolName.isNullOrBlank()) {
                m.put("name", msg.toolName)
            }
            arr.put(m)
        }
        // 第一人称思考（PIPELINE 插件，不改 Rust）：经仓库原版的 _agent_preserve_system
        // 通道（世界书同一条）在底部、紧贴当前 user 消息前注入。插件关闭时 systemRules()
        // 返回空串 → 不注入，messages 与没有该插件时逐字节一致。
        val firstPersonRules = com.yunian.ai.feature.chat.plugin.FirstPersonReasoningRules.systemRules()
        if (firstPersonRules.isNotEmpty()) {
            val inject = JSONObject().apply {
                put("role", "system")
                put("content", firstPersonRules)
                put("_agent_preserve_system", true)
            }
            // 插到当前 user 消息前：最后一条 user 消息是当前输入，规则贴它前面权重最高。
            val lastUserIdx = (arr.length() - 1 downTo 0).firstOrNull { i ->
                arr.optJSONObject(i)?.optString("role") == "user"
            }
            if (lastUserIdx != null) {
                // JSONArray 无 insert：尾部暂存 → 放规则 → 尾部放回。
                val tail = mutableListOf<JSONObject>()
                for (i in arr.length() - 1 downTo lastUserIdx) {
                    tail.add(0, arr.getJSONObject(i))
                    arr.remove(i)
                }
                arr.put(inject)
                tail.forEach { arr.put(it) }
            } else {
                arr.put(inject)
            }
        }
        return arr.toString()
    }

    /** `send_sticker` 事件 extra 中携带的预选表情包 id：`entry_id=123`。 */
    private val stickerEntryIdPattern = Regex("entry_id=(\\d+)")

    /**
     * 运行 Agent 回合；命中确认门（`finishedReason == "confirm_pending"` 且含 `confirm_request`
     * 事件）时，经本地 [_confirmationRequest] / [respondToConfirmation] 询问用户：
     * 批准 → `approveTool` 后重跑；拒绝 → `rejectTool` 后重跑。
     *
     * 世界书在回合前同步注入 Rust（伴侣级优先，回退全局）；注入失败不影响回合。
     */
    private suspend fun runTurnWithConfirmation(
        turnRequest: com.yunian.ai.agent.uniffi.AgentTurnRequest,
        toolHost: com.yunian.ai.agent.host.AgentToolHost,
        companionId: Long,
    ): com.yunian.ai.agent.uniffi.AgentTurnResult? {
        runCatching {
            com.yunian.ai.agent.worldbook.WorldbookRepository(application)
                .syncActiveToRuntime(companionId)
        }.onFailure {
            SecureLog.w("ChatGenerationManager", "worldbook sync failed: ${it.message}")
        }
        var result = runInterruptibleSafe(timeoutMs = TimeoutBudgets.CHAT_VM_API_TIMEOUT_MS * 3) {
            com.yunian.ai.agent.AgentFacade.runTurn(turnRequest, application, companionId, toolHost)
        }
        var guard = 0
        while (result?.finishedReason == "confirm_pending") {
            val pending = result.events.firstOrNull { it.kind == "confirm_request" } ?: break
            guard += 1
            if (guard > 3) break // 防呆：连续确认超限即放弃
            val approved = requestToolConfirmation(pending.text, pending.extra)
            if (approved) {
                com.yunian.ai.agent.AgentFacade.approveTool(application, pending.text, pending.extra)
            } else {
                com.yunian.ai.agent.AgentFacade.rejectTool(application, pending.text, pending.extra)
            }
            result = runInterruptibleSafe(timeoutMs = TimeoutBudgets.CHAT_VM_API_TIMEOUT_MS * 3) {
                com.yunian.ai.agent.AgentFacade.runTurn(turnRequest, application, companionId, toolHost)
            }
        }
        return result
    }

    /**
     * 组装本轮 tools 列表（Rust 记忆工具 + 技能工具 + 可用全局工具），
     * 并构建与 Rust `orchestration_options` 对齐的编排选项。
     *
     * ⚠️ 去重：Rust `memoryToolDefinitions` 已含 recall/save/consolidate_memory，
     * feature:memory 的 `MemoryRecallTools` 也注册同名工具 → 同请求 tools 重名会被
     * DeepSeek 拒绝（HTTP 400: Tool names must be unique）→ Rust 记忆工具优先。
     *
     * 全局（domain）工具的装配**只经折叠点** `AgentFacade.toolDefinitionsFor`：
     * 与 QQ / 微信共用同一条授权判定（按伴侣 × 工具，**不含通道维度**），
     * 本方法不再自行决定工具类别。
     * suspend 化的原因即在此（折叠点要读一次授权存储）。
     */
    private suspend fun buildAgentTools(
        useGlobalTools: Boolean,
    ): Triple<
        List<com.yunian.ai.agent.uniffi.ToolDefinition>,
        List<String>,
        com.yunian.ai.agent.uniffi.PromptOrchestratorOptions,
        > {
        val memoryTools = com.yunian.ai.agent.AgentFacade.memoryToolDefinitions(application)
        val memoryNames = memoryTools.map { it.name }.toSet()
        val skillTools = com.yunian.ai.agent.AgentFacade.skillToolDefinitions()
        val availableTools = ToolRegistry.availableTools(includeAppLocal = true).map { it.name }
            .toMutableList()
            .apply {
                addAll(memoryNames)
                addAll(skillTools.map { it.name })
                addAll(listOf("emit_segmented", "send_sticker", "emit_bubble"))
            }
        val options = com.yunian.ai.agent.uniffi.PromptOrchestratorOptions(
            memoryLimit = 5u,
            skillLimit = 3u,
            includeMemorySkill = true,
            includeSafetyNote = true,
            availableTools = availableTools,
            deviceId = com.yunian.ai.common.DeviceIdProvider.getDeviceId(application),
            timezone = TimeZone.getDefault().id,
            sessionId = null,
            ownerName = null,
            companionNameMapJson = null,
            workingMemoryLimit = 200u,
        )
        // 工具装配的**唯一折叠点**（授权按伴侣 × 工具命中，**不含通道维度**）：
        // 显式允许的工具不再被强制 COMMERCE、显式禁止的工具即使自身没声明也会被强制 COMMERCE；
        // 无决定 / 存储未注册 / 读取失败一律 fail-closed 维持既有类别。
        // includeAppLocal = true 是本机会话的渠道声明，
        // 执行侧 AgentToolHost(allowAppLocalTools = true) 是第二道闸。
        val globalTools = com.yunian.ai.agent.AgentFacade.toolDefinitionsFor(
            companionId = companionId,
            tools = ToolRegistry.availableTools(includeAppLocal = true),
        )
        val tools = buildList {
            addAll(memoryTools)
            addAll(skillTools)
            if (useGlobalTools) addAll(globalTools.filter { it.name !in memoryNames })
        }
        return Triple(tools, availableTools, options)
    }

    /**
     * 同步全局可变配置到 Rust `AgentRuntime`（settings / stickers / credentials 热更新）。
     *
     * Rust 无法解密 SQLite 中的 Tink 加密 API Key（`enc:v4:...`），必须由 Kotlin 解密后经
     * credentials 传入，否则 DeepSeek/CUSTOM 请求会带加密串 → 401。认证分离：`session`/
     * `client_id` 仅用于 PARTNER 内置 API，其他 provider 走 OpenAI 标准 Bearer。
     */
    private suspend fun syncAgentRuntimeConfig(): com.yunian.ai.database.model.ApiConfig? {
        var activeApi: com.yunian.ai.database.model.ApiConfig? = null
        runCatching {
            val role = userRepository?.selectedRole?.value?.name ?: CompanionRole.GIRLFRIEND.name
            // 生图协议：提示词由 Rust 组装，必须经 settings 下发，否则模型不知道能生图却仍被触发
            val imageGenRules = runCatching {
                ImageGenTriggerLogic.systemRules(
                    enabled = appSettingsStore.getImageGenEnabled(),
                    hasKeywordTrigger = appSettingsStore.getImageGenKeywords().isNotEmpty(),
                )
            }.getOrDefault("")
            val settingsJson = com.yunian.ai.agent.AgentFacade.buildSettingsJson(
                role = role,
                imageGenRules = imageGenRules,
            )
            val stickers = com.yunian.ai.agent.sticker.StickerPreferenceFacade
                .availableTagsWithFallback(application)
            val partnerSession = com.yunian.ai.common.RemoteKeyProvider.getPartnerSession(application)
            activeApi = apiConfigRepository.getActiveEnabledConfig()
            val decryptedKey = activeApi?.apiKey?.takeIf { it.isNotBlank() }
            val isPartner = activeApi?.provider == ApiProvider.PARTNER
            val credentialsJson = com.yunian.ai.agent.AgentFacade.buildCredentialsJson(
                sessionToken = if (isPartner) partnerSession?.token else null,
                clientId = if (isPartner) partnerSession?.clientId else null,
                apiKey = decryptedKey,
            )
            com.yunian.ai.agent.AgentFacade.syncRuntimeConfig(
                application,
                settingsJson,
                stickers,
                credentialsJson,
            )
        }.onFailure {
            SecureLog.w("ChatGenerationManager", "syncRuntimeConfig failed: ${it.message}")
        }
        return activeApi
    }

    /**
     * 落一条 Agent 调度日志（provider / model / 起止 / 回合数 / 完成原因 / 工具调用 / 事件流）。
     *
     * 与 [recordAgentTurnAudit] 互补：本方法回答「AI 是怎么跑的」，
     * audit 回答「为什么这样回复」（编排 dry_run）。
     */
    private fun recordAgentDispatchLog(
        sessionId: String,
        startedAtMs: Long,
        agentResult: com.yunian.ai.agent.uniffi.AgentTurnResult,
        toolHost: com.yunian.ai.agent.host.AgentToolHost,
        tools: List<com.yunian.ai.agent.uniffi.ToolDefinition>,
        activeApi: com.yunian.ai.database.model.ApiConfig?,
        querySummary: String,
    ) {
        runCatching {
            com.yunian.ai.agent.AgentFacade.recordDispatchLog(
                context = application,
                companionId = companionId,
                groupId = null,
                sessionId = sessionId,
                dispatchId = sessionId,
                provider = activeApi?.provider?.name ?: "",
                model = activeApi?.model ?: "",
                startedAtMs = startedAtMs,
                completedAtMs = System.currentTimeMillis(),
                roundsUsed = agentResult.roundsUsed.toInt(),
                finishedReason = agentResult.finishedReason,
                error = agentResult.error,
                toolNames = tools.map { it.name },
                toolCalls = toolHost.collectedToolCalls(),
                events = agentResult.events,
                querySummary = querySummary,
            )
        }.onFailure {
            SecureLog.w("ChatGenerationManager", "recordDispatchLog failed: ${it.message}")
        }
    }

    /**
     * 落一条回合审计（编排 dry_run 片段摘要 + 规则指纹 + 工具名快照）。
     *
     * 失败不影响主链路（审计是纯旁路）。
     */
    private fun recordAgentTurnAudit(
        sessionId: String,
        options: com.yunian.ai.agent.uniffi.PromptOrchestratorOptions,
        query: String,
        roundsUsed: Int,
        toolNames: List<String>,
    ) {
        runCatching {
            com.yunian.ai.agent.AgentFacade.recordTurnAudit(
                context = application,
                companionId = companionId,
                groupId = null,
                sessionId = sessionId,
                options = options,
                query = query,
                roundsUsed = roundsUsed,
                toolNames = toolNames,
            )
        }.onFailure {
            SecureLog.w("ChatGenerationManager", "recordTurnAudit failed: ${it.message}")
        }
    }

    private suspend fun startSendMessage(batch: List<String>, commitSignal: TurnCommitSignal): Job? {
        val contentBatch = batch
        val content = if (contentBatch.size == 1) contentBatch[0] else contentBatch.joinToString("\n")

        if (BanManager.isBanned(application)) {
            val banInfo = BanManager.getBanInfo(application)
            val banMsg = if (banInfo.remainingDays > 0) {
                "账号已被封禁（剩余${banInfo.remainingDays}天${banInfo.remainingHours}小时），原因：${banInfo.levelName}。第${banInfo.violationCount}次违规。"
            } else if (banInfo.remainingHours > 0) {
                "账号已被封禁（剩余${banInfo.remainingHours}小时${banInfo.remainingMinutes}分钟），原因：${banInfo.levelName}。第${banInfo.violationCount}次违规。"
            } else {
                "账号已被封禁，原因：${banInfo.levelName}。第${banInfo.violationCount}次违规。请完成安全答题以解除封禁。"
            }
            _events.tryEmit(ChatUiEvent.Error(banMsg))
            return null
        }

        if (apiConfigRepository.getActiveEnabledConfig() == null && !hasCompanionBoundConfig()) {
            for (msg in contentBatch) {
                val inputCheck = runCatching { ContentFilter.checkInput(msg) }.getOrNull()
                if (inputCheck == null) {
                    _events.tryEmit(ChatUiEvent.Error("安全检查异常"))
                    return null
                }
                if (inputCheck.isViolating) {
                    BanManager.recordViolation(application, inputCheck.level)
                    _events.tryEmit(ChatUiEvent.ContentBlocked("内容违规: ${inputCheck.reason}"))
                    return null
                }
            }

            _events.tryEmit(ChatUiEvent.Error("请先配置API：我 → API设置 → 添加密钥"))
            return null
        }

        val pipelineOk = try {
            withTimeoutOrNull(TimeoutBudgets.PIPELINE_EXECUTE_MS) {
                pipeline.execute(MessagePipeline.PipelineInput(rawText = content, companionId = companionId))
            }
        } catch (_: Exception) { null }

        if (pipelineOk != true) {
            _events.tryEmit(ChatUiEvent.ContentBlocked(pipeline.pipelineState.value.error ?: "内容可能违规"))
            return null
        }

        turnState.reset()
        val fetchedHistory = contextResolver.getHistoryForAi(companionId)
            .filterNot { !it.isFromUser && it.content.replace("\u200B", "").isBlank() }
        val settings = chatDetailSettingsStore.getSettings(companionId)
        val job = startAiResponse(
            history = fetchedHistory,
            stickerProbability = settings.stickerProbability,
            userContentForMemory = content,
            batchMessageCount = contentBatch.size,
            ntpTimeEnabled = settings.ntpTimeEnabled,
            onCommitted = commitSignal::markCommitted,
        )
        turnState.sendMessageJob = job
        return job
    }

    private fun startAiResponse(
        history: List<ChatMessage>,
        stickerProbability: Int,
        userContentForMemory: String,
        imagePath: String? = null,
        batchMessageCount: Int = 1,
        ntpTimeEnabled: Boolean = false,
        onCommitted: (() -> Unit)? = null
    ) = scope.launch(Dispatchers.IO + exceptionHandler) {
        val requestStartedAt = System.currentTimeMillis()

        com.yunian.ai.feature.chat.timeline.StreamingReasoningMessagePipeline
            .clearAllStreaming(companionId)
        val pendingTurn = com.yunian.ai.feature.chat.timeline.PendingTurn.start(
            conversation = com.yunian.ai.domain.timeline.ConversationRef(
                conversationId = companionId,
                conversationType = "chat",
            ),
            startedAtMs = requestStartedAt,
        )
        enterLoading()

        var loadingReleased = false
        // 本轮工具调用活动（按执行先后保序，同 id 覆盖 RUNNING→终态）；
        // 轮次结束时持久化为一条 TOOL_ACTIVITY 消息，让过程卡片进入消息流。
        val turnActivityMap = LinkedHashMap<Long, ToolActivity>()
        try {
            val companion = companionRepository.getCompanionById(companionId)
            if (companion == null) {
                _events.tryEmit(ChatUiEvent.Error("系统正在加载伴侣信息，请稍后再试"))
                return@launch
            }
            val imageGenEnabled = runCatching { appSettingsStore.getImageGenEnabled() }.getOrDefault(false)
            val imageGenRules = runCatching {
                ImageGenTriggerLogic.systemRules(
                    enabled = imageGenEnabled,
                    hasKeywordTrigger = appSettingsStore.getImageGenKeywords().isNotEmpty(),
                )
            }.getOrDefault("")
            // 生图协议文本通过「自定义角色指令」并入系统提示词，
            // 从而无需改动 core:domain 接口与 core:network 的提示词装配。
            val aiCompanion = companion.toAiCompanionInfo().let { base ->
                if (imageGenRules.isBlank()) {
                    base
                } else {
                    base.copy(
                        systemPrompt = listOfNotNull(
                            base.systemPrompt?.trim()?.takeIf { it.isNotEmpty() },
                            imageGenRules,
                        ).joinToString("\n\n")
                    )
                }
            }
            latestCompanionInfo = aiCompanion

            val modelHistory = com.yunian.ai.domain.AiDialogueHistoryPolicy
                .sanitizeForModel(history.toAiChatMessages())

            val showReasoning = appSettingsStore.getShowReasoning()

            // ── 文本主路径：全面 Agent 化（Cordis Agent 架构） ──
            // 决策（分段/表情/工具/确认）全部下沉 Rust `AgentFacade.runTurn`；
            // 本方法只负责：运行时配置热同步 → 组装请求 → 消费事件流 → 落库。
            // vision（imagePath != null）仍保留各自分支（图片理解尚未下沉）。
            // D4：本地模型路径已随 `feature:localmodel` 删除，不再存在分支。
            if (imagePath == null) {
                val activeApi = syncAgentRuntimeConfig()
                val conversationId = java.util.UUID.randomUUID().toString()
                // A 方案（用户拍板 2026-09-28）：**始终注入全部工具**。
                // 旧行为由 ChatToolIntent 关键词白名单门控：未命中时模型一个全局工具都拿不到，
                // 真机实测「你有啥工具」只挂载 4 个 memory/skill 工具 → 模型答"你说的工具是指什么呀"，
                // 表现为「工具调用恒定失败」。代价是每轮多约 3~5k prompt tokens（已确认接受）。
                val (agentTools, availableTools, orchestrationOptions) = buildAgentTools(
                    useGlobalTools = true,
                )
                val turnRequest = com.yunian.ai.agent.uniffi.AgentTurnRequest(
                    groupId = null,
                    historyJson = serializeHistoryJson(modelHistory),
                    tools = agentTools,
                    // 单回合 **API 调用轮数**预算（与「气泡条数」护栏严格区分）：
                    // 旧实现误用 BubbleLoopRunner.MAX_BUBBLES(=64 条气泡护栏) 当轮数，
                    // 导致「预算还很充足」的错觉 → 连发失控、内容漂移（Issue #2 症状 2）。
                    maxRounds = ChatConstants.AGENT_MAX_TOOL_ROUNDS.toUInt(),
                    // 首轮用 auto（不用 required）：DeepSeek thinking 模式拒绝 required
                    toolChoice = "auto",
                    stickerProbability = 0u,
                    image = null,
                    systemPrompt = null,
                    companionNameMapJson = null,
                )
                // App 内单聊是本机会话：显式开启本机敏感工具（个人资料）的执行权限。
                val toolHost = com.yunian.ai.agent.host.AgentToolHost(
                    application,
                    allowAppLocalTools = true,
                )
                // Chat 是 Cordis 的独立订阅插件：只订阅本轮随机 streamId，绝不由 ToolHost 直连 UI。
                val projection = ChatToolLifecycleProjection(toolHost.lifecycleStreamId) { snapshots ->
                    val projected = snapshots.mapIndexed { index, item -> item.toToolActivity(index.toLong()) }
                    synchronized(turnActivityMap) {
                        turnActivityMap.clear()
                        projected.forEach { activity -> turnActivityMap[activity.id] = activity }
                    }
                    _toolActivity.value = projected
                }
                val lifecycleHost = ServiceRegistry.get(com.yunian.ai.domain.plugin.PluginHost::class.java)
                val lifecyclePlugin = ChatToolLifecyclePlugin(toolHost.lifecycleStreamId, projection)
                lifecycleHost?.register(lifecyclePlugin)
                val lifecycleLoaded = lifecycleHost?.load(lifecyclePlugin.id, null) ==
                    com.yunian.ai.domain.plugin.PluginLoadResult.Loaded
                val agentResult = try {
                    runTurnWithConfirmation(turnRequest, toolHost, companionId)
                        ?: throw java.util.concurrent.TimeoutException("AI response timeout")
                } finally {
                    if (lifecycleLoaded) lifecycleHost?.unload(lifecyclePlugin.id)
                    lifecycleHost?.unregister(lifecyclePlugin.id)
                }

                recordAgentDispatchLog(
                    sessionId = conversationId,
                    startedAtMs = requestStartedAt,
                    agentResult = agentResult,
                    toolHost = toolHost,
                    tools = agentTools,
                    activeApi = activeApi,
                    querySummary = userContentForMemory,
                )

                val emitted = mutableListOf<String>()
                var stickerEmitted = 0
                val stickerContents = mutableListOf<String>()
                var lastDelivered: AiResponseFinalizer.DeliveredResponse? = null
                // 本轮用量与思考过程（Rust 事件流带出；Agent 路径 HTTP 已下沉，AiService 不再记录）
                var usageInputTokens = 0L
                var usageOutputTokens = 0L
                val reasoningParts = mutableListOf<String>()

                for (event in agentResult.events) {
                    when (event.kind) {
                        "bubble" -> {
                            val text = AgentReplyText.forDisplay(event.text, imageGenEnabled)
                            if (text.isBlank()) continue
                            val toastMsg = com.yunian.ai.domain.AiOperationalMessages.asToastMessage(text)
                            if (toastMsg != null) continue
                            delay(800L + kotlin.random.Random.nextLong(1200L))
                            lastDelivered = responseFinalizer.deliverResponse(
                                aiContent = text,
                                reasoning = reasoningParts.joinToString("\n\n").takeIf { it.isNotBlank() },
                                userContentForMemory = null,
                                pendingTurn = pendingTurn,
                                reasoningStartedAtMs = requestStartedAt,
                                onCommitted = onCommitted,
                            )
                            emitted.add(text)
                        }
                        "sticker" -> {
                            val desc = event.text.trim()
                            if (desc.isBlank()) continue
                            val entryId = stickerEntryIdPattern.find(event.extra)
                                ?.groupValues?.getOrNull(1)?.toLongOrNull()
                            val msgId = runCatching {
                                responseFinalizer.deliverSticker(desc, entryId)
                            }.getOrElse { -1L }
                            if (msgId > 0) {
                                stickerEmitted++
                                stickerContents.add("[$desc]")
                            }
                        }
                        "reasoning" -> {
                            // 「启用思考过程」的数据来源：Rust 已解析 reasoning_content（含工具轮），
                            // 这里汇总后交给 finalizer 落库，由消息列表按 showReasoning 决定展示/折叠。
                            val part = event.text.trim()
                            if (part.isNotEmpty()) reasoningParts.add(part)
                        }
                        "usage" -> {
                            // token 用量：OpenAI 用 prompt_tokens/completion_tokens，
                            // Anthropic 用 input_tokens/output_tokens —— 两种键都兼容。
                            runCatching {
                                val usage = JSONObject(event.text)
                                usageInputTokens += usage.optLong("prompt_tokens", usage.optLong("input_tokens", 0L))
                                usageOutputTokens += usage.optLong("completion_tokens", usage.optLong("output_tokens", 0L))
                            }.onFailure {
                                SecureLog.w("ChatGenerationManager", "parse usage event failed: ${it.message}")
                            }
                        }
                        else -> Unit // status / confirm_request 仅过程态，不落库
                    }
                }

                // 落库 token 用量（迁移后 Agent 路径不再经过 AiService，必须在此记录，
                // 否则「Token 使用统计」页恒为空）。
                if (usageInputTokens > 0 || usageOutputTokens > 0) {
                    runCatching {
                        com.yunian.ai.database.repository.TokenUsageRepository(application)
                            .recordTokenUsage(companionId, usageInputTokens, usageOutputTokens)
                    }.onFailure {
                        SecureLog.w("ChatGenerationManager", "recordTokenUsage failed: ${it.message}")
                    }
                }

                // Rust 侧以 error 结束且未产出任何输出 → 原样上报（消息可见 > 静默失败）。
                // 已产出气泡的 error（例如 max_text/max_rounds 截断）不算失败，继续落地。
                if (agentResult.finishedReason == "error" &&
                    emitted.isEmpty() && stickerEmitted == 0
                ) {
                    StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
                    val rawErr = agentResult.error?.takeIf { it.isNotBlank() } ?: "AI 请求失败，请重试"
                    val errMsg = com.yunian.ai.domain.AiOperationalMessages.asToastMessage("[TOAST]$rawErr")
                        ?: rawErr
                    _events.tryEmit(ChatUiEvent.Error(errMsg.removePrefix("[TOAST]")))
                    SecureLog.e("ChatGenerationManager", "Agent turn failed: $rawErr")
                    return@launch
                }

                // 兜底：Rust 对有正文的回合必然产出 bubble 事件（agent.rs 无需补发）；
                // 仅当事件流完全无输出（异常路径）而 finalText 有内容时，才用 finalText 补救一条，
                // 避免模型意图丢失。正常路径不会走到这里（防重复气泡）。
                if (emitted.isEmpty() && stickerEmitted == 0) {
                    val closing = AgentReplyText.forDisplay(agentResult.finalText.trim(), imageGenEnabled)
                    val closingToast = com.yunian.ai.domain.AiOperationalMessages.asToastMessage(closing)
                    if (closing.isNotBlank() && closingToast == null) {
                        delay(800L + kotlin.random.Random.nextLong(1200L))
                        lastDelivered = responseFinalizer.deliverResponse(
                            aiContent = closing,
                            reasoning = reasoningParts.joinToString("\n\n").takeIf { it.isNotBlank() },
                            userContentForMemory = null,
                            pendingTurn = pendingTurn,
                            reasoningStartedAtMs = requestStartedAt,
                            onCommitted = onCommitted,
                        )
                        emitted.add(closing)
                    }
                }

                if (emitted.isEmpty() && stickerEmitted == 0) {
                    StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
                    _events.tryEmit(ChatUiEvent.Error("AI 未返回有效内容，请重试"))
                    return@launch
                }

                exitLoading()
                loadingReleased = true

                // 生图：Agent 路径输入仍为「用户输入 + 本轮全部气泡文本」
                maybeTriggerImageGeneration(
                    userText = userContentForMemory,
                    // 原始 bubble 可含生图标签；emitted 已清洗，不能用于恢复画面描述。
                    aiText = (listOf(agentResult.finalText) +
                        agentResult.events.filter { it.kind == "bubble" }.map { it.text }).joinToString("\n"),
                    enabled = imageGenEnabled,
                )

                val allBubbleContent = emitted.joinToString("\n")
                    .ifBlank { stickerContents.joinToString("\n") }
                val finalDelivered = lastDelivered ?: AiResponseFinalizer.DeliveredResponse(
                    messageId = -1,
                    aiContent = allBubbleContent,
                    segments = emitted.ifEmpty { stickerContents },
                    userContentForMemory = userContentForMemory,
                    allowFollowUpMessage = true,
                )
                responseFinalizer.afterDeliver(
                    finalDelivered.copy(
                        aiContent = allBubbleContent,
                        segments = emitted.ifEmpty { stickerContents },
                        userContentForMemory = userContentForMemory,
                    )
                )
                persistToolActivities(pendingTurn.turnId.value, turnActivityMap.values.toList())
                recordAgentTurnAudit(
                    sessionId = conversationId,
                    options = orchestrationOptions,
                    query = userContentForMemory,
                    roundsUsed = agentResult.roundsUsed.toInt(),
                    toolNames = availableTools,
                )
                SecureLog.d(
                    "ChatGenerationManager",
                    "Agent turn completed in ${System.currentTimeMillis() - requestStartedAt}ms, " +
                        "reason=${agentResult.finishedReason}, bubbles=${emitted.size}, stickers=$stickerEmitted",
                )
                // 先落地主回复并结束 typing，再串行执行子回合；子任务耗时不能挡住已有回复。
                if (agentResult.finishedReason != "confirm_pending") {
                    try {
                        val coordinator = ServiceRegistry.get(
                            com.yunian.ai.domain.delegation.DelegationCoordinator::class.java
                        )
                        (coordinator as? com.yunian.ai.agent.delegation.DelegationCoordinatorImpl)
                            ?.runPendingDelegations(application)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (exception: Exception) {
                        SecureLog.w("ChatGenerationManager", "runPendingDelegations failed: ${exception.message}")
                    }
                }
                return@launch
            }

            // ── vision 路径：图片理解尚未下沉 Rust，仍走本地 AiService ──
            // 文本路径已在上方 Agent 分支内 `return@launch`，走到这里 imagePath 必非 null
            // （Kotlin 已据该分支完成 smart-cast）。
            // 旧 `useTools`（本地工具循环）与 `streamMessage`（本地流式）分支已随 Cordis Agent
            // 全面接管文本路径而删除 —— 二者在 `imagePath == null` 成立时本就不可达。
            val streamEvents = when {
                else -> {
                    val aiResponse = withTimeoutOrNull(TimeoutBudgets.CHAT_VM_VISION_TIMEOUT_MS) {
                        aiService.sendMessageWithImage(
                            aiCompanion,
                            modelHistory,
                            imagePath,
                            stickerProbability,
                            ntpTimeEnabled,
                        )
                    } ?: throw Exception(application.getString(R.string.api_error_generic))
                    NonStreamingAssistantStreamAdapter.fromCompleted(
                        turnId = pendingTurn.turnId,
                        reasoning = aiResponse.reasoningContent,
                        content = aiResponse.content,
                        startedAtMs = requestStartedAt,
                        completedAtMs = System.currentTimeMillis(),
                    )
                }
            }

            // ADPF：流式回复期间把「主线程 + 当前刷新周期」声明为关键工作负载，
            // 让系统据此提频/摆核。每个流事件后上报一次「上一事件 → 本次」的实耗
            // （频率克制：每事件一次）。仅在支持时创建，否则 perfSession 为 null → 零开销跳过。
            val perfSession = if (PerfBoost.isSupported) {
                PerfBoost.createSession(
                    tag = "chat-stream",
                    targetWorkDurationNanos = PerfBoost.frameIntervalNanos(application),
                    threadIds = PerfBoost.withMainThread(),
                )
            } else {
                null
            }
            val streamResult = try {
                var lastEventNanos = SystemClock.elapsedRealtimeNanos()
                val timedEvents = if (perfSession != null) {
                    streamEvents.onEach {
                        val nowNanos = SystemClock.elapsedRealtimeNanos()
                        perfSession.reportActual(nowNanos - lastEventNanos)
                        lastEventNanos = nowNanos
                    }
                } else {
                    streamEvents
                }
                streamApplier.apply(
                    events = timedEvents,
                    turn = pendingTurn,
                    projectLive = showReasoning,
                    onReasoningSnapshot = { snapshot ->

                        if (EventCommitRules.shouldProjectReasoningLive(showReasoning, snapshot)) {
                            StreamingReasoningMessagePipeline.upsertStreaming(
                                companionId = companionId,
                                turnId = pendingTurn.turnId,
                                text = snapshot,
                                timestamp = pendingTurn.startedAtMs,
                                eventIndex = pendingTurn.streamingReasoningEvent()?.eventIndex,
                                anchorMessageId = pendingTurn.anchorMessageId,
                            )
                        }
                    },
                )
            } finally {
                perfSession?.close()
            }

            if (streamResult.failedMessage != null) {
                StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
                val toastMsg = com.yunian.ai.domain.AiOperationalMessages.asToastMessage(
                    "[TOAST]${streamResult.failedMessage}",
                ) ?: streamResult.failedMessage
                _events.tryEmit(ChatUiEvent.Error(toastMsg))
                return@launch
            }

            val aiContentRaw = streamResult.assistantText
            // vision 路径不接入气泡协议：整段原文交由 `deliverResponse` 走「空行分段」兜底。
            val aiContent = AgentReplyText.forDisplay(aiContentRaw, imageGenEnabled)
            val toastMsg = com.yunian.ai.domain.AiOperationalMessages.asToastMessage(aiContent)
            if (toastMsg != null) {
                StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
                _events.tryEmit(ChatUiEvent.Error(toastMsg))
                return@launch
            }
            if (aiContent.isBlank()) {
                StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
                _events.tryEmit(ChatUiEvent.Error("API返回空内容，请检查模型名是否正确"))
                return@launch
            }

            val reasoningForCommit = streamResult.reasoningText

            val firstDelivered = responseFinalizer.deliverResponse(
                aiContent = aiContent,
                reasoning = reasoningForCommit,
                userContentForMemory = null,
                logMessage = if (batchMessageCount > 1) "AI batch response received (${batchMessageCount} msgs)" else "AI response received",
                pendingTurn = pendingTurn,
                reasoningStartedAtMs = requestStartedAt,
                onCommitted = onCommitted,
                // 始终允许按 AI 自己敲的换行拆分：AI 的回车 = 想换一条，无论协议是否遵守
                // （协议 text 内若塞了换行，同样按回车拆分，防止「全部塞在一起」）。
                allowParagraphSplit = true,
            )

            exitLoading()
            loadingReleased = true

            // AI 生图：关键词/概率触发，独立协程执行，失败绝不影响聊天主流程
            maybeTriggerImageGeneration(
                userText = userContentForMemory,
                aiText = aiContentRaw,
                enabled = imageGenEnabled,
            )

            // 气泡连发（追尾气泡）已由 Rust `AgentFacade.runTurn` 的 bubble 事件流承担：
            // Agent 路径按 `ChatConstants.AGENT_MAX_TOOL_ROUNDS` 轮预算在 Rust 侧连发，
            // 本 vision 分支为单轮图片理解，不产出追尾气泡。
            val allBubbleContent = aiContent
            responseFinalizer.afterDeliver(
                firstDelivered.copy(
                    aiContent = allBubbleContent,
                    segments = listOf(aiContent),
                    userContentForMemory = userContentForMemory,
                )
            )
            // 所有气泡已落库后再写工具卡片：保证其时间戳最大，在消息流中排在本轮最后一条助手消息之后。
            persistToolActivities(pendingTurn.turnId.value, turnActivityMap.values.toList())
            SecureLog.d("ChatGenerationManager", "AI request completed in ${System.currentTimeMillis() - requestStartedAt}ms, chars=${aiContent.length}, bubbles=1")
        } catch (e: CancellationException) {
            StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
            // 被打断（如新一轮消息到达/再生成）：丢弃实时过程态，避免卡片残留在输入框上方。
            // 此处不落库（协程已取消，且避免与新一轮消息顺序错乱）。
            _toolActivity.value = emptyList()
            val cancelReason = e.message ?: ""
            if (!cancelReason.contains("batch started") && !cancelReason.contains("stale")) {
                _events.tryEmit(ChatUiEvent.Error("回复被打断，请重试"))
            }
            throw e
        } catch (e: Exception) {
            StreamingReasoningMessagePipeline.removeStreaming(companionId, pendingTurn.turnId)
            // 出错也要把已发生的工具调用记录进消息流，用户能看到实际执行了什么。
            persistToolActivities(pendingTurn.turnId.value, turnActivityMap.values.toList())
            val rawMessage = e.message ?: "发送失败"
            _events.tryEmit(ChatUiEvent.Error(rawMessage.removePrefix("[TOAST]")))
            SecureLog.e("ChatGenerationManager", "AI response failed", e)
        } finally {
            if (!loadingReleased) {
                exitLoading()
            }
        }
    }


    /** 标准脱敏事件到既有实时/持久 ToolActivity 模型的唯一映射。 */
    private fun ChatToolLifecycleItem.toToolActivity(localId: Long): ToolActivity = ToolActivity(
        id = localId,
        toolName = toolName,
        // 生命周期事件故意不含 args/result；持久卡片也因此不可能泄漏它们。
        argsSummary = "",
        status = when (status) {
            ChatToolLifecycleStatus.RUNNING -> ToolStatus.RUNNING
            ChatToolLifecycleStatus.DONE -> ToolStatus.DONE
            ChatToolLifecycleStatus.FAILED -> ToolStatus.FAILED
        },
        resultSummary = null,
        startedAtMs = startedAtMs,
    )

    private fun enterLoading() {
        if (activeRequests.incrementAndGet() == 1) {
            _isLoading.value = true
            typingState.startTyping()
        }
    }

    private fun exitLoading() {
        val remaining = activeRequests.updateAndGet { current -> (current - 1).coerceAtLeast(0) }
        if (remaining == 0) {
            _isLoading.value = false
            typingState.stopTyping()
        }
    }


    // 注：关键词门控（ChatToolIntent）已随 A 方案停用——工具改为每轮全量注入。
    // 该类保留备用（未来若需做“软提示”而非硬门控可复用其词表）。
    /**
     * 聊天配图触发。
     *
     * 独立协程执行，绝不阻塞或打断聊天主流程；触发条件与执行细节全部收敛在
     * ImageGenService（→ ImageGenCoordinator / ImageGenTriggerLogic，可单测）。
     * 总开关关闭时不读取任何其他配置即返回。
     */
    private fun maybeTriggerImageGeneration(userText: String, aiText: String, enabled: Boolean) {
        if (!enabled) return
        val service = ServiceRegistry.get(ImageGenService::class.java)
        if (service == null) {
            SecureLog.w("ChatGenerationManager", "ImageGenService not registered, skip image gen")
            return
        }
        scope.launch(Dispatchers.IO) {
            try {
                val toastEnabled = runCatching { appSettingsStore.getImageGenToastEnabled() }
                    .getOrDefault(true)
                // 判定/生图/落库/微信镜像全部由 ImageGenService 统一完成；
                // 微信与 QQ 桥接链路复用同一份实现，避免第二套判定逻辑。
                service.generateForReply(
                    companionId = companionId,
                    userText = userText,
                    aiText = aiText,
                    mirrorToWeChat = true,
                    onMessage = { text, isError ->
                        if (toastEnabled) {
                            if (isError) {
                                _events.tryEmit(ChatUiEvent.Error(text))
                            } else {
                                _events.tryEmit(ChatUiEvent.Info(text))
                            }
                        }
                    },
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                SecureLog.e("ChatGenerationManager", "Image gen trigger failed", e)
            } finally {
                // 兜底：任何异常路径都不能让等待动画一直转下去
                ImageGenGenerationStatus.markFinished(companionId)
            }
        }
    }

    /**
     * 把本轮的工具调用活动持久化为**一条** [MessageType.TOOL_ACTIVITY] 消息。
     *
     * - 幂等：同 turnId 先删旧记录（防止重复写入）；
     * - 写入走 [MessageWriteCoordinator]（→ ChatRepository.batchInsertMessages →
     *   MessageCache.appendChatMessage），已打开的聊天页可立即刷新出该卡片；
     * - 该类型不参与 AI 上下文/会话摘要（见 ChatContextResolver / ChatRepository 的过滤）；
     * - 结束后清空实时态 [_toolActivity]，由持久化卡片接管消息流显示。
     */
    private suspend fun persistToolActivities(turnId: String, activities: List<ToolActivity>) {
        try {
            if (activities.isNotEmpty() && turnId.isNotBlank()) {
                runCatching { chatRepository.deleteToolActivitiesForTurn(companionId, turnId) }
                    .onFailure { SecureLog.w("ChatGenerationManager", "Delete stale tool card failed: ${it.message}") }
                messageWriter.enqueueChat(
                    ChatMessage(
                        companionId = companionId,
                        content = ToolActivityCodec.encode(activities),
                        isFromUser = false,
                        timestamp = System.currentTimeMillis(),
                        type = MessageType.TOOL_ACTIVITY,
                        turnId = turnId,
                    )
                )
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            SecureLog.e("ChatGenerationManager", "Persist tool activities failed", e)
        } finally {
            _toolActivity.value = emptyList()
        }
    }

    private fun broadcastWeChatMessage(messageId: Long, finalContent: String? = null) {
        WeChatProactiveSync.enqueue(companionId, messageId, finalContent)
    }

    private fun CompanionEntity.toAiCompanionInfo() = AiCompanionInfo(
        id = id,
        name = name,
        personality = personality,
        age = age,
        backstory = backstory,
        speakingStyle = speakingStyle,
        systemPrompt = systemPrompt
    )

    private fun List<ChatMessage>.toAiChatMessages(): List<AiChatMessage> = map { msg ->
        val base = AiChatMessage(
            isFromUser = msg.isFromUser,
            // 图片消息把画面描述一并带给模型，否则追问「再生成一张」会货不对板
            content = msg.contentForModel(),
            timestamp = msg.timestamp,
            type = if (msg.type == MessageType.IMAGE) AiMessageType.IMAGE else AiMessageType.TEXT,
            companionId = msg.companionId
        )
        com.yunian.ai.domain.AiDialogueHistoryPolicy.normalizeRole(base)
    }

}
