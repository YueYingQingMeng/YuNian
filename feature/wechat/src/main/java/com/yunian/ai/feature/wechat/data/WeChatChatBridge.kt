package com.yunian.ai.feature.wechat.data

import android.content.Context
import com.yunian.ai.common.SecureLog
import com.yunian.ai.common.StickerInfo
import com.yunian.ai.common.StickerManager
import com.yunian.ai.database.AppDatabase
import com.yunian.ai.database.model.ChatMessage
import com.yunian.ai.database.model.MessageType
import com.yunian.ai.database.repository.ChatRepository
import com.yunian.ai.database.repository.CompanionRepository
import com.yunian.ai.domain.ServiceRegistry
import com.yunian.ai.domain.imagegen.ImageGenService
import com.yunian.ai.domain.wechat.WeChatContentKind
import com.yunian.ai.domain.wechat.WeChatContentPart
import com.yunian.ai.domain.wechat.WeChatDialoguePort
import com.yunian.ai.domain.wechat.WeChatDialogueRequest
import com.yunian.ai.domain.wechat.WeChatDialogueResult
import com.yunian.ai.domain.wechat.WeChatInboundMessage
import com.yunian.ai.domain.wechat.WeChatMediaRef
import com.yunian.ai.feature.wechat.WeChatDebugLog
import com.yunian.ai.feature.wechat.data.model.M0
import com.yunian.ai.feature.wechat.data.model.M1
import com.yunian.ai.feature.wechat.data.model.M1Type
import com.yunian.ai.feature.wechat.data.model.M2
import com.yunian.ai.feature.wechat.service.WeChatOutboundDropLog
import com.yunian.ai.feature.wechat.service.WeChatServiceLocator
import com.yunian.ai.wechat.map.WeChatContentCleaner
import com.yunian.ai.wechat.map.WeChatOutboundText
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.util.concurrent.ConcurrentHashMap

internal fun normalizeOutboundText(text: String): String = text.trim()
    .replace(Regex("[\\t\\x0B\\f\\r ]+"), " ")
    .replace(Regex(" *\\n *"), "\n")
    .replace(Regex("\\n{3,}"), "\n\n")

private const val MISSING_COMPANION_HINT_COOLDOWN_MS = 10 * 60 * 1000L

internal fun extractInboundText(message: M0): String? = message.itemList.orEmpty()
    .mapNotNull { item -> item.textItem?.text?.takeIf { it.isNotBlank() } }
    .joinToString("\n")
    .takeIf { it.isNotBlank() }

class WeChatChatBridge(
    private val context: Context,
    private val weChatRepository: WeChatMessageRepository
) {
    private val database = AppDatabase.getDatabase(context)
    private val chatRepository = ServiceRegistry.getOrThrow(ChatRepository::class.java)
    private val companionRepository = CompanionRepository(database.companionDao())
    private val tokenStore = WeChatTokenStore(context)
    private val mappingManager = WeChatUserMappingManager(tokenStore, companionRepository)
    private val dialoguePort: WeChatDialoguePort by lazy {
        ServiceRegistry.get(WeChatDialoguePort::class.java)
            ?: throw IllegalStateException("WeChatDialoguePort not registered in ServiceRegistry")
    }

    private val missingCompanionHintAtMs = ConcurrentHashMap<String, Long>()

    /**
     * 表情包在 App 内聊天显示用的持久拷贝：outbox 的 sticker 缓存目录会被定期清理，
     * 聊天记录必须引用长期目录。优先外部私有目录（getExternalFilesDir，可被 adb 直接
     * 验证），不可用时回退内部 filesDir；同内容文件幂等去重。
     */
    private fun persistStickerForDisplay(localPath: String, stickerName: String): String? =
        runCatching {
            val src = java.io.File(localPath)
            if (!src.exists() || !src.isFile) return null
            val dir = context.getExternalFilesDir("sticker_display")
                ?: java.io.File(context.filesDir, "wechat_sticker_display").apply { mkdirs() }
            val digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(src.readBytes())
                .joinToString("") { "%02x".format(it) }
                .take(16)
            val safeName = stickerName.replace(Regex("[^a-zA-Z0-9._-]"), "_").take(32)
            val ext = src.extension.ifBlank { "png" }
            val out = java.io.File(dir, "${digest}_$safeName.$ext")
            if (!out.exists() || out.length() != src.length()) {
                src.copyTo(out, overwrite = true)
            }
            out.absolutePath
        }.getOrNull()

    suspend fun handleIncomingMessage(message: M0): String? = withContext(Dispatchers.IO) {
        val wechatUserId = message.fromUserId ?: return@withContext null
        val text = extractInboundText(message) ?: return@withContext null
        if (text.isBlank()) return@withContext null

        val companionId = mappingManager.getOrCreateMapping(wechatUserId)
        if (companionId == null) {
            WeChatDebugLog.log("[Bridge] No companion mapping for $wechatUserId, sending setup hint")
            notifyMissingCompanion(wechatUserId)
            return@withContext null
        }

        val inbound = M0WireAdapter.toInbound(message)
            ?: WeChatInboundMessage(
                dedupeKey = "bridge-text-$wechatUserId-${System.currentTimeMillis()}",
                fromUserId = wechatUserId,
                parts = listOf(
                    WeChatContentPart(
                        kind = WeChatContentKind.TEXT,
                        text = text,
                    )
                ),
            )

        val result = dialoguePort.generateReply(
            WeChatDialogueRequest(
                companionId = companionId,
                wechatUserId = wechatUserId,
                inbound = inbound,
            )
        )
        WeChatDebugLog.log("[Bridge] generateReply done companion=$companionId blocked=${result.blocked} reply_len=${result.replyText.length}")

        deliverDialogueResult(
            companionId = companionId,
            wechatUserId = wechatUserId,
            result = result,
            forceDeliver = result.blocked,
            userText = text,
        )
    }

    suspend fun handleTextMessage(wechatUserId: String, text: String): String? = withContext(Dispatchers.IO) {
        val message = M0(
            fromUserId = wechatUserId,
            toUserId = "",
            itemList = listOf(
                M1(type = 1, textItem = M2(text = text))
            )
        )
        handleIncomingMessage(message)
    }

    private suspend fun notifyMissingCompanion(wechatUserId: String) {
        val now = System.currentTimeMillis()
        val last = missingCompanionHintAtMs.putIfAbsent(wechatUserId, now) ?: 0L
        if (last > 0L && now - last < MISSING_COMPANION_HINT_COOLDOWN_MS) {
            missingCompanionHintAtMs[wechatUserId] = last
            return
        }
        missingCompanionHintAtMs[wechatUserId] = now
        if (!tokenStore.getForwardEnabled()) return
        runCatching {
            enqueueAndDrainText(
                companionId = 0L,
                wechatUserId = wechatUserId,
                text = "还没有可用的 AI 伴侣，请先在予念里创建一个伴侣，再回来和我聊天。",
            )
        }.onFailure {
            WeChatDebugLog.log("[Bridge] setup hint enqueue failed: ${it.message}")
        }
    }

    suspend fun handleImageMessage(wechatUserId: String, message: M0): String? = withContext(Dispatchers.IO) {
        try {
            android.util.Log.d("WeChatBridge", "handleImageMessage called for $wechatUserId")

            val companionId = mappingManager.getOrCreateMapping(wechatUserId)
            if (companionId == null) {
                android.util.Log.e("WeChatBridge", "Failed to get/create mapping for $wechatUserId")
                notifyMissingCompanion(wechatUserId)
                return@withContext null
            }

            val imageItem = message.itemList?.firstOrNull { it.type == M1Type.IMAGE.value }?.imageItem
            if (imageItem == null) {
                android.util.Log.w("WeChatBridge", "No image item found in message")
                return@withContext null
            }

            android.util.Log.d("WeChatBridge", "Image item found, cdnInfo present=${imageItem.cdnImg != null}")

            val imagePath = downloadImageFromCdn(message, imageItem)
            if (imagePath == null) {
                android.util.Log.e("WeChatBridge", "Failed to download image, sending fallback response")
                val fallbackResponse = "收到您的图片了！不过暂时无法识别图片内容，可能是因为SDK版本限制。您可以描述一下图片内容，我会尽力帮助您~"
                enqueueAndDrainText(companionId, wechatUserId, fallbackResponse)
                return@withContext fallbackResponse
            }

            android.util.Log.d("WeChatBridge", "Image downloaded successfully: $imagePath")

            val baseInbound = M0WireAdapter.toInbound(message)
            val inbound = if (baseInbound != null) {
                baseInbound.copy(
                    parts = baseInbound.parts.map { part ->
                        if (part.kind == WeChatContentKind.IMAGE) {
                            part.copy(
                                media = (part.media ?: WeChatMediaRef(kind = WeChatContentKind.IMAGE))
                                    .copy(localPath = imagePath),
                            )
                        } else {
                            part
                        }
                    }.ifEmpty {
                        listOf(
                            WeChatContentPart(
                                kind = WeChatContentKind.IMAGE,
                                media = WeChatMediaRef(
                                    kind = WeChatContentKind.IMAGE,
                                    localPath = imagePath,
                                ),
                            )
                        )
                    },
                )
            } else {
                WeChatInboundMessage(
                    dedupeKey = "bridge-image-$wechatUserId-${System.currentTimeMillis()}",
                    fromUserId = wechatUserId,
                    parts = listOf(
                        WeChatContentPart(
                            kind = WeChatContentKind.IMAGE,
                            media = WeChatMediaRef(
                                kind = WeChatContentKind.IMAGE,
                                localPath = imagePath,
                            ),
                        )
                    ),
                )
            }

            val result = dialoguePort.generateReply(
                WeChatDialogueRequest(
                    companionId = companionId,
                    wechatUserId = wechatUserId,
                    inbound = inbound,
                )
            )

            deliverDialogueResult(
                companionId = companionId,
                wechatUserId = wechatUserId,
                result = result,
                forceDeliver = true,
                userText = "",
            )
        } catch (e: Exception) {
            android.util.Log.e("WeChatBridge", "Error in handleImageMessage", e)
            val errorResponse = "图片识别过程中出现错误: ${e.message}. 请稍后重试或发送文字描述。"
            runCatching {
                val companionId = mappingManager.getOrCreateMapping(wechatUserId)
                if (companionId != null) {
                    enqueueAndDrainText(companionId, wechatUserId, errorResponse)
                }
            }
            errorResponse
        }
    }

    private suspend fun deliverDialogueResult(
        companionId: Long,
        wechatUserId: String,
        result: WeChatDialogueResult,
        forceDeliver: Boolean,
        userText: String = "",
    ): String? {
        // 消费判定（P3-3c，**防双发**）：turn != null → **只**消费 events；
        // turn == null → 回退 replyText（与改动前逐字一致）。
        // 两者在 WeChatTurnProjection.of 的 if 上严格互斥，投影结果是本次唯一的文本来源。
        val projection = WeChatTurnProjection.of(result.replyText, result.turn)
        val outcome = deliverDialogueResultInternal(
            companionId = companionId,
            wechatUserId = wechatUserId,
            result = result,
            forceDeliver = forceDeliver,
            projection = projection,
        )
        // 生图：与 App 内聊天同一套判定逻辑（ImageGenService），失败绝不影响聊天主流程。
        // 输入文本与文本出站同源（turn 非空时即 events），顺序与调用时机均未变。
        runCatching {
            generateAndForwardImages(
                companionId = companionId,
                wechatUserId = wechatUserId,
                userText = userText,
                aiText = projection.sourceText,
                shouldForward = outcome.shouldForward,
            )
        }.onFailure { e ->
            SecureLog.e(TAG, "bridge image gen failed: ${e.message}", e)
        }
        return outcome.reply
    }

    private data class DialogueDelivery(
        val reply: String?,
        val shouldForward: Boolean,
    )

    private suspend fun deliverDialogueResultInternal(
        companionId: Long,
        wechatUserId: String,
        result: WeChatDialogueResult,
        forceDeliver: Boolean,
        projection: WeChatTurnProjection,
    ): DialogueDelivery {
        // 唯一文本来源：turn 非空时 = events 连接，否则 = replyText（逐字未变的回退）。
        // ⚠️ 这条路径**不经过** WeChatContentCleaner.clean()（那是主动发送/端口的管线），
        // 因此思考块必须在这里先剥掉——否则事件文本里的 <thinking>… 会被当正文发到微信
        // （Issue #2 症状 1 的通道泄漏面）。只剥思考、不动换行，气泡拆分语义不变。
        val aiResponseText = WeChatContentCleaner.stripThinking(projection.sourceText)
        val aiMessageId = result.assistantMessageId ?: 0L

        if (aiMessageId > 0 && aiResponseText.isNotBlank()) {
            val processed = runCatching { extractStickerTags(aiResponseText, projection.eventStickerLabels) }
                .getOrDefault(Pair(aiResponseText, emptyList<StickerInfo>()))
            if (processed.first.isNotEmpty() && processed.first != aiResponseText) {
                val messageIds = result.assistantMessageIds.ifEmpty { listOf(aiMessageId) }

                val segments = if (messageIds.size <= 1) {
                    listOf(processed.first)
                } else {
                    messageIds.indices.map { index ->
                        val text = processed.first
                        if (index == messageIds.lastIndex) text else ""
                    }
                }
                messageIds.zip(segments).forEach { (messageId, segment) ->
                    if (segment.isNotBlank()) {
                        chatRepository.updateMessageContent(messageId, segment)
                    }
                }
            }
        }

        if (result.blocked && !forceDeliver) {
            return DialogueDelivery(aiResponseText.ifBlank { null }, shouldForward = false)
        }

        val forwardEnabled = tokenStore.getForwardEnabled()
        val shouldForward = forceDeliver || forwardEnabled
        WeChatDebugLog.log("[Bridge] deliverDialogueResult forwardEnabled=$forwardEnabled forceDeliver=$forceDeliver shouldForward=$shouldForward text_len=${aiResponseText.length}")
        if (!shouldForward || aiResponseText.isBlank()) {
            WeChatDebugLog.log("[Bridge] deliverDialogueResult SKIPPED (forward=$shouldForward blank=${aiResponseText.isBlank()})")
            return DialogueDelivery(aiResponseText.ifBlank { null }, shouldForward = false)
        }

        val (cleanText, stickers) = runCatching {
            extractStickerTags(aiResponseText, projection.eventStickerLabels)
        }.getOrDefault(Pair(aiResponseText, emptyList<StickerInfo>()))

        android.util.Log.d(
            "WeChatBridge",
            "Forward: cleanText length=${cleanText.length}, stickers count=${stickers.size}, blocked=${result.blocked}",
        )

        val isTextMeaningful = cleanText.isNotBlank() &&
            !cleanText.all { it.isWhitespace() } &&
            cleanText != "\u200B"

        if (stickers.isNotEmpty() && !isTextMeaningful) {
            android.util.Log.d("WeChatBridge", "Only stickers, no meaningful text to send")
        } else if (isTextMeaningful) {
            when (val prepared = WeChatOutboundText.prepare(cleanText)) {
                is WeChatOutboundText.Prepared.Sendable -> {
                    val contextToken = weChatRepository.getContextToken(wechatUserId)
                    val rootId = weChatRepository.enqueueTextOutbound(
                        companionId = companionId,
                        wechatUserId = wechatUserId,
                        text = prepared.text,
                        contextToken = contextToken,
                        sourceMessageId = aiMessageId.takeIf { it > 0 },
                    )
                    val sent = weChatRepository.drainOutbox()
                    WeChatDebugLog.log("[Bridge] Text delivered rootId=$rootId drainSent=$sent")
                    android.util.Log.d(
                        "WeChatBridge",
                        "Text enqueued rootId=$rootId drainSent=$sent",
                    )
                }

                is WeChatOutboundText.Prepared.Dropped -> {
                    // 这一轮确实没有可发送的文本：不伪造内容、不强行发送，但必须留证据。
                    // 旧实现在这里没有任何分支 —— 回复被清洗成空串后完全静默。
                    WeChatOutboundDropLog.record(
                        reason = prepared.reason.wireName,
                        cleanedLength = prepared.cleanedLength,
                        strippedLength = prepared.strippedLength,
                        stickerCount = stickers.size,
                    )
                    android.util.Log.w(
                        "WeChatBridge",
                        "No sendable text this round: reason=" + prepared.reason.wireName +
                            " cleanedLen=" + prepared.cleanedLength +
                            " stickers=" + stickers.size,
                    )
                }
            }
        }

        if (stickers.isNotEmpty()) {
            val contextToken = weChatRepository.getContextToken(wechatUserId)
            var stickerEnqueued = false
            stickers.forEachIndexed { index, sticker ->
                runCatching {
                    android.util.Log.i(
                        "WeChatBridge",
                        "Enqueue sticker[$index]: name=${sticker.name}, path=${sticker.path}",
                    )
                    val rootId = weChatRepository.enqueueStickerOutbound(
                        companionId = companionId,
                        wechatUserId = wechatUserId,
                        sticker = sticker,
                        contextToken = contextToken,
                        sourceMessageId = aiMessageId.takeIf { it > 0 },
                    )
                    if (rootId != null) {
                        stickerEnqueued = true
                        android.util.Log.i(
                            "WeChatBridge",
                            "Sticker[$index] enqueued rootId=$rootId",
                        )
                        // 两端一致：表情包同时写入 App 内聊天记录（assistant 图片消息），
                        // 否则 App 端只剩剥掉标签后的文字，微信端却收得到表情包。
                        runCatching {
                            val material = WeChatStickerMaterializer.materialize(context, sticker)
                            if (material == null) {
                                android.util.Log.w("WeChatBridge", "Sticker[$index] chat-mirror: materialize null")
                            } else {
                                val displayPath = persistStickerForDisplay(material.localPath, sticker.name)
                                if (displayPath == null) {
                                    android.util.Log.w(
                                        "WeChatBridge",
                                        "Sticker[$index] chat-mirror: persist null src=${material.localPath}",
                                    )
                                } else {
                                    val mirroredId = chatRepository.sendMessage(
                                        ChatMessage(
                                            companionId = companionId,
                                            // 必须用系统保留标签「[图片]」：[xxx] 会被 ChatListItem
                                            // 识别为表情包标签去 StickerManager 查找（查无此包 → 异常图标），
                                            // 「图片」在 systemTags 名单里才能正确走 ImageMessage 渲染。
                                            content = "[图片]",
                                            isFromUser = false,
                                            timestamp = System.currentTimeMillis(),
                                            type = MessageType.IMAGE,
                                            linkString = displayPath,
                                            searchContent = sticker.description ?: sticker.name,
                                        ),
                                    )
                                    android.util.Log.i(
                                        "WeChatBridge",
                                        "Sticker[$index] chat-mirror ok id=$mirroredId companionId=$companionId path=$displayPath",
                                    )
                                }
                            }
                        }.onFailure { e ->
                            android.util.Log.w("WeChatBridge", "Sticker[$index] chat-mirror failed: ${e.message}")
                        }
                    } else {
                        android.util.Log.w(
                            "WeChatBridge",
                            "Sticker[$index] materialize/enqueue failed: ${sticker.name}",
                        )
                    }
                }.onFailure { e ->
                    android.util.Log.e("WeChatBridge", "Error enqueue sticker[$index]", e)
                }
            }
            if (stickerEnqueued) {
                val sent = weChatRepository.drainOutbox()
                android.util.Log.i("WeChatBridge", "Sticker drainSent=$sent")
            }
        }

        return DialogueDelivery(aiResponseText.ifBlank { null }, shouldForward = true)
    }

    /**
     * 桥接链路的生图：判定逻辑与 App 内完全一致（复用 [ImageGenService]），
     * 落库由服务完成，这里只负责把同一张图排进微信出站队列。
     *
     * context_token 缺失/过期时 [WeChatOutboxCoordinator] 会判死并记日志，这是 iLink
     * 协议的正常限制（只能回复对方先发来的消息），此处不重试、不绕过。
     */
    private suspend fun generateAndForwardImages(
        companionId: Long,
        wechatUserId: String,
        userText: String,
        aiText: String,
        shouldForward: Boolean,
    ) {
        if (aiText.isBlank()) return
        val service = ServiceRegistry.get(ImageGenService::class.java)
        if (service == null) {
            SecureLog.i(TAG, "image gen skipped: ImageGenService not registered")
            return
        }
        val images = service.generateForReply(
            companionId = companionId,
            userText = userText,
            aiText = aiText,
            // 微信桥接自己负责发送，不能让服务再镜像一次（否则重复发送）
            mirrorToWeChat = false,
        )
        if (images.isEmpty()) return
        SecureLog.i(TAG, "bridge image gen done companionId=$companionId count=${images.size}")
        if (!shouldForward) {
            SecureLog.i(TAG, "image not forwarded: forward disabled companionId=$companionId")
            return
        }
        val contextToken = weChatRepository.getContextToken(wechatUserId)
        var enqueued = 0
        images.forEach { image ->
            if (image.filePath.isBlank()) return@forEach
            runCatching {
                weChatRepository.enqueueImageOutbound(
                    companionId = companionId,
                    wechatUserId = wechatUserId,
                    localPath = image.filePath,
                    fileName = null,
                    description = null,
                    contextToken = contextToken,
                    sourceMessageId = image.messageId.takeIf { it > 0 },
                )
                enqueued++
            }.onFailure { e ->
                SecureLog.w(TAG, "image enqueue failed path=${image.filePath.take(80)}: ${e.message}")
            }
        }
        if (enqueued > 0) {
            val sent = weChatRepository.drainOutbox()
            SecureLog.i(TAG, "image outbound drained enqueued=$enqueued sent=$sent user=${wechatUserId.take(3)}***")
        }
    }

    private suspend fun enqueueAndDrainText(
        companionId: Long,
        wechatUserId: String,
        text: String,
        sourceMessageId: Long? = null,
    ): Int {
        if (text.isBlank()) return 0
        val contextToken = weChatRepository.getContextToken(wechatUserId)
        weChatRepository.enqueueTextOutbound(
            companionId = companionId,
            wechatUserId = wechatUserId,
            text = text,
            contextToken = contextToken,
            sourceMessageId = sourceMessageId,
        )
        return weChatRepository.drainOutbox()
    }

    private suspend fun downloadImageFromCdn(message: M0, imageItem: com.yunian.ai.feature.wechat.data.model.M3): String? {
        return try {
            val sdkClient = WeChatServiceLocator.sdkClientManager(context)
            val tempFile = File(context.cacheDir, "wechat_img_${System.currentTimeMillis()}.jpg")

            when {
                imageItem.cdnImg != null -> {
                    android.util.Log.d("WeChatBridge", "Attempting to download image via SDK CDN")
                    runCatching {
                        val downloadedBytes = sdkClient.downloadMedia(
                            com.yunian.ai.wechat.ilink.IlinkCdnMedia(
                                encryptQueryParam = imageItem.cdnImg.encryptQueryParam,
                                aesKey = imageItem.cdnImg.aesKey,
                            ),
                        )
                        if (downloadedBytes != null && downloadedBytes.isNotEmpty()) {
                            tempFile.writeBytes(downloadedBytes)
                            tempFile.absolutePath
                        } else {
                            android.util.Log.w("WeChatBridge", "SDK returned empty bytes for image")
                            null
                        }
                    }.getOrNull()
                }
                else -> {
                    android.util.Log.w("WeChatBridge", "No CDN info available for image")
                    null
                }
            }
        } catch (e: Exception) {
            android.util.Log.e("WeChatBridge", "Failed to download image from CDN", e)
            null
        }
    }

    fun close() {
    }

    private companion object {
        private const val TAG = "WeChatChatBridge"
    }

    /**
     * 剥离表情标签并解析本轮要发的表情。
     *
     * @param pinnedStickerLabels **结构化快照给出的表情标签**（P3-3c）：
     *  - `null`（turn == null，回退 replyText）→ 旧行为逐字不变：表情标签由文本正则发现，
     *    且文本里找不到标签时还有「按 StickerManager 规则在文本里找描述」的随机兜底；
     *  - 非 `null`（turn != null）→ **表情只认事件**：文本里的 `[标签]` 照旧被剥离
     *    （纯文本清洗，与改动前一致），但发哪个表情由该列表决定，文本不再参与表情发现，
     *    随机兜底也必须关闭——否则会发出模型从未要求过的表情。
     *
     * 返回 (清洗后的文本, 表情列表)；文本清洗 / outbox / 图片镜像路径均未改动。
     */
    private fun extractStickerTags(
        text: String,
        pinnedStickerLabels: List<String>? = null,
    ): Pair<String, List<StickerInfo>> {
        val stickerManager = StickerManager.getInstance(context)
        val systemTags = setOf("语音", "图片", "视频", "文件", "位置", "红包", "转账")
        val stickerRegex = Regex("\\[([^\\[\\]]+?)\\]")
        val fileNamePattern = Regex("^[a-zA-Z0-9_\\-]+\\.(png|jpg|jpeg|gif|webp)$", RegexOption.IGNORE_CASE)
        val matches = stickerRegex.findAll(text).toList()

        val stickers = mutableListOf<StickerInfo>()
        // turn != null：表情由 Sticker 事件定夺，**不再从文本正则抠**。
        // 标签 → StickerInfo 仍走既有 StickerManager 匹配（精确优先，再模糊），
        // 匹配不到就什么都不发（与旧行为里「标签匹配不到」的结局一致）。
        pinnedStickerLabels?.forEach { label ->
            val sticker = stickerManager.findStickerByDescriptionExact(label)
                ?: stickerManager.findStickerByDescription(label)
            if (sticker != null && stickers.none { it.name == sticker.name }) {
                stickers.add(sticker)
            }
        }
        val sentStickerDescs = mutableSetOf<String>()
        var cleanText = text

        val rolePrefixRegex = Regex("(?m)^\\s*\\[(?:角色\\d+|[^\\[\\]]+?)\\]\\s*")
        cleanText = rolePrefixRegex.replace(cleanText, "")

        val thinkRegex = Regex("(?is)<think[^>]*>[\\s\\S]*?</think\\s*>")
        cleanText = thinkRegex.replace(cleanText, "")

        val encRegex = Regex("(?m)^enc:\\S+$")
        cleanText = encRegex.replace(cleanText, "")

        for (match in matches) {
            val description = match.groupValues[1].trim()
            if (description in systemTags) continue

            var found = false
            val sticker = stickerManager.findStickerByDescriptionExact(description)
                ?: stickerManager.findStickerByDescription(description)
            if (pinnedStickerLabels == null) {
                if (sticker != null) {
                    if (stickers.none { it.name == sticker.name }) {
                        stickers.add(sticker)
                        found = true
                    } else {
                        android.util.Log.d("WeChatBridge", "Duplicate sticker skipped: ${sticker.name}")
                    }
                }
            } else {
                // 表情已由事件定夺：文本里的这个 [标签] 只做清洗，不参与表情发现；
                // 但匹配成功就置 found，免得误报 unmatched。
                found = sticker != null
            }
            sentStickerDescs.add(description)
            sticker?.description?.let { sentStickerDescs.add(it) }
            cleanText = cleanText.replace(match.value, "")
            if (!found && fileNamePattern.matches(description)) {
                android.util.Log.d("WeChatBridge", "Removed unmatched sticker file tag: [$description]")
            }
            if (!found && !fileNamePattern.matches(description)) {
                android.util.Log.w("WeChatBridge", "Removed unmatched sticker tag: [$description]")
            }
        }

        for (desc in sentStickerDescs) {
            if (desc.length >= 2 && cleanText.contains(desc)) {
                cleanText = cleanText.replace(desc, "")
                android.util.Log.d("WeChatBridge", "Removed residual sticker desc from text: $desc")
            }
        }

        cleanText = cleanText.replace("]", "").replace("[", "")
        cleanText = Regex("\\bsticker_\\w+\\.png\\b", RegexOption.IGNORE_CASE).replace(cleanText, "")

        for (sticker in stickers) {
            val desc = sticker.description
            if (!desc.isNullOrBlank() && desc.length >= 2 && cleanText.contains(desc)) {
                cleanText = cleanText.replace(desc, "")
                android.util.Log.d("WeChatBridge", "Removed sticker desc from text (extra): $desc")
            }
            val name = sticker.name
            if (name.length >= 2 && cleanText.contains(name)) {
                cleanText = cleanText.replace(name, "")
                android.util.Log.d("WeChatBridge", "Removed sticker name from text (extra): $name")
            }
        }

        var result = cleanText.trim()
            .replace(Regex("\\r\\n|\\r|\\n+"), "，")
            .replace(Regex("，{2,}"), "，")
            .trim()
            .trimStart('，', ',', '.', '。', ' ')

        if (stickers.isEmpty() && pinnedStickerLabels == null) {
            // 旧兜底：文本里没有任何标签时，按 StickerManager 规则在文本里找描述随机配一个表情。
            // 只在回退路径（turn == null）生效——有结构化快照时表情以事件为准，绝不凭空猜一个。
            val allRules = stickerManager.getAllRules()
            if (allRules.isNotEmpty()) {
                val matchedStickers = mutableListOf<Pair<StickerInfo, String>>()
                for (rule in allRules.shuffled()) {
                    val desc = rule.description
                    if (desc.length >= 2 && text.contains(desc)) {
                        val sticker = stickerManager.findStickerByDescription(desc)
                        if (sticker != null) matchedStickers.add(sticker to desc)
                    }
                }
                if (matchedStickers.isNotEmpty()) {
                    val (picked, matchedDesc) = matchedStickers.random()
                    if (stickers.none { it.name == picked.name }) {
                        stickers.add(picked)
                        android.util.Log.d("WeChatBridge", "Matched sticker from text: ${picked.name}")
                    } else {
                        android.util.Log.d("WeChatBridge", "Duplicate sticker skipped: ${picked.name}")
                    }
                    val descToRemove = if (result.contains(matchedDesc)) {
                        matchedDesc
                    } else {
                        (picked.description ?: picked.name)
                    }
                    if (descToRemove.length >= 2) {
                        result = result.replace(descToRemove, "")
                        android.util.Log.d(
                            "WeChatBridge",
                            "Removed sticker desc from text: $descToRemove (matched: $matchedDesc)",
                        )
                    }
                    result = removeLocalRepetition(result)
                }
            }
        }

        result = removeLocalRepetition(result)
        return result to stickers
    }

    private fun removeLocalRepetition(text: String): String {
        if (text.length < 4) return text
        var result = text

        for (len in result.length / 2 downTo 2) {
            val suffix = result.takeLast(len)
            val beforeSuffix = result.dropLast(len)
            if (beforeSuffix.endsWith(suffix)) {
                result = beforeSuffix
                return removeLocalRepetition(result)
            }
        }

        for (len in result.length / 2 downTo 4) {
            val suffix = result.takeLast(len)
            val beforeSuffix = result.dropLast(len)
            if (beforeSuffix.contains(suffix)) {
                result = beforeSuffix
                return removeLocalRepetition(result)
            }
            val suffixCleaned = suffix.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
            val beforeSuffixCleaned = beforeSuffix.trimEnd('。', '！', '？', '，', '.', '!', '?', ',', ' ')
            if (suffixCleaned.length >= 4 && beforeSuffixCleaned.endsWith(suffixCleaned)) {
                result = beforeSuffix
                return removeLocalRepetition(result)
            }
        }

        val sentenceDelimiters = Regex("(?<=[。！？.!?])")
        val sentences = result.split(sentenceDelimiters)
        if (sentences.size >= 2) {
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
                    deduped.add(trimmed)
                }
            }
            val joined = deduped.joinToString("")
            if (joined.length < result.length) {
                result = joined
                return removeLocalRepetition(result)
            }
        }

        return result
    }
}
