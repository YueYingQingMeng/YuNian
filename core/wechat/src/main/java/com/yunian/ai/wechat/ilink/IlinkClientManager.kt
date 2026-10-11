package com.yunian.ai.wechat.ilink

import android.util.Log
import com.yunian.ai.common.concurrent.AppDispatchers
import com.yunian.ai.wechat.wire.WireWeChatMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.IOException
import java.io.InterruptedIOException
import java.net.URLEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 微信连接管理器：纯 HTTP 实现的官方 iLink Bot 协议（@tencent-weixin/openclaw-weixin）。
 *
 * 重构背景：lith0924 SDK（2.3.3）请求头停留在 1.x 协议，微信服务端对旧协议参数返回
 * ret=0 但静默不投递（SDK issue #15），且 SDK 内部 cursor/context/心跳黑盒不可观测。
 * 本实现删除全部 SDK 依赖：
 * - 登录：get_bot_qrcode + get_qrcode_status 长轮询（纯 HTTP，无客户端对象）
 * - 收消息：getupdates 长轮询，get_updates_buf 即时持久化到 [IlinkSessionStore]
 * - 发消息：sendmessage 直发，context_token 缺失时按官方行为拒绝发送
 * - 媒体：getuploadurl + CDN AES-128-ECB 加密上传 / 下载解密
 * - errcode=-14 抛 [IlinkSessionExpiredException]，由上层识别为"需重新扫码登录"
 *
 * 内部状态仅剩：登录会话（bot_token/ilink_bot_id/baseUrl，持久化在 sessionStore）、
 * 二维码登录的内存状态、get_updates_buf（存取走 sessionStore）。
 */
class IlinkClientManager(
    private val sessionStore: IlinkSessionStore,
) {

    private val api = IlinkHttpApi()
    private val cdn = IlinkCdnClient()
    private val json = Json { ignoreUnknownKeys = true }
    private val random = SecureRandom()

    /** 每进程一次的 channel_version 异步刷新门禁（fire-and-forget，不阻塞登录/收发路径）。 */
    private val channelVersionRefreshTriggered = AtomicBoolean(false)
    private val channelVersionRefreshScope = CoroutineScope(SupervisorJob() + AppDispatchers.io)

    /** 二维码登录的内存状态（不再有 SDK 客户端对象；轮询 host 可随 IDC 重定向切换）。 */
    @Volatile
    private var pendingLogin: PendingQrLogin? = null

    private data class PendingQrLogin(
        val qrcode: String,
        val displayContent: String,
        val startedAtMs: Long,
        val currentApiBaseUrl: String,
        val refreshCount: Int,
    )

    // -----------------------------------------------------------------------
    // 登录（对标 login-qr.ts）
    // -----------------------------------------------------------------------

    /** 获取登录二维码（GET get_bot_qrcode，5s 超时）。 */
    suspend fun startLogin(): IlinkQrCode = withContext(Dispatchers.IO) {
        ensureChannelVersionRefreshed()
        val raw = api.getRaw(DEFAULT_BASE_URL, IlinkHttpApi.GET_BOT_QRCODE_ENDPOINT, IlinkHttpApi.GET_QRCODE_TIMEOUT_MS)
        val dto = json.decodeFromString<IlinkQrCodeRespDto>(raw)
        val qrcode = dto.qrcode?.takeIf { it.isNotBlank() }
            ?: throw IllegalStateException("获取登录二维码失败：响应缺少 qrcode")
        val displayContent = dto.qrcodeImgContent?.takeIf { it.isNotBlank() } ?: qrcode
        pendingLogin = PendingQrLogin(
            qrcode = qrcode,
            displayContent = displayContent,
            startedAtMs = System.currentTimeMillis(),
            currentApiBaseUrl = DEFAULT_BASE_URL,
            refreshCount = 0,
        )
        IlinkQrCode(statusToken = qrcode, displayContent = displayContent)
    }

    /**
     * 轮询一次二维码状态（内部执行一次 get_qrcode_status 长轮询，35s 超时）。
     * - wait → 抛 IllegalStateException("等待扫码")；scaned → 抛("等待手机确认")，沿用原语义；
     * - expired → 自动刷新二维码（≤3 次），刷新后继续在同一调用内长轮询；
     * - scaned_but_redirect → 轮询 host 切到 https://{redirect_host} 后继续；
     * - confirmed → 返回 [IlinkAccount] 并持久化到 sessionStore；
     * - 轮询中的网络错误/网关错误一律当作 wait 对待（对标官方 pollQRStatus）。
     */
    suspend fun pollLoginStatus(): IlinkAccount = withContext(Dispatchers.IO) {
        var login = pendingLogin
            ?: throw IllegalStateException("请先获取微信登录二维码")

        if (System.currentTimeMillis() - login.startedAtMs > LOGIN_TIMEOUT_MS) {
            pendingLogin = null
            throw IllegalStateException("登录超时，请重新扫码")
        }

        while (true) {
            val status = pollQrStatusOnce(login)
            when (status.status) {
                QR_STATUS_WAIT -> throw IllegalStateException("等待扫码")
                QR_STATUS_SCANED -> throw IllegalStateException("等待手机确认")
                QR_STATUS_EXPIRED -> {
                    if (login.refreshCount + 1 > MAX_QR_REFRESH_COUNT) {
                        pendingLogin = null
                        throw IllegalStateException(
                            "二维码已过期（已刷新 $MAX_QR_REFRESH_COUNT 次），请重新发起登录",
                        )
                    }
                    login = refreshQrCode(login)
                    pendingLogin = login
                    Log.i(TAG, "QR expired, refreshed (${login.refreshCount}/$MAX_QR_REFRESH_COUNT)")
                }
                QR_STATUS_REDIRECT -> {
                    val host = status.redirectHost
                    if (!host.isNullOrBlank()) {
                        login = login.copy(currentApiBaseUrl = "https://$host")
                        pendingLogin = login
                        Log.i(TAG, "IDC redirect: polling host switched to $host")
                    }
                    // redirect_host 缺失时沿用当前 host 继续（对标官方）
                }
                QR_STATUS_CONFIRMED -> {
                    val botId = status.ilinkBotId
                    if (botId.isNullOrBlank()) {
                        pendingLogin = null
                        throw IllegalStateException("登录失败：服务器未返回 ilink_bot_id")
                    }
                    pendingLogin = null
                    val account = IlinkAccount(
                        botToken = status.botToken.orEmpty(),
                        ilinkBotId = botId,
                        ilinkUserId = status.ilinkUserId.orEmpty(),
                        baseUrl = status.baseurl?.takeIf { it.isNotBlank() } ?: DEFAULT_BASE_URL,
                    )
                    sessionStore.saveSessionAccount(account)
                    Log.i(TAG, "Login confirmed: ilink_bot_id=$botId baseurl=${account.baseUrl}")
                    return@withContext account
                }
                else -> throw IllegalStateException("等待登录")
            }
        }
        // 上面循环只会通过 return / throw 退出；此行仅为满足编译器控制流分析
        throw IllegalStateException("登录轮询异常退出")
    }

    /** 单次二维码状态长轮询；任何网络/解析错误都降级为 wait（对标官方 pollQRStatus）。 */
    private fun pollQrStatusOnce(login: PendingQrLogin): IlinkQrStatusRespDto {
        return try {
            val endpoint =
                "${IlinkHttpApi.GET_QRCODE_STATUS_ENDPOINT}?qrcode=${URLEncoder.encode(login.qrcode, "UTF-8")}"
            val raw = api.getRaw(login.currentApiBaseUrl, endpoint, IlinkHttpApi.QR_LONG_POLL_TIMEOUT_MS)
            json.decodeFromString<IlinkQrStatusRespDto>(raw)
        } catch (e: Exception) {
            Log.d(TAG, "pollQrStatus network/gateway error treated as wait: ${e.message}")
            IlinkQrStatusRespDto(status = QR_STATUS_WAIT)
        }
    }

    /** 刷新二维码：重新 get_bot_qrcode，重置 startedAt 与轮询 host。失败时抛出原异常。 */
    private suspend fun refreshQrCode(login: PendingQrLogin): PendingQrLogin =
        withContext(Dispatchers.IO) {
            val raw = api.getRaw(DEFAULT_BASE_URL, IlinkHttpApi.GET_BOT_QRCODE_ENDPOINT, IlinkHttpApi.GET_QRCODE_TIMEOUT_MS)
            val dto = json.decodeFromString<IlinkQrCodeRespDto>(raw)
            val qrcode = dto.qrcode?.takeIf { it.isNotBlank() }
                ?: throw IllegalStateException("刷新二维码失败：响应缺少 qrcode")
            login.copy(
                qrcode = qrcode,
                displayContent = dto.qrcodeImgContent?.takeIf { it.isNotBlank() } ?: qrcode,
                startedAtMs = System.currentTimeMillis(),
                currentApiBaseUrl = DEFAULT_BASE_URL,
                refreshCount = login.refreshCount + 1,
            )
        }

    // -----------------------------------------------------------------------
    // 收消息（对标 monitor.ts）
    // -----------------------------------------------------------------------

    /**
     * 长轮询拉取新消息。
     * - 客户端超时（长轮询正常超时）→ 返回空列表（等价 ret=0、msgs 空、buf 不变）；
     * - ret/errcode != 0 → 抛错；-14 抛 [IlinkSessionExpiredException]（上层据此暂停收发并要求重新扫码）；
     * - get_updates_buf 非空 → 立即持久化（sessionStore.saveCursor）。
     */
    suspend fun getUpdates(): List<WireWeChatMessage> = withContext(Dispatchers.IO) {
        ensureChannelVersionRefreshed()
        val account = sessionStore.getSessionAccount()
            ?: throw IllegalStateException("未登录微信")

        val body = IlinkWireJson
            .buildGetUpdatesBody(getUpdatesBuf = sessionStore.getCursor(), baseInfo = api.buildBaseInfo())
            .toString()

        val raw = try {
            api.postJson(
                baseUrl = account.baseUrl,
                endpoint = IlinkHttpApi.GET_UPDATES_ENDPOINT,
                body = body,
                botToken = account.botToken,
                timeoutMs = IlinkHttpApi.DEFAULT_LONG_POLL_TIMEOUT_MS,
            )
        } catch (e: InterruptedIOException) {
            // 客户端超时是长轮询的正常路径，返回空结果供调用方直接重试
            Log.d(TAG, "getUpdates client timeout, returning empty")
            return@withContext emptyList()
        }

        val resp = IlinkWireJson.parseUpdatesResponse(raw)
        api.checkApiError(resp.ret, resp.errcode, resp.errmsg)

        resp.getUpdatesBuf?.takeIf { it.isNotEmpty() }?.let { sessionStore.saveCursor(it) }

        resp.msgs.map { IlinkWireJson.toWireMessage(it) }
    }

    /**
     * get_updates_buf 在 getUpdates 响应时已即时持久化（对标官方 saveGetUpdatesBuf 时机），
     * 无需再提交。保留签名以兼容 WeChatMessageRepository 调用序列。
     */
    suspend fun commitUpdates(): Unit = withContext(Dispatchers.IO) {
        // no-op
    }

    /**
     * 纯 HTTP 实现下 buf 已即时落库，回滚语义不再需要。保留签名，仅记录日志。
     */
    suspend fun resetToCommittedUpdates(): Unit = withContext(Dispatchers.IO) {
        Log.d(TAG, "resetToCommittedUpdates: no-op (updates buf persisted immediately)")
        Unit
    }

    // -----------------------------------------------------------------------
    // 发消息（对标 send.ts / cdn/upload.ts / cdn/cdn-upload.ts）
    // -----------------------------------------------------------------------

    /**
     * 发送文本消息。context_token 缺失时按官方行为拒绝发送（缺失会破坏会话关联）。
     */
    suspend fun sendText(
        toUserId: String,
        text: String,
        contextToken: String? = null,
    ): Unit = withContext(Dispatchers.IO) {
        ensureChannelVersionRefreshed()
        val account = sessionStore.getSessionAccount()
            ?: throw IllegalStateException("未登录微信")
        val token = requireContextToken(contextToken)
        sessionStore.saveContextToken(account.accountId, toUserId, token)
        postTextMessage(account, toUserId, text, token)
        Unit
    }

    /** 分段文本逐条发送（每条独立 sendmessage 请求）。 */
    suspend fun sendTextSegments(
        toUserId: String,
        segments: List<String>,
        contextToken: String? = null,
    ): Unit = withContext(Dispatchers.IO) {
        val texts = segments.filter { it.isNotBlank() }
        require(texts.isNotEmpty()) { "text segments empty" }
        ensureChannelVersionRefreshed()
        val account = sessionStore.getSessionAccount()
            ?: throw IllegalStateException("未登录微信")
        val token = requireContextToken(contextToken)
        sessionStore.saveContextToken(account.accountId, toUserId, token)
        for (text in texts) {
            postTextMessage(account, toUserId, text, token)
        }
        Unit
    }

    /**
     * 发送图片：CDN 加密上传 + sendmessage 图片 item。
     * 文字说明按官方做法作为单独一条文本消息先发（每条请求 item_list 只放一个 item）。
     */
    suspend fun sendImage(
        toUserId: String,
        imageBytes: ByteArray,
        fileName: String,
        description: String? = null,
        contextToken: String? = null,
    ): Unit = withContext(Dispatchers.IO) {
        ensureChannelVersionRefreshed()
        val account = sessionStore.getSessionAccount()
            ?: throw IllegalStateException("未登录微信")
        val token = requireContextToken(contextToken)
        sessionStore.saveContextToken(account.accountId, toUserId, token)

        if (!description.isNullOrBlank()) {
            postTextMessage(account, toUserId, description, token)
        }

        val uploaded = uploadImageToCdn(account, toUserId, imageBytes)
        // 对齐官方 send.ts sendImageMessageWeixin：media.aes_key = base64(aeskey hex 字符串的 ASCII 字节)
        // （32 字节，接收端 parseAesKey 兼容「base64(16B raw)」与「base64(32 字符 hex)」两形态，
        //  bot 通道实测可用的是后者，故不传 base64(16B raw)）。
        val aesKeyBase64 = Base64.getEncoder()
            .encodeToString(uploaded.aesKeyHex.toByteArray(Charsets.US_ASCII))
        val body = IlinkWireJson.buildImageMessageBody(
            toUserId = toUserId,
            contextToken = token,
            clientId = nextClientId(),
            downloadEncryptedQueryParam = uploaded.downloadEncryptedQueryParam,
            aesKeyBase64 = aesKeyBase64,
            cipherSize = uploaded.cipherSize,
            baseInfo = api.buildBaseInfo(),
        ).toString()
        val raw = api.postJson(account.baseUrl, IlinkHttpApi.SEND_MESSAGE_ENDPOINT, body, account.botToken, IlinkHttpApi.API_TIMEOUT_MS)
        api.checkSendResponse(raw)
        Log.i(
            TAG,
            "sendImage: sendmessage ok to=${maskId(toUserId)} fileName=$fileName " +
                "rawSize=${uploaded.rawSize}B cipherSize=${uploaded.cipherSize}B desc=${description?.length ?: 0}chars",
        )
        Unit
    }

    /**
     * CDN 上传管线（官方 uploadMediaToCdn）：
     * filekey = 16 随机字节 hex；aeskey = 16 随机字节；rawsize/rawfilemd5 取明文；
     * filesize = AES-128-ECB PKCS7 密文大小；getuploadurl → CDN POST 密文 → 取 x-encrypted-param。
     */
    private fun uploadImageToCdn(
        account: IlinkAccount,
        toUserId: String,
        plaintext: ByteArray,
    ): IlinkCdnClient.UploadedMedia {
        val filekey = newRandomHex(16)
        val aeskey = ByteArray(16).also { random.nextBytes(it) }
        val rawsize = plaintext.size
        val rawfilemd5 = MessageDigest.getInstance("MD5").digest(plaintext).joinToString("") { "%02x".format(it) }
        val filesize = IlinkCdnCodec.aesEcbPaddedSize(rawsize)

        val uploadUrlBody = IlinkWireJson.buildGetUploadUrlBody(
            filekey = filekey,
            toUserId = toUserId,
            rawsize = rawsize,
            rawfilemd5 = rawfilemd5,
            filesize = filesize,
            aeskeyHex = IlinkCdnCodec.bytesToHex(aeskey),
            baseInfo = api.buildBaseInfo(),
        ).toString()
        val rawResp = api.postJson(
            baseUrl = account.baseUrl,
            endpoint = IlinkHttpApi.GET_UPLOAD_URL_ENDPOINT,
            body = uploadUrlBody,
            botToken = account.botToken,
            timeoutMs = IlinkHttpApi.API_TIMEOUT_MS,
        )
        val uploadUrlResp = json.decodeFromString<IlinkUploadUrlRespDto>(rawResp)
        Log.i(
            TAG,
            "uploadImage: getuploadurl ok rawsize=$rawsize filesize=$filesize " +
                "fullUrl=${uploadUrlResp.uploadFullUrl?.takeIf { it.isNotBlank() }?.let { it.substringAfter("://").substringBefore('/') } ?: "none"} " +
                "uploadParam=${uploadUrlResp.uploadParam?.length ?: 0}chars",
        )

        val cdnUrl = uploadUrlResp.uploadFullUrl?.trim()?.takeIf { it.isNotEmpty() }
            ?: uploadUrlResp.uploadParam?.takeIf { it.isNotBlank() }?.let {
                IlinkCdnClient.buildUploadUrl(IlinkCdnClient.CDN_BASE_URL, it, filekey)
            }
            ?: throw IllegalStateException("getUploadUrl 未返回上传地址（upload_full_url / upload_param 均为空）")

        val ciphertext = IlinkCdnCodec.encryptAesEcb(plaintext, aeskey)
        val downloadParam = cdn.upload(cdnUrl, ciphertext)
        Log.i(
            TAG,
            "uploadImage: cdn upload ok cipher=${ciphertext.size}B " +
                "downloadParam=${downloadParam.length}chars",
        )

        return IlinkCdnClient.UploadedMedia(
            filekey = filekey,
            downloadEncryptedQueryParam = downloadParam,
            aesKeyHex = IlinkCdnCodec.bytesToHex(aeskey),
            aesKeyBase64 = Base64.getEncoder().encodeToString(aeskey),
            rawSize = rawsize,
            cipherSize = filesize,
        )
    }

    // -----------------------------------------------------------------------
    // 媒体下载（对标 cdn-url.ts + pic-decrypt.ts）
    // -----------------------------------------------------------------------

    /** 用户 ID 掩码（前 3 后 2，≤6 位整体 ***），用于日志脱敏。 */
    private fun maskId(id: String): String {
        if (id.length <= 6) return "***"
        return id.take(3) + "***" + id.takeLast(2)
    }

    /**
     * 下载并解密 CDN 媒体：优先 full_url，否则 `{cdnBaseUrl}/download?encrypted_query_param=...`；
     * aes_key 兼容 hex 明文 / base64(16 字节) / base64(32 字符 hex) 三种形态。
     * 失败返回 null（保持原有消费方语义）。
     */
    suspend fun downloadMedia(cdnInfo: IlinkCdnMedia): ByteArray? = withContext(Dispatchers.IO) {
        val aesKey = cdnInfo.aesKey?.takeIf { it.isNotBlank() }
        if (aesKey.isNullOrBlank()) {
            Log.w(TAG, "downloadMedia: missing aes_key")
            return@withContext null
        }
        val url = cdnInfo.fullUrl?.takeIf { it.isNotBlank() }
            ?: cdnInfo.encryptQueryParam?.takeIf { it.isNotBlank() }?.let {
                IlinkCdnClient.buildDownloadUrl(it, IlinkCdnClient.CDN_BASE_URL)
            }
        if (url == null) {
            Log.w(TAG, "downloadMedia: missing encrypt_query_param / full_url")
            return@withContext null
        }
        try {
            val encrypted = cdn.download(url)
            IlinkCdnCodec.decryptAesEcb(encrypted, IlinkCdnCodec.parseAesKeyBytes(aesKey))
        } catch (e: IOException) {
            Log.e(TAG, "downloadMedia network failure", e)
            null
        } catch (e: Exception) {
            Log.e(TAG, "downloadMedia failed", e)
            null
        }
    }

    // -----------------------------------------------------------------------
    // 会话辅助（保持签名兼容）
    // -----------------------------------------------------------------------

    /**
     * 通知 context_token 已更新：纯 HTTP 下只需持久化到 sessionStore。
     *
     * 凭证是「能不能回复」的唯一前置（iLink 只能回复对方先发来的消息），因此**不允许静默
     * 丢弃**：落库失败或会话未就绪时必须留证据，否则线上只能看到「AI 回了但微信没收到」，
     * 无法区分「服务端没下发凭证」与「我们没存下来」——这正是用户报障时卡住的那一步。
     */
    suspend fun notifyContextTokenUpdated(userId: String, contextToken: String? = null) {
        if (userId.isBlank()) {
            Log.w(TAG, "context_token dropped: blank user id")
            return
        }
        val token = contextToken?.takeIf { it.isNotBlank() }
        if (token == null) {
            Log.w(TAG, "context_token dropped: blank token for user=${maskId(userId)}")
            return
        }
        val account = sessionStore.getSessionAccount()
        if (account == null) {
            Log.w(TAG, "context_token dropped: no session account, user=${maskId(userId)}")
            return
        }
        sessionStore.saveContextToken(account.accountId, userId, token)
        Log.i(TAG, "context_token saved user=${maskId(userId)} account=${account.accountId}")
    }

    /**
     * 纯 HTTP 实现无 SDK 客户端对象可重建，保留签名以兼容看门狗调用，仅记录日志。
     */
    suspend fun requestForceRebuild(reason: String) {
        Log.w(TAG, "requestForceRebuild ignored (pure-HTTP transport, nothing to rebuild): $reason")
    }

    /** 取消进行中的二维码登录。 */
    suspend fun cancelLogin(): Unit = withContext(Dispatchers.IO) {
        pendingLogin = null
        Unit
    }

    /** 清空登录态：内存二维码状态 + 持久化会话。 */
    suspend fun closeAndClear(): Unit = withContext(Dispatchers.IO) {
        pendingLogin = null
        sessionStore.clearSessionAccount()
        Unit
    }

    // -----------------------------------------------------------------------
    // 私有工具
    // -----------------------------------------------------------------------

    private fun postTextMessage(account: IlinkAccount, toUserId: String, text: String, contextToken: String) {
        val body = IlinkWireJson.buildTextMessageBody(
            toUserId = toUserId,
            text = text,
            contextToken = contextToken,
            clientId = nextClientId(),
            baseInfo = api.buildBaseInfo(),
        ).toString()
        val raw = api.postJson(account.baseUrl, IlinkHttpApi.SEND_MESSAGE_ENDPOINT, body, account.botToken, IlinkHttpApi.API_TIMEOUT_MS)
        // sendmessage 响应携带 ret/errcode 时判错（-14 → IlinkSessionExpiredException，发送链路据此判死不再重试）
        api.checkSendResponse(raw)
    }

    /**
     * 每进程一次：登录前/首次收发前异步刷新官方 channel_version（fire-and-forget）。
     * 不阻塞调用方；拉取失败静默保留兜底值（refreshOfficialChannelVersion 内部已 runCatching）。
     */
    private fun ensureChannelVersionRefreshed() {
        if (channelVersionRefreshTriggered.compareAndSet(false, true)) {
            channelVersionRefreshScope.launch { refreshOfficialChannelVersion() }
        }
    }

    /** 官方行为：context_token 缺失拒绝发送（缺失会破坏会话关联）。 */
    private fun requireContextToken(contextToken: String?): String {
        return contextToken?.takeIf { it.isNotBlank() }
            ?: throw IllegalStateException(
                "context_token is required: 缺少 context_token，已按官方协议拒绝发送",
            )
    }

    /** client_id 格式：`lianyu-android:{millis}-{8位hex}`。 */
    private fun nextClientId(): String =
        "lianyu-android:${System.currentTimeMillis()}-${newRandomHex(4)}"

    private fun newRandomHex(byteCount: Int): String =
        ByteArray(byteCount).also { random.nextBytes(it) }
            .joinToString("") { "%02x".format(it) }

    internal companion object {
        const val TAG = "IlinkClientManager"
        const val DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
        const val LOGIN_TIMEOUT_MS = 5 * 60 * 1000L

        /** 二维码状态值（官方 login-qr.ts）。 */
        const val QR_STATUS_WAIT = "wait"
        const val QR_STATUS_SCANED = "scaned"
        const val QR_STATUS_EXPIRED = "expired"
        const val QR_STATUS_CONFIRMED = "confirmed"
        const val QR_STATUS_REDIRECT = "scaned_but_redirect"

        /** 二维码过期自动刷新上限（官方 MAX_QR_REFRESH_COUNT）。 */
        const val MAX_QR_REFRESH_COUNT = 3

        /**
         * 兜底 channel_version：与腾讯官方微信 AI 插件（@tencent-weixin/openclaw-weixin 2.4.8）对齐。
         * 正常运行时会启动时从 npm registry 拉取官方最新版本号动态覆盖（见 refreshOfficialChannelVersion），
         * 官方更新后无需改代码。仅离线/首次拉取失败时使用此值。
         */
        const val FALLBACK_CHANNEL_VERSION = "2.4.8"
        const val NPM_MIRROR_LATEST_URL =
            "https://registry.npmmirror.com/@tencent-weixin/openclaw-weixin/latest"

        /** 进程级缓存：npm registry 拉到的官方插件最新版本号 */
        @Volatile
        private var dynamicChannelVersion: String? = null

        fun currentChannelVersion(): String = dynamicChannelVersion ?: FALLBACK_CHANNEL_VERSION

        /**
         * 拉取腾讯官方微信 AI 插件最新版本号，作为 base_info.channel_version 上报值。
         * 官方插件的 channel_version 即其包版本号；过旧的版本号可能触发服务端投递降级
         * （sendmessage ret=0 但对端收不到，对应 lith0924 SDK issue #15 的诱因之一）。
         * 拉取失败时静默保留现有值，不影响登录与收发。
         */
        suspend fun refreshOfficialChannelVersion(): Unit = withContext(Dispatchers.IO) {
            runCatching {
                val client = okhttp3.OkHttpClient.Builder()
                    .connectTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                    .readTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                    .build()
                val body = client.newCall(
                    okhttp3.Request.Builder().url(NPM_MIRROR_LATEST_URL).build(),
                ).execute().use { resp ->
                    if (!resp.isSuccessful) return@runCatching
                    resp.body?.string()
                } ?: return@runCatching

                val version = Json.parseToJsonElement(body)
                    .jsonObject["version"]?.jsonPrimitive?.contentOrNull
                if (!version.isNullOrBlank() && Regex("^\\d+\\.\\d+\\.\\d+").containsMatchIn(version)) {
                    dynamicChannelVersion = version
                    Log.i(TAG, "iLink channel_version updated to official: $version")
                }
            }.onFailure {
                Log.w(TAG, "refreshOfficialChannelVersion failed: ${it.message}")
            }
        }
    }
}
