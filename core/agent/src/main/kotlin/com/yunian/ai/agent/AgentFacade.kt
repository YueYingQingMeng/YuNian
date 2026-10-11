package com.yunian.ai.agent

import android.content.Context
import com.yunian.ai.agent.audit.AgentDispatchRecorder
import com.yunian.ai.agent.audit.PromptAuditRecorder
import com.yunian.ai.agent.memory.MemoryStoreImpl
import com.yunian.ai.agent.skill.CompositeSkillStore
import com.yunian.ai.agent.skill.SkillStoreImpl
import com.yunian.ai.agent.audit.ToolCallRecord
import com.yunian.ai.agent.uniffi.AgentGlobalConfig
import com.yunian.ai.agent.uniffi.AgentEvent
import com.yunian.ai.agent.uniffi.AgentRuntime
import com.yunian.ai.agent.uniffi.AgentTurnRequest
import com.yunian.ai.agent.uniffi.AgentTurnResult
import com.yunian.ai.agent.uniffi.ApiProbe
import com.yunian.ai.agent.uniffi.MemoryContext
import com.yunian.ai.agent.uniffi.MemorySelector
import com.yunian.ai.agent.uniffi.PromptFragment
import com.yunian.ai.agent.uniffi.PromptFragmentSummary
import com.yunian.ai.agent.uniffi.PromptOrchestrator
import com.yunian.ai.agent.uniffi.PromptOrchestratorOptions
import com.yunian.ai.agent.uniffi.SkillContext
import com.yunian.ai.agent.uniffi.SkillMenuEntry
import com.yunian.ai.agent.uniffi.SkillSelector
import com.yunian.ai.agent.uniffi.SkillStore
import com.yunian.ai.agent.uniffi.SplitMode
import com.yunian.ai.agent.uniffi.StreamSink
import com.yunian.ai.agent.uniffi.ToolCategory
import com.yunian.ai.agent.uniffi.ToolDefinition
import com.yunian.ai.agent.uniffi.ToolHost
import com.yunian.ai.common.DeviceIdProvider
import com.yunian.ai.domain.CapabilityGrantStore
import com.yunian.ai.domain.ServiceRegistry

/**
 * Agent 核心门面（纯增量模块，不触碰既有 Agent 调用链）
 *
 * 职责边界：
 * - **Agent 决策逻辑全部在 Rust**（`agent-native` crate），本模块只做 UniFFI 绑定 + 轻量门面。
 * - `AgentRuntime` 由 Rust 构造：全局单例（方案 B——全局状态驻留 Rust），持有
 *   db_path / device_id（固定）+ settings / stickers / credentials（热更新）+ 全局工具注册表；
 *   每次 runTurn 内部从配置快照构造 NativeGateway（Rust 直读 SQLite + 组装 system prompt
 *   + HTTP/SSE），不再需要 Kotlin ModelGateway 回调。
 * - Kotlin 侧仅实现 [ToolHost]（工具副作用）与 [StreamSink]（流式增量落地），
 *   不承载任何 Agent 决策逻辑。
 *
 * 加载说明：UniFFI 绑定通过 `loadIndirect("lianyu_agent")` 自动加载
 * `liblianyu_agent.so`（位于各 ABI 的 jniLibs），无需手动 System.loadLibrary。
 */
object AgentFacade {

    private const val TAG = "AgentFacade"

    /** Room 数据库文件名（与 core:database AppDatabase.DB_NAME 一致）。 */
    private const val DB_NAME = "yunian_database"

    /** 共享全局运行时实例。AgentRuntime 是线程安全的（内部互斥），可复用。 */
    @Volatile
    private var runtime: AgentRuntime? = null

    /**
     * 获取（惰性创建）共享 [AgentRuntime]（全局单例，一次 init）。
     *
     * 全局配置（方案 B）：
     * - db_path / device_id：App 级固定，首次创建时确定；
     * - settings / stickers / credentials：初始为空，通过 [syncRuntimeConfig] 热更新。
     */
    fun runtime(context: Context): AgentRuntime =
        runtime ?: synchronized(this) {
            runtime ?: AgentRuntime(
                AgentGlobalConfig(
                    dbPath = context.getDatabasePath(DB_NAME).absolutePath,
                    deviceId = DeviceIdProvider.getDeviceId(context),
                    settingsJson = "{}",
                    stickers = emptyList(),
                    credentialsJson = "{}",
                    orchestrator = promptOrchestrator(context),
                )
            ).also { runtime = it }
        }

    /**
     * 预热 Agent 运行时（P2-9）：提前触发 UniFFI 库加载（双 loadIndirect，约百毫秒级），
     * 避免首次 runTurn 卡顿。建议 App 启动数秒后在后台线程调用一次；幂等。
     */
    fun warmUp(context: Context) {
        runCatching { runtime(context) }.onFailure {
            android.util.Log.w(TAG, "warmUp failed: ${it.message}")
        }
        // Forward any previous-session native diagnostics without blocking runtime creation.
        RustAgentLogBridge.dump(context)
    }

    /**
     * 核心插件宿主快照（方案 A：cordis-rs 底座可见性，UniFFI 透传）。
     *
     * 返回 JSON：回合统计（turns_started/turns_completed/consolidations_due）、
     * 各内置插件 fiber 状态（observer/core_tools/prompt_fragments/turn_consolidation）、
     * 已注册工具与片段。宿主未初始化返回 {"core_plugins":"unavailable"}。
     */
    fun corePluginSnapshot(context: Context): String =
        runtime(context).corePluginSnapshot()

    /**
     * 批准一次待确认的 Commerce 工具调用（Approval 门）。
     * 一次性授权：重跑回合时名称+参数完全匹配的调用直接执行（使用后失效）。
     */
    fun approveTool(context: Context, name: String, args: String) {
        runtime(context).approveTool(name, args)
    }

    /**
     * 拒绝一次待确认的 Commerce 工具调用；后续匹配调用回灌「用户拒绝」而不执行。
     */
    fun rejectTool(context: Context, name: String, args: String) {
        runtime(context).rejectTool(name, args)
    }

    /**
     * Eval：注入脚本化传输（离线 mock，按序弹出预置响应，不发真实网络）。
     * 空列表 = 清空（恢复默认 ureq 传输）。用于零成本离线回归。
     */
    fun setMockTransport(context: Context, responses: List<String>) {
        runtime(context).setMockTransport(responses)
    }

    /**
     * 安装 suflow.cloud（PARTNER）设备签名回调（私钥在 Android Keystore，签名不跨 FFI）。
     * PARTNER 通道请求在 Rust 侧 fail-closed：无签名器或签名失败则拒绝发送。
     */
    fun installRequestSigner(context: Context) {
        runtime(context).setSignatureProvider(AgentRequestSigner())
    }

    /**
     * 注入世界书 JSON（社区 SillyTavern World Info 格式；聊天陪伴）。
     * 回合组装时按 keys/正则/constant/scan_depth/预算语义自动注入，不再手写规则。
     * 空串/非法 JSON 静默忽略（保留原世界书）。
     */
    fun setWorldbook(context: Context, json: String) {
        runCatching { runtime(context).setWorldbook(if (json.isBlank()) null else json) }
            .onFailure { android.util.Log.w("AgentFacade", "setWorldbook failed", it) }
    }

    /**
     * 解析角色卡 PNG（Tavern V2：tEXt chunk "chara" 内嵌 base64 JSON）。
     * 返回 null = 解析失败（非 PNG / 无角色卡数据 / JSON 非法）。
     */
    fun parseCharacterCardPng(context: Context, bytes: ByteArray): com.yunian.ai.agent.uniffi.CharacterCardInfo? =
        runCatching { com.yunian.ai.agent.uniffi.parseCharacterCardPng(bytes) }.getOrNull()

    /** 解析角色卡 JSON（V1/V2/V3；含 Tavern map 世界书回退）。 */
    fun parseCharacterCardJson(context: Context, json: String): com.yunian.ai.agent.uniffi.CharacterCardInfo? =
        runCatching { com.yunian.ai.agent.uniffi.parseCharacterCardJson(json) }.getOrNull()

    /**
     * 同步全局可变配置到 Rust [AgentRuntime]（settings / stickers / credentials 热更新）。
     *
     * 每次 runTurn / runTurnStream 前调用，传入调用方当前配置来源的最新值：
     * - settingsJson：{role, reasoning_field, yandere, ...}
     *   （来自 AppSettingsStore / 会话设置；inner_thought/ntp_time 已废弃移除）
     * - stickers：可用表情包名称列表（StickerManager 产物）
     * - credentialsJson：{session, client_id}（RemoteKeyProvider.getPartnerSession，PARTNER 用）
     */
    fun syncRuntimeConfig(
        context: Context,
        settingsJson: String,
        stickers: List<String>,
        credentialsJson: String,
    ) {
        val rt = runtime(context)
        rt.updateSettings(settingsJson)
        rt.updateStickers(stickers)
        rt.updateCredentials(credentialsJson)
    }

    /**
     * 组装 Rust `AgentGlobalConfig.settingsJson`。
     * 字段与 native_gateway.rs `load_companion_profile` / agent.rs `orchestration_options`
     * 实际解析项对齐：
     * - `role`：伴侣角色（L1 身份映射，默认 GIRLFRIEND）
     * - `timezone`：设备时区（L0 动态环境上下文，如 Asia/Shanghai）
     * - `working_memory_limit`：WORKING 短期记忆注入上限（默认 200）
     * （inner_thought / ntp_time 已废弃移除；reasoning_field / yandere 等暂未被 Rust 消费，不传）
     */
    fun buildSettingsJson(
        role: String = "GIRLFRIEND",
        imageGenRules: String? = null,
    ): String = org.json.JSONObject().apply {
        put("role", role)
        put("timezone", java.util.TimeZone.getDefault().id)
        put("working_memory_limit", 200)
        // 生图协议文本（Kotlin ImageGenTriggerLogic.systemRules 产物）。
        // 提示词组装已下沉 Rust：旧做法是把协议拼进 aiCompanion.systemPrompt 交给 Kotlin
        // 请求链路，Agent 路径不经过那里，于是「模型说不会画画、但关键词仍触发生图」。
        // 经 settings 下发（与 timezone 等同一热更新通道），由 Rust 拼进 system prompt。
        if (!imageGenRules.isNullOrBlank()) put("image_gen_rules", imageGenRules)
    }.toString()

    /**
     * 组装 Rust `AgentGlobalConfig.credentialsJson`（PARTNER 场景）。
     * native_gateway.rs `provider_headers` 读取 `session` / `client_id` 两个 key；
     * 会话缺失时返回 `{}`（Rust 侧 PARTNER 请求将不带会话头）。
     *
     * @param sessionToken PARTNER session token（RemoteKeyProvider.getPartnerSession）
     * @param clientId PARTNER client id
     * @param apiKey 已解密的 API Key（非 PARTNER 模式由 ApiConfigRepository.decryptForUse 提供；
     *                Rust 无法解密 SQLite 中的 Tink 加密串，需 Kotlin 解密后经此传入）
     * @param extraApiKeys 已解密的备用 Key（逗号分隔）
     */
    fun buildCredentialsJson(
        sessionToken: String?,
        clientId: String?,
        apiKey: String? = null,
        extraApiKeys: String? = null,
    ): String {
        val json = org.json.JSONObject()
        if (!sessionToken.isNullOrBlank() && !clientId.isNullOrBlank()) {
            json.put("session", sessionToken)
            json.put("client_id", clientId)
        }
        if (!apiKey.isNullOrBlank()) json.put("api_key", apiKey)
        if (!extraApiKeys.isNullOrBlank()) json.put("extra_api_keys", extraApiKeys)
        return if (json.length() == 0) "{}" else json.toString()
    }

    /** 分段入口（Rust 实现）：按模式把长文本拆成气泡列表。 */
    fun segment(text: String, mode: SplitMode = SplitMode.SIMPLE): List<String> =
        com.yunian.ai.agent.uniffi.agentSegment(text, mode)

    /** 纯噪声判断（Rust 实现）：用于过滤打扰式消息。 */
    fun isNoise(text: String): Boolean =
        com.yunian.ai.agent.uniffi.agentIsNoise(text)

    /**
     * 运行一轮 Agent 回合（标准循环：required → auto）。
     *
     * @param request 回合请求（含历史 JSON、工具定义、max_rounds、额外系统规则等）
     * @param context 应用上下文（首次调用时用于初始化全局 AgentRuntime）
     * @param companionId 当前伴侣 ID（None = 群聊场景，需 system_prompt 覆盖）
     * @param toolHost 工具宿主回调（Kotlin 实现 → 现有 ToolRegistry 副作用）
     * @return 回合结果：事件列表（bubble/sticker/status/confirm_request）+ 最终文本
     */
    fun runTurn(
        request: AgentTurnRequest,
        context: Context,
        companionId: Long?,
        toolHost: ToolHost,
    ): AgentTurnResult = runtime(context).runTurn(request, companionId, toolHost)
        .also { RustAgentLogBridge.dump(context) }

    /**
     * 运行一轮流式 Agent 回合（SSE 流式输出完全下沉 rs）。
     *
     * Rust 侧 NativeGateway::send_stream 直连 LLM 的 SSE 流，增量经
     * [StreamSink] 实时回调给 Kotlin 消息管线（打字机效果）；决策仍在 Rust。
     *
     * @param request 回合请求（同 [runTurn]）
     * @param context 应用上下文（首次调用时用于初始化全局 AgentRuntime）
     * @param companionId 当前伴侣 ID
     * @param toolHost 工具宿主回调
     * @param sink 流式增量回调（Rust → Kotlin 消息管线落地）
     * @return 回合结果（与 [runTurn] 同构）
     */
    fun runTurnStream(
        request: AgentTurnRequest,
        context: Context,
        companionId: Long?,
        toolHost: ToolHost,
        sink: StreamSink,
    ): AgentTurnResult = runtime(context).runTurnStream(request, companionId, toolHost, sink)
        .also { RustAgentLogBridge.dump(context) }

    // ── 全局工具注册表（决策在 Rust：builtin + global + session 三级组装） ──

    /**
     * 注册全局工具（进程级，跨会话共享）。
     * Kotlin 侧启动时把现有 ToolRegistry 的工具转成 [ToolDefinition] 注册；
     * Rust 每次 runTurn 组装 tool_defs = builtin + global + session。
     */
    fun registerGlobalTools(context: Context, tools: List<ToolDefinition>): Unit =
        runtime(context).registerGlobalTools(tools)

    /** 注销单个全局工具。 */
    fun unregisterGlobalTool(context: Context, name: String): Unit =
        runtime(context).unregisterGlobalTool(name)

    /** 查询当前全部全局工具定义。 */
    fun globalToolDefinitions(context: Context): List<ToolDefinition> =
        runtime(context).globalToolDefinitions()

    // ── Agent Skills（混合存储：Room 索引 + 文件系统正文） ──

    /** 共享技能选择器（决策在 Rust：召回 → 评分 → 读取正文）。 */
    @Volatile
    private var skillSelector: SkillSelector? = null

    /** 技能存储适配器覆盖位（Q6：本地技能体系桥接，见 [AgentFacade.installSkillStoreProvider]）。 */
    @Volatile
    private var skillStoreOverride: SkillStore? = null

    /**
     * 注册技能存储适配器（应在首次 [skillSelector] 创建**之前**调用）。
     *
     * Q6 双体系收敛：`feature:skills` 的 `SkillStoreAdapter` 把本地 `assets/skills` +
     * `filesDir/external_skills` + 技能市场桥接到 Rust `SkillSelector`，从而退役 `use_skill`、
     * 统一为 `load_skill`（防模型双调，R22）。
     *
     * 若在 [skillSelector] 首次创建后调用，则覆盖位只对**后续**新建的选择器生效
     * （已建实例的 `SkillStore` 不可热替换）——故默认蓝图装载时机必须早于首次 Agent 回合。
     */
    fun installSkillStoreProvider(store: SkillStore) {
        skillStoreOverride = store
    }

    /**
     * 获取（惰性创建）共享 [SkillSelector]。
     * Rust 侧持有 [com.yunian.ai.agent.uniffi.SkillStore] 回调；
     * 优先使用 [installSkillStoreProvider] 注册的适配器（本地技能体系），
     * 缺省回退 [SkillStoreImpl]（Room 索引 + SkillFileStore 文件正文）。
     */
    fun skillSelector(context: Context): SkillSelector =
        skillSelector ?: synchronized(this) {
            skillSelector ?: SkillSelector(skillStore(context)).also { skillSelector = it }
        }

    /**
     * Rust `SkillSelector` 的数据源：注入本地资产适配器时**仍与** Agent 原生存储组合，
     * 否则种进 Room 的内置技能（如 `builtin_chat_tool_protocol`）不会出现在 L1 目录，
     * 模型将失去聊天工具协议与表情包/分段工具的调用依据。
     */
    private fun skillStore(context: Context): SkillStore {
        val localOverride = skillStoreOverride ?: return SkillStoreImpl(context)
        return CompositeSkillStore(localOverride, SkillStoreImpl(context))
    }

    /** 技能选择（Rust 决策）：按 query 召回并读取正文。companionId=null 表示全局技能。 */
    fun selectSkills(context: Context, query: String, companionId: Long? = null, limit: UInt = 5u): List<SkillContext> =
        skillSelector(context).select(query, companionId, limit)

    /** 将选中技能拼成 system prompt 片段（Rust 实现）。 */
    fun buildSkillSystemContext(context: Context, skillContexts: List<SkillContext>): String =
        skillSelector(context).buildSystemContext(skillContexts)

    // ── Agent Skills 渐进式披露（L1 目录 / L2 按需加载） ──

    /**
     * L1 技能目录：仅返回启用技能的 name + description（不含正文），token 极低。
     * 注入 system prompt 让模型知道有哪些技能可用，需要时再调 [loadSkillContent] 取正文。
     *
     * 汇合点（对齐 Hermes）：`availableTools` 非空时，技能声明依赖的工具（meta.tools）
     * 全部不在当前可用工具集 → 技能不出现在目录（模型不知道 = 无法用）；空 = 不过滤。
     */
    fun discoverSkills(
        context: Context,
        companionId: Long? = null,
        limit: UInt = 5u,
        availableTools: List<String> = emptyList(),
    ): List<SkillMenuEntry> =
        skillSelector(context).discover(companionId, limit, availableTools)

    /** 将技能目录拼成 system prompt 片段（Rust 实现，含 load_skill 使用提示）。 */
    fun buildSkillMenuContext(context: Context, menu: List<SkillMenuEntry>): String =
        skillSelector(context).buildMenuContext(menu)

    /** L2 按需加载技能完整正文（供 load_skill 工具回调）。已禁用/缺失返回 null。 */
    fun loadSkillContent(context: Context, skillId: String, companionId: Long? = null): String? =
        skillSelector(context).loadContent(skillId, companionId)

    /**
     * load_skill 工具定义（渐进式披露 L2）。
     *
     * 作为**会话级工具**（request.tools）传入：Rust `builtin_tool_definitions` 不含它，
     * 因此不会与 builtin 列表重复；执行时 Rust `execute_tool` 查 builtin map 未命中 →
     * 查 session_tools（本定义注册）→ 回调 [AgentToolHost] → 本类 [loadSkillContent] 读正文。
     */
    fun skillToolDefinitions(): List<ToolDefinition> = listOf(
        ToolDefinition(
            name = "load_skill",
            description = "按需加载一个技能的完整操作说明。触发条件：当系统提示词中 [可用技能目录] 列出了某技能、且当前用户请求与该技能描述的场景匹配时，必须先调用本工具加载其完整规则再执行。约束：与当前对话无关的技能不要加载；加载后按技能正文执行。",
            parametersJson = """{"type":"object","properties":{"skill_id":{"type":"string","minLength":1,"description":"技能 ID（目录中的编号名，如 builtin_chat_tool_protocol）"}},"required":["skill_id"],"additionalProperties":false}""",
            category = ToolCategory.CHAT,
            toolsets = listOf("skill"),
            available = true,
        ),
    )

    // ── 内置技能种子（系统级 Skill：优化内置聊天工具调用） ──

    /** 内置聊天工具协议 skill 的固定 ID（幂等种子键，同时作为 Room 主键） */
    private const val BUILTIN_CHAT_TOOL_SKILL_ID = "builtin_chat_tool_protocol"

    /**
     * 内置聊天工具调用协议 + 聊天红线 skill 正文。
     *
     * 目标：让模型（DeepSeek 思考模式等）同时做到两件事：
     * 1. 更克制、更不像 AI 腔的微信聊天（只列禁止项）；
     * 2. 更优地调用 Rust 内置聊天工具 `emit_segmented` / `send_sticker` / `emit_bubble`。
     *
     * 编写原则（v7）：**从「教育 AI 怎么做」转为「只列禁止做什么」**——
     * 模型本身就会聊天，正向指导既浪费 token 又易让模型照本宣科。
     * 本 skill 只写「红线（严禁什么）」+「工具映射（协议）」；
     * 不重复描述工具已有信息（输入输出格式由工具定义承担）。
     *
     * 由 [SkillSelector] 在用户 query 命中「气泡 / 分段 / 表情包 / 连发 / 讲故事」等词时
     * 召回并注入 system prompt（L4 技能层）。
     */
    private val BUILTIN_CHAT_TOOL_SKILL_CONTENT: String = """
# 微信真人聊天规范（红线优先版）

你是 AI 恋人，通过微信式气泡与用户聊天。你本身就擅长自然聊天——本规范不教你"怎么做"，只列"禁止做什么"。凡是没被禁止的，按你的自然判断来。

## 一、内容红线（触碰即失败）
1. 禁止小作文：单条气泡超过 3 个短句即违规；除非用户明确要求"多讲点/展开说/讲故事/分析一下"。
2. 禁止说教、总结、升华：结尾不加"所以呀，生活就是这样"这类鸡汤；不催用户（睡觉/吃饭/上班）。
3. 禁止答非所问、绕开话题自说自话。
4. 禁止无视用户拒绝：用户说"不要总催我"后立刻停，别再提。
5. 禁止自问自答：不自己提问又替用户回答，不模拟用户语气。
6. 禁止替用户说话：一条气泡只说你自己这一方。
7. 禁止重复已说内容。

## 二、语气红线（AI 腔）
1. 禁止波浪号"～"多于 1 个/条。
2. 禁止语气词"呀/呢/啦/嘛/哦"多于 1 个/句。
3. 禁止固定口头禅开场（"好呀好呀""好啦好啦""好好好"）。
4. 禁止书面语、排比句。
5. 禁止元信息（"我这就给你讲个故事""我发你一条气泡"）。
6. 禁止套路化故事开场（永远"从前有只XX"）；讲完一个就停。

## 三、工具使用（唯一需要你主动做的事 · 协议）
用户消息或当前语境出现以下情况时，用工具输出，不写普通文本：

| 场景 | 工具 |
|---|---|
| 普通口语回复，一条消息 | emit_bubble |
| 长文本按语义/逐句分段 | emit_segmented |
| 情绪/撒娇/可爱 | send_sticker |

- 用户要求"分开说/一句一句发/分条发/连发/多发几条/多句" → 用工具连发。
- 描述大型沉重情感话题 → 不分段。

## 四、工具红线
1. 禁止一条气泡超过 14 个短句；内容多就拆成多条。
2. 禁止连发重复内容：每条都是新信息增量。
3. 禁止 markdown（`**`、`#`、列表符号）、禁止括号说明。
4. 禁止把思考过程 / 内心独白 / 推理分析写进 `emit_bubble` / `emit_segmented` 的 `text`：
   气泡里**只能**是你要对用户说的话。像"嗯，等待用户回答""先分析一下用户意图"这类
   元叙述一律不得输出——思考留给自己，不要发出去。
5. 话说完/已提出问题 → 停止调用工具，直接返回短文本收尾，不要为凑条数继续输出。
""".trimIndent()

    /**
     * 幂等种子：把内置聊天工具协议 skill 写入混合存储（Room 索引 + content.md）。
     * 已存在（version ≥ [BUILTIN_CHAT_TOOL_SKILL_VERSION]）则跳过；应用启动时调用一次。
     * 返回是否已就绪（已存在当前版本也视为成功；false 仅表示写入失败）。
     */
    fun seedBuiltinChatToolSkill(context: Context): Boolean {
        val store = SkillStoreImpl(context)
        // 先查索引：已存在且版本足够则跳过（不覆盖用户可能的自定义）
        val listJson = store.listSkills(null)
        val exists = runCatching {
            val arr = org.json.JSONArray(listJson)
            (0 until arr.length()).any { i ->
                val o = arr.getJSONObject(i)
                o.optString("skill_id") == BUILTIN_CHAT_TOOL_SKILL_ID &&
                    o.optInt("version", 0) >= BUILTIN_CHAT_TOOL_SKILL_VERSION
            }
        }.getOrDefault(false)
        if (exists) return true
        val meta = org.json.JSONObject().apply {
            put("skill_id", BUILTIN_CHAT_TOOL_SKILL_ID)
            put("name", "微信真人聊天规范")
            put(
                "description",
                "微信真人聊天红线与内置工具调用协议：禁小作文(超3句)、禁说教催人、禁波浪号/语气词堆叠、禁固定口头禅、禁套路化开场、禁自问自答、禁替用户说话；用户要求分开说/一句一句发/连发/多句/讲故事/哄我/撒娇时，用 emit_bubble 逐条输出、emit_segmented 分段、send_sticker 表情包；禁 markdown、禁括号说明。",
            )
            put("category", "CHAT")
            put(
                "tags",
                "气泡,连发,分开说,一句一句,分条,多句,分段,表情包,表情,口语,微信,sticker,bubble,segment,悄悄话,撒娇,可爱,故事,哄我,哄,讲个故事,哄睡,真人,像人,说话,聊天,语气,短句,波浪号,ai腔,说教,催,睡觉,口头禅,开场,小作文",
            )
            // 依赖的工具（汇合点）：builtin 聊天工具恒可用，因此本技能恒显示
            put("tools", org.json.JSONArray(listOf("emit_bubble", "emit_segmented", "send_sticker")))
            put("enabled", true)
            put("companion_id", org.json.JSONObject.NULL)
            put("version", BUILTIN_CHAT_TOOL_SKILL_VERSION)
            put("updated_at", System.currentTimeMillis())
        }
        val ok = store.saveSkill(meta.toString(), BUILTIN_CHAT_TOOL_SKILL_CONTENT)
        if (ok > 0) {
            android.util.Log.i(TAG, "seeded builtin chat-tool skill v$BUILTIN_CHAT_TOOL_SKILL_VERSION")
        } else {
            android.util.Log.w(TAG, "seed builtin chat-tool skill failed: $ok")
        }
        return ok > 0
    }

    private const val BUILTIN_CHAT_TOOL_SKILL_VERSION = 8

    // ── Agent Memory（决策在 Rust：召回/评分/排序/整理；Kotlin 仅纯 IO） ──

    /** 共享记忆选择器（决策在 Rust）。 */
    @Volatile
    private var memorySelector: MemorySelector? = null

    /**
     * 获取（惰性创建）共享 [MemorySelector]。
     * Rust 侧持有 [com.yunian.ai.agent.uniffi.MemoryStore] 回调，Kotlin 实现为
     * [MemoryStoreImpl]（Room unified_memories 表 + SharedPreferences 整理时间，纯 IO）。
     */
    fun memorySelector(context: Context): MemorySelector =
        memorySelector ?: synchronized(this) {
            memorySelector ?: MemorySelector(MemoryStoreImpl(context)).also { memorySelector = it }
        }

    /** 记忆召回（Rust 决策）：query 为空按时间衰减，非空按关键词 + 语义 + 衰减综合评分。 */
    fun selectMemories(
        context: Context,
        query: String,
        scopeJson: String,
        limit: UInt = 5u,
    ): List<MemoryContext> = memorySelector(context).select(query, scopeJson, limit)

    /** 将选中记忆拼成 system prompt 片段（Rust 实现）。 */
    fun buildMemorySystemContext(context: Context, memoryContexts: List<MemoryContext>): String =
        memorySelector(context).buildSystemContext(memoryContexts)

    /**
     * 是否需要整理记忆（Rust 决策，惰性触发）。
     * 规则：最后一条对话距今 > 1h 且 上次整理距今 > 30min；无整理记录视为刚整理过。
     */
    fun shouldConsolidateMemories(context: Context, companionId: Long? = null): Boolean =
        memorySelector(context).shouldConsolidate(companionId)

    /** 整理记忆（Rust 决策）：遗忘过期/陈旧 → WORKING→EPISODIC 合并 → 去重。返回 JSON 结果。 */
    fun consolidateMemories(context: Context, companionId: Long? = null): String =
        memorySelector(context).runConsolidation(companionId)

    /** Agent Memory 工具定义（recall_memory / save_memory / consolidate_memory，ToolCategory::Memory）。 */
    fun memoryToolDefinitions(context: Context): List<ToolDefinition> =
        memorySelector(context).memoryToolDefinitions()

    /** 执行记忆工具（Rust 决策；副作用经 MemoryStore 回调落库）。 */
    fun executeMemoryTool(
        context: Context,
        name: String,
        argsJson: String,
        contextJson: String,
    ): String = memorySelector(context).executeMemoryTool(name, argsJson, contextJson)

    /**
     * 记忆技能描述（程序性记忆 Skill，Rust 内置）：注入 Agent system prompt，
     * 让 Agent 知道何时调用 recall_memory / save_memory / consolidate_memory。
     */
    fun memorySkillContext(): String = com.yunian.ai.agent.uniffi.memorySkillContext()

    // ── Prompt Orchestrator（7 层编排，决策全在 Rust） ──

    /** 共享提示词编排器（惰性创建；memory/skill 选择器复用上层共享实例）。 */
    @Volatile
    private var promptOrchestrator: PromptOrchestrator? = null

    /**
     * 获取（惰性创建）共享 [PromptOrchestrator]。
     * Rust 侧持有 MemorySelector / SkillSelector 回调，Kotlin 实现为
     * MemoryStoreImpl / SkillStoreImpl（纯 IO）。
     */
    fun promptOrchestrator(context: Context): PromptOrchestrator =
        promptOrchestrator ?: synchronized(this) {
            promptOrchestrator ?: PromptOrchestrator(
                memorySelector(context),
                skillSelector(context),
            ).also {
                promptOrchestrator = it
                android.util.Log.i("AgentFacade", "[debug] PromptOrchestrator created memory=${memorySelector(context) != null} skill=${skillSelector(context) != null}")
            }
        }

    /**
     * 按 7 层（L1-L7）编排本次请求的提示词片段（Rust 决策）。
     * 片段按 layer 稳定排序，供调用方拼入 system prompt 或做审计。
     */
    fun buildPromptFragments(
        context: Context,
        companionId: Long?,
        groupId: Long?,
        query: String,
        options: PromptOrchestratorOptions,
    ): List<PromptFragment> =
        promptOrchestrator(context).buildFragments(null, companionId, groupId, query, options)

    /**
     * 编排 extra system rules（与片段同序的规则文本，L0-L5 全部片段）。
     * 主要用于 [recordTurnAudit] 的规则指纹计算（L6/L7 已移除）。
     */
    fun buildExtraSystemRules(
        context: Context,
        companionId: Long?,
        groupId: Long?,
        query: String,
        options: PromptOrchestratorOptions,
    ): List<String> =
        promptOrchestrator(context).buildExtraSystemRules(null, companionId, groupId, query, options)

    /**
     * 干跑编排（不含正文，供审计）：返回片段摘要（id/source/layer/lifetime/chars）。
     * 每次 runTurn 后调用，配合 [promptAudit] 落 prompt_audit 表。
     */
    fun dryRunPrompt(
        context: Context,
        companionId: Long?,
        groupId: Long?,
        query: String,
        options: PromptOrchestratorOptions,
    ): List<PromptFragmentSummary> =
        promptOrchestrator(context).dryRun(null, companionId, groupId, query, options)

    // ── Prompt 编排审计（可观测性，纯 IO 只写） ──

    /** 共享审计记录器（惰性创建）。 */
    @Volatile
    private var auditRecorder: PromptAuditRecorder? = null

    /** 获取（惰性创建）共享 [PromptAuditRecorder]。 */
    fun promptAudit(context: Context): PromptAuditRecorder =
        auditRecorder ?: synchronized(this) {
            auditRecorder ?: PromptAuditRecorder(context).also { auditRecorder = it }
        }

    // ── Agent 工具装配 ──

    /**
     * 领域 [com.yunian.ai.domain.AiTool] → Rust [ToolDefinition]。
     * feature 层把全局工具（ToolRegistry）转成 Rust 会话工具定义传入 AgentTurnRequest。
     *
     * 归档工具集（Hermes TOOLSETS）：AiTool.toolsets 为空时保持空（= 通用 General），
     * 不再硬塞「domain」；available 取 AiTool 静态标记（动态 check_fn 由 ToolRegistry.availableTools 提前过滤）。
     *
     * category 推导：显式 [category] 参数优先；装配期请用 [toolDefinitionsFor]（它按伴侣的有效决定
     * 逐工具调用 [deriveToolCategory]），只有在需要覆盖单个工具类别时才显式传 [category]。
     */
    fun toolDefinition(
        tool: com.yunian.ai.domain.AiTool,
        category: ToolCategory,
    ): ToolDefinition = ToolDefinition(
        name = tool.name,
        description = tool.description,
        parametersJson = tool.parametersJsonSchema,
        category = category,
        toolsets = tool.toolsets.toList(),
        available = tool.isAvailable(),
    )

    /**
     * 按「工具自身声明 + 该伴侣的显式授权决定」推导 Rust [ToolCategory]
     * （Hermes TOOLSETS 分组 + 确认语义 → 类别语义；空工具集 = 通用）。
     *
     * **这里是「工具是否需要用户确认」的接线点。** Rust 侧唯一的门控判定
     * （`agent-native/src/agent.rs`：`definition.category == ToolCategory::Commerce`）
     * 目前以 COMMERCE 作为「需要确认」的载体；而 [ToolDefinition] 的 JSON 投影只取
     * name / description / parameters_json，既不带 category 也不带 requiresConfirmation，
     * 所以「这个工具要不要确认」只能在**装配期**折进 category——否则该声明在 Agent 链路上
     * 没有任何消费者（等于摆设）。
     *
     * 判定优先级（三档，实现收在 core:domain 的 `CapabilityDefaults.requiresConfirm`，
     * 与设置页开关**共用同一实现**，避免两边漂移）：
     * 1. [decisions] 里该工具显式为 `true` ⇒ 不需要确认 ⇒ 回落到 [deriveToolsetCategory]；
     * 2. [decisions] 里该工具显式为 `false` ⇒ 需要确认 ⇒ **COMMERCE**
     *    （连本来不需要确认的安全工具也能被用户显式改成「必须先确认」）；
     * 3. [decisions] 里没有该工具 ⇒ 回到**工具自身声明** `requiresConfirmation`
     *    （true ⇒ COMMERCE，false ⇒ [deriveToolsetCategory]）。
     *
     * 工具集只决定「归档 / 分组」，确认语义决定「治理等级」，后者必须在门控上生效
     * （如 requiresConfirmation = true 的 memory 工具仍须确认）。
     * 无显式决定时映射与历史行为逐字一致，不改动任何既有类别。
     *
     * 将来若给 [ToolDefinition] 加显式 `requires_confirm` 字段（Rust 门控直接读该字段），
     * 本函数的确认分支应当一并删除：确认语义回归字段本身，本函数退化为纯工具集分组映射。
     *
     * @param decisions 该伴侣视角下的有效决定表（工具名 → 是否允许），来自
     *   `CapabilityGrantStore.decisionsFor`；空表 = 无任何显式决定。
     */
    internal fun deriveToolCategory(
        tool: com.yunian.ai.domain.AiTool,
        decisions: Map<String, Boolean>,
    ): ToolCategory = when {
        com.yunian.ai.domain.CapabilityDefaults.requiresConfirm(
            explicit = decisions[tool.name],
            toolRequiresConfirmation = tool.requiresConfirmation,
        ) -> ToolCategory.COMMERCE

        else -> deriveToolsetCategory(tool)
    }

    /**
     * 纯工具集映射（不含确认语义分支）。**行为与重构前的 `deriveToolCategory` 逐字一致**，
     * 拆出来只是为了让「确认语义」这条唯一的差异可以被单独表达
     * （显式允许 ⇒ 跳过确认；显式禁止 ⇒ 强制确认）。
     */
    private fun deriveToolsetCategory(tool: com.yunian.ai.domain.AiTool): ToolCategory = when {
        tool.toolsets.isEmpty() -> ToolCategory.GENERAL
        "commerce" in tool.toolsets -> ToolCategory.COMMERCE
        "memory" in tool.toolsets -> ToolCategory.MEMORY
        "chat" in tool.toolsets -> ToolCategory.CHAT
        else -> ToolCategory.CUSTOM
    }

    // ── 工具授权（CapabilityGrant）：唯一折叠点 ──

    /**
     * **工具授权的唯一折叠点**：按伴侣装配本回合的工具定义（**不含通道维度**）。
     *
     * 为什么需要它：Rust 侧唯一的确认门判定是
     * `definition.category == ToolCategory::Commerce`（agent-native/src/agent.rs），而
     * `category` 只在**装配期**由 [deriveToolCategory] 算出来。因此在装配期折叠是放行
     * 「已授权工具」的**唯一**位置——本函数也是唯一的入口，所有工具装配点都必须走它，
     * 否则同一次请求里会出现两套互相矛盾的类别（有的工具走授权表、有的不走）。
     *
     * 折叠语义（**只改变被显式决定的那一个工具，其余一律维持现状**）：
     * - 该伴侣的有效决定表里 `toolName → true` ⇒ 不再强制 COMMERCE，
     *   类别回落到它自身的工具集映射（生产确认类工具 toolsets 为空 ⇒ GENERAL）；
     * - 该伴侣的有效决定表里 `toolName → false` ⇒ 强制 COMMERCE（**即使工具自己没声明要确认**）；
     * - 表里没有该工具（无决定 / 无记录 / 存储读取失败 / 存储未注册）
     *   ⇒ 调用**同一个** [deriveToolCategory]，类别与引入授权表前**逐字一致**。
     *
     * 注意：授权只解除 / 施加「确认」这一道；工具**自身工具集**本来就是 `commerce` 的工具
     * （如历史遗留的 commerce 工具集）仍然得到 COMMERCE，仍会走确认门。
     * 这是刻意的保守选择——授权表不是「关掉确认门」，而是「承认用户已经逐条同意过的那条组合」。
     *
     * fail-closed 保证：[resolveDecisions] 在「存储未注册 / 读取抛异常」时返回空表，
     * 于是所有工具都走 [deriveToolCategory]，行为与没有授权表时完全一致。
     *
     * @param companionId 目标伴侣 ID（来自 `DialogueRequest.companionId`）。
     * @param tools 本次要装配的领域工具（通常是 `ToolRegistry.availableTools()`）。
     */
    suspend fun toolDefinitionsFor(
        companionId: Long,
        tools: List<com.yunian.ai.domain.AiTool>,
    ): List<ToolDefinition> {
        val decisions = resolveDecisions(companionId)
        return tools.map { tool -> toolDefinition(tool, deriveToolCategory(tool, decisions)) }
    }

    /**
     * 解析「伴侣 [companionId] 视角下的有效决定表」。
     *
     * fail-closed 的两条路径：
     * 1. 存储未注册（如 App 尚未 `ServiceRegistry` 绑定，或单元测试环境）⇒ 空表；
     * 2. 读取抛异常 ⇒ 空表（吞掉，绝不让授权表读取失败影响对话回合）。
     *
     * 此处**刻意不记日志**：`SecureLog` 底层是 `android.util.Log`，在纯 JVM 单测里会抛
     * `RuntimeException("Stub!")`，在这里兜一层日志反而会把 fail-closed 兜底路径变成崩溃点。
     * 真实存储实现（`CapabilityGrantStoreImpl`）自身已记录读取失败原因，可观测性不减。
     *
     * `CancellationException` 单独重抛（对齐 [AgentConfirmGuard] 的 U2 结论）：
     * 取消不是「读取失败」，吞掉它会让已取消的作用域继续往下装配工具。
     */
    internal suspend fun resolveDecisions(companionId: Long): Map<String, Boolean> {
        val store = ServiceRegistry.get(CapabilityGrantStore::class.java) ?: return emptyMap()
        return try {
            store.decisionsFor(companionId)
        } catch (cancellation: kotlinx.coroutines.CancellationException) {
            throw cancellation
        } catch (_: Throwable) {
            emptyMap()
        }
    }

    /**
     * 一次 Agent 回合的完整审计落库（编排 dry_run + 规则指纹 + 工具名快照）。
     *
     * 对齐可观测性原则：决策在 Rust（dryRun 产出片段摘要），本方法仅做
     * 摘要序列化 + 近似指纹（规则拼接文本的 SHA-256，用于变更检测）+ 纯 IO 写入。
     */
    fun recordTurnAudit(
        context: Context,
        companionId: Long?,
        groupId: Long?,
        sessionId: String?,
        options: PromptOrchestratorOptions,
        query: String,
        roundsUsed: Int,
        toolNames: List<String>,
    ) {
        val summaries = dryRunPrompt(context, companionId, groupId, query, options)
        val rules = buildExtraSystemRules(context, companionId, groupId, query, options)
        val fingerprint = rules.joinToString("\n\n").let { text ->
            if (text.isBlank()) "" else sha256Hex(text)
        }
        promptAudit(context).record(
            companionId = companionId,
            groupId = groupId,
            sessionId = sessionId,
            fragments = summaries,
            roundsUsed = roundsUsed,
            systemPromptHash = fingerprint,
            toolNames = toolNames,
        )
    }

    // ── Agent 调度日志（可观测性，纯 IO 只写） ──

    /** 共享调度日志记录器（惰性创建）。 */
    @Volatile
    private var dispatchRecorder: AgentDispatchRecorder? = null

    /** 获取（惰性创建）共享 [AgentDispatchRecorder]。 */
    fun dispatchLog(context: Context): AgentDispatchRecorder =
        dispatchRecorder ?: synchronized(this) {
            dispatchRecorder ?: AgentDispatchRecorder(context).also { dispatchRecorder = it }
        }

    /**
     * 记录一次完整 Agent 回合调度日志（一次 runTurn 一条）。
     *
     * 与 [recordTurnAudit] 互补：audit 回答「为什么这样回复」（编排 dry_run），
     * 本方法回答「AI 是怎么跑的」——完整调度时间线：provider/model、起止时间、
     * 实际回合数、完成原因、错误、工具调用明细、事件流。
     *
     * @param dispatchId 本次调度唯一标识（建议复用 sessionId 或新 UUID）
     * @param startedAtMs 回合开始时间戳（调用方在 runTurn 前记录）
     * @param completedAtMs 回合结束时间戳（runTurn 返回后记录）
     * @param toolCalls AgentToolHost 采集的工具调用明细
     * @param events Rust 事件流（agentResult.events）
     */
    fun recordDispatchLog(
        context: Context,
        companionId: Long?,
        groupId: Long?,
        sessionId: String?,
        dispatchId: String,
        provider: String,
        model: String,
        startedAtMs: Long,
        completedAtMs: Long,
        roundsUsed: Int,
        finishedReason: String,
        error: String?,
        toolNames: List<String>,
        toolCalls: List<ToolCallRecord>,
        events: List<AgentEvent>,
        querySummary: String,
    ) {
        dispatchLog(context).record(
            companionId = companionId,
            groupId = groupId,
            sessionId = sessionId,
            dispatchId = dispatchId,
            provider = provider,
            model = model,
            startedAtMs = startedAtMs,
            completedAtMs = completedAtMs,
            roundsUsed = roundsUsed,
            finishedReason = finishedReason,
            error = error,
            toolNames = toolNames,
            toolCalls = toolCalls,
            events = events,
            querySummary = querySummary,
        )
    }

    private fun sha256Hex(text: String): String =
        runCatching {
            val digest = java.security.MessageDigest.getInstance("SHA-256")
            digest.digest(text.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
        }.getOrDefault("")

    // ── 网络探测服务（决策全在 Rust：模型列表 / 余额 / OpenAI 兼容测试 / Anthropic 测试） ──

    /**
     * 获取（惰性创建）共享 [ApiProbe]。
     *
     * 对齐旧 Kotlin `AiService` 的探测面（fetchModels / queryBalance / 协议测试），
     * 实现下沉 Rust（ureq 直连，原生 TLS），本门面只做轻量缓存。
     */
    @Volatile
    private var apiProbe: ApiProbe? = null

    /** 获取（惰性创建）共享 [ApiProbe]。 */
    fun apiProbe(): ApiProbe =
        apiProbe ?: synchronized(this) {
            apiProbe ?: ApiProbe().also { apiProbe = it }
        }

    /** 是否使用 Anthropic 协议（Rust 决策）。 */
    fun apiUsesAnthropicProtocol(provider: String, formatHint: String): Boolean =
        com.yunian.ai.agent.uniffi.apiUsesAnthropicProtocol(provider, formatHint)

    /** 是否支持 OpenAI 模型列表（Rust 决策）。 */
    fun apiSupportsOpenaiModelList(provider: String, formatHint: String): Boolean =
        com.yunian.ai.agent.uniffi.apiSupportsOpenaiModelList(provider, formatHint)

    /** 模型是否要求固定温度（kimi-k2.6，Rust 决策）。 */
    fun apiRequiresFixedTemperature(model: String): Boolean =
        com.yunian.ai.agent.uniffi.apiRequiresFixedTemperature(model)

    /** 家族均衡随机（Rust 决策）：按模型家族分组后均衡选取。 */
    fun apiFamilyBalancedRandom(candidates: List<String>): String =
        com.yunian.ai.agent.uniffi.apiFamilyBalancedRandom(candidates)
}
