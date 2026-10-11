// lianyu_agent — LianYu Agent 中间层（Rust 核心）
//
// 设计边界（只读规划 Phase 2 定稿）：
// - Agent 层不依赖任何 Android/消息层代码，纯 Rust + UniFFI 导出。
// - 工具层：AgentTool 注册表 + 基础聊天工具（按标点分段输出 / 发送表情包 / 通用气泡）。
// - LLM 调用与消息落地副作用通过 UniFFI 接口回调 Kotlin（模型网关/工具宿主），
//   Rust 侧只负责：循环状态机、工具编排、分段算法、确认策略。
// - Kotlin 侧仅保留：UniFFI 绑定 + 回调实现（AiServiceProvider / MessageWriteCoordinator 适配），
//   不承载任何 Agent 决策逻辑。

pub mod agent;
pub mod agentlog;
pub mod api_probe;
pub mod card_import;
pub mod cordis_bridge;
pub mod lorebook;

// UniFFI 0.29 顶层函数不支持 Result<T, String> 抛错类型 → Option（None = 解析失败，详情在 Rust 侧 eprintln）
#[uniffi::export]
pub fn parse_character_card_png(bytes: Vec<u8>) -> Option<card_import::CharacterCardInfo> {
    card_import::parse_character_card_png(&bytes).ok()
}

#[uniffi::export]
pub fn parse_character_card_json(json: String) -> Option<card_import::CharacterCardInfo> {
    card_import::parse_character_card_json(&json).ok()
}
pub mod memory_selector;
pub mod native_gateway;
pub mod prompt_orchestrator;
pub(crate) mod retry;
pub mod segmenter;
pub mod skill_selector;
pub mod sticker_preference;
pub(crate) mod temperature;

pub use agent::*;
pub use api_probe::*;
pub use cordis_bridge::*;
pub use memory_selector::*;
pub use native_gateway::*;
pub use prompt_orchestrator::*;
pub use segmenter::*;
pub use skill_selector::*;
pub use sticker_preference::*;

uniffi::setup_scaffolding!();
