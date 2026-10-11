// temperature.rs — 请求体 temperature 的「2 位小数」归一（网关校验红线）
//
// === 线上实证（智谱清言通道，2026-10-11 用户报障）===
// 现象：智谱清言渠道聊天必失败，HTTP 400：
//     「temperature参数非法 限制小数点[2]位」
// 用户误判为 max_tokens 问题（改填 128000 无效）——根因与 max_tokens 无关。
//
// 根因：`ApiConfig.temperature` 在 Kotlin 侧是 Float，Room 写 SQLite REAL 列时按
// Float → Double **加宽**，带出二进制精度伪影：
//     0.7f  -> 0.699999988079071
//     0.85f -> 0.8500000238418579
// Rust 网关此前把这个 f64 **原样**写进请求体，serde_json 输出其最短往返表示
// （`"temperature":0.699999988079071`），凡是「限制小数点 2 位」的网关
// （智谱清言 glm 系、Clove 私有网关等）直接 400 拒绝整个请求。
//
// 为什么此前没修：Kotlin 侧早已有等价修法（core:network `ApiTemperature.toApiTemperature`），
// 但聊天/工具/识图生成与 Anthropic 通道都已下沉 Rust 网关，Rust 侧漏修——
// 于是「Kotlin 路径修好了、真正的线上路径仍然必现」。
//
// 修法：先放大 100 倍取整、再除以 100，得到干净的 2 位小数
// （0.699999988079071 → 0.7，serde_json 序列化为 `0.7`）。
// 区间与既有行为保持一致（clamp 到 [0.1, 1.5]）。
//
// 单一门控原则：**所有**把 temperature 写进请求体的点都必须经由 [normalize_temperature]，
// 禁止在请求构造处直接写 `cfg.temperature`。

/// 把 [0.1, 1.5] 区间内的 temperature 归一为干净的 2 位小数。
///
/// 与 Kotlin `ApiTemperature.toApiTemperature()`（`(coerceIn * 100).roundToInt() / 100.0`）
/// 逐位同语义，避免两条链路对同一配置产出不同请求体。
pub(crate) fn normalize_temperature(raw: f64) -> f64 {
    (raw.clamp(0.1, 1.5) * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 取证：Float 0.7 经 Float→Double 加宽后确实带伪影（本修复的存在理由）
    #[test]
    fn float_widening_produces_artifact() {
        let widened = 0.7f32 as f64;
        assert_ne!(widened, 0.7);
        assert_eq!(format!("{widened}"), "0.699999988079071");
    }

    /// 归一后必须是可被「2 位小数」网关接受的干净值
    #[test]
    fn normalize_removes_float_widening_artifact() {
        assert_eq!(normalize_temperature(0.7f32 as f64), 0.7);
        assert_eq!(format!("{}", normalize_temperature(0.7f32 as f64)), "0.7");
        assert_eq!(normalize_temperature(0.85f32 as f64), 0.85);
        assert_eq!(format!("{}", normalize_temperature(0.85f32 as f64)), "0.85");
        assert_eq!(normalize_temperature(0.3f32 as f64), 0.3);
        assert_eq!(normalize_temperature(0.95f32 as f64), 0.95);
    }

    /// 已是干净值 / 整数 / 边界值时保持稳定（不引入新偏差）
    #[test]
    fn normalize_is_stable_for_clean_values() {
        for v in [0.1, 0.2, 0.5, 0.7, 1.0, 1.25, 1.5] {
            assert_eq!(normalize_temperature(v), v, "干净值 {v} 不应被改变");
        }
    }

    /// clamp 区间与既有 `coerceIn(0.1f, 1.5f)` / `clamp(0.1, 1.5)` 行为一致
    #[test]
    fn normalize_clamps_to_existing_range() {
        assert_eq!(normalize_temperature(0.0), 0.1);
        assert_eq!(normalize_temperature(-3.0), 0.1);
        assert_eq!(normalize_temperature(9.9), 1.5);
    }

    /// 任意两位小数输入都不应再出现超过 2 位小数的序列化结果（穷举该区间）
    #[test]
    fn serialize_never_exceeds_two_decimals() {
        for hundredths in 10..=150i32 {
            let raw = (hundredths as f32 / 100.0) as f64; // 模拟 Room 读出的 Float 加宽值
            let normalized = normalize_temperature(raw);
            let text = format!("{normalized}");
            let decimals = text
                .split_once('.')
                .map(|(_, frac)| frac.trim_end_matches('0').len())
                .unwrap_or(0);
            assert!(
                decimals <= 2,
                "hundredths={hundredths} raw={raw} -> {text} 小数位={decimals}"
            );
        }
    }
}
