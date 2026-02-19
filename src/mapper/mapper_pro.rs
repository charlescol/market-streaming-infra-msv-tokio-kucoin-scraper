use anyhow::Result;
use lexical_core::parse as fast_parse;
use schema_core::exchange_depth_update_raw_v1::{DepthUpdate, Exchange, OrderBookEntry};
use serde::{Deserialize, Deserializer};

use crate::common::error::WebSocketJsonError;

#[derive(Debug, Deserialize)]
struct KucoinWsMessage {
    #[serde(rename = "T")]
    subject: String,

    #[serde(rename = "P")]
    publish_ts: i64,

    #[serde(rename = "d")]
    data: KucoinDepthData,
}

#[derive(Debug, Deserialize)]
struct KucoinDepthData {
    #[serde(rename = "O")]
    sequence_start: i64,

    #[serde(rename = "C")]
    sequence_end: i64,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "a", deserialize_with = "deserialize_orderbook_entries")]
    asks: Vec<OrderBookEntry>,

    #[serde(rename = "b", deserialize_with = "deserialize_orderbook_entries")]
    bids: Vec<OrderBookEntry>,
}

/// Convert a KuCoin Pro "obu" depth message into a DepthUpdate struct.
pub fn to_depth_update(msg: &str) -> Result<DepthUpdate, WebSocketJsonError> {
    let parsed: KucoinWsMessage = serde_json::from_str(msg)
        .map_err(|_| WebSocketJsonError::CannotParseMessage(msg.to_string()))?;

    Ok(DepthUpdate {
        event_type: parsed.subject,
        timestamp_micro: (parsed.publish_ts / 1000) as i64,
        reception_time_micro: crate::common::utils::utc_micro::UtcMicro::now(),
        symbol: parsed.data.symbol,
        event_first_update_id: parsed.data.sequence_start,
        event_final_update_id: parsed.data.sequence_end,
        bids_to_update: parsed.data.bids,
        asks_to_update: parsed.data.asks,
        exchange: Exchange::Kucoin as i32,
        is_monitored: false,
    })
}

/// Deserialize a Kucoin orderbook entry (unchanged).
pub fn deserialize_orderbook_entries<'de, D>(
    deserializer: D,
) -> Result<Vec<OrderBookEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<Vec<&'de str>> = Deserialize::deserialize(deserializer)?;
    raw.into_iter()
        .map(|row| {
            let price_s = row
                .get(0)
                .copied()
                .ok_or_else(|| serde::de::Error::custom("missing price"))?;
            let qty_s = row
                .get(1)
                .copied()
                .ok_or_else(|| serde::de::Error::custom("missing qty"))?;

            let price = fast_parse::<f64>(price_s.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            let quantity = fast_parse::<f64>(qty_s.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;

            Ok(OrderBookEntry { price, quantity })
        })
        .collect()
}
