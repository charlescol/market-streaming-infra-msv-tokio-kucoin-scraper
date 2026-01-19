use anyhow::Result;
use lexical_core::parse as fast_parse;
use schema_core::exchange_depth_update_raw_v1::{DepthUpdate, Exchange, OrderBookEntry};
use serde::{Deserialize, Deserializer};

use crate::common::error::WebSocketJsonError;

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum KucoinWsMessage {
    #[serde(rename = "message")]
    Message {
        data: KucoinDepthData,
        subject: String,
    },
}

#[derive(Debug, Deserialize)]
struct KucoinDepthData {
    changes: KucoinChanges,
    #[serde(rename = "sequenceStart")]
    sequence_start: u64,
    #[serde(rename = "sequenceEnd")]
    sequence_end: u64,
    symbol: String,
    #[serde(rename = "time")]
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct KucoinChanges {
    #[serde(deserialize_with = "deserialize_orderbook_entries")]
    asks: Vec<OrderBookEntry>,

    #[serde(deserialize_with = "deserialize_orderbook_entries")]
    bids: Vec<OrderBookEntry>,
}

/// Convert a Kucoin depth update message into a DepthUpdate struct.
/// The symbol is converted to a string without the first dash.
/// # Arguments
/// - `msg`: The message to convert.
///
/// # Returns
/// A Result containing the converted DepthUpdate struct or an error.
pub fn to_depth_update(msg: &str) -> Result<DepthUpdate, WebSocketJsonError> {
    let parsed: KucoinWsMessage = serde_json::from_str(msg)
        .map_err(|_| WebSocketJsonError::CannotParseMessage(msg.to_string()))?;

    let KucoinWsMessage::Message { subject, data } = parsed;

    Ok(DepthUpdate {
        event_type: subject,
        timestamp_micro: (data.timestamp * 1000) as i64,
        reception_time_micro: crate::common::utils::utc_micro::UtcMicro::now(),
        symbol: data.symbol,
        event_first_update_id: data.sequence_start as i64,
        event_final_update_id: data.sequence_end as i64,
        bids_to_update: data.changes.bids,
        asks_to_update: data.changes.asks,
        exchange: Exchange::Kucoin as i32,
        is_monitored: false,
    })
}

/// Deserialize a Kucoin orderbook entry.
/// The Kucoin orderbook entries are represented as a tuple of strings (price, quantity).
/// This function converts them into OrderBookEntry structs with f64 fields.
/// # Arguments
/// - `deserializer`: The deserializer to use.
///
/// # Returns
/// A vector of OrderBookEntry structs.
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
            // Parse price and quantity
            let price = fast_parse::<f64>(price_s.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            let quantity = fast_parse::<f64>(qty_s.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            Ok(OrderBookEntry { price, quantity })
        })
        .collect()
}
