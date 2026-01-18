use anyhow::Result;
use schema_core::depth_update_raw_v1::DepthUpdate;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct KucoinDepthUpdate {
    #[serde(rename = "type")]
    subject: String,
    data: KucoinDepthData,
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
    asks: Vec<Vec<String>>,
    bids: Vec<Vec<String>>,
}

pub fn to_depth_update(
    msg: &str,
    symbol_map: Option<&HashMap<String, String>>,
) -> Result<DepthUpdate> {
    let parsed: Value = serde_json::from_str(msg)?;

    // Check for welcome message or other types
    if let Some(msg_type) = parsed.get("type").and_then(|t| t.as_str()) {
        if msg_type != "message" {
            // Return error for non-update messages to be handled/ignored by caller
            // Or better, define a custom error? For now, let's try to parse as update
        }
    }

    let update: KucoinDepthUpdate = serde_json::from_value(parsed)?;

    Ok(DepthUpdate {
        event_type: update.subject,
        timestamp_micro: (update.data.timestamp * 1000) as i64,
        reception_time_micro: crate::common::utils::utc_micro::UtcMicro::now(),
        symbol: if let Some(map) = symbol_map {
            map.get(&update.data.symbol)
                .cloned()
                .unwrap_or(update.data.symbol)
        } else {
            update.data.symbol
        },
        event_first_update_id: update.data.sequence_start as i64,
        event_final_update_id: update.data.sequence_end as i64,
        bids_to_update: convert_entries(update.data.changes.bids),
        asks_to_update: convert_entries(update.data.changes.asks),
        // TODO: add Kucoin exchange to schema core
        exchange: 3,
        is_monitored: false,
    })
}

fn convert_entries(
    entries: Vec<Vec<String>>,
) -> Vec<schema_core::depth_update_raw_v1::OrderBookEntry> {
    entries
        .into_iter()
        .filter_map(|e| {
            // Price must be f64
            let price = e[0].parse::<f64>().ok()?;
            let quantity = e[1].parse::<f64>().ok()?; // Quantity expected as f64 per error msg

            Some(schema_core::depth_update_raw_v1::OrderBookEntry { price, quantity })
        })
        .collect()
}
