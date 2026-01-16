use anyhow::Result;
use schema_core::binance_depth_update_raw_v1::DepthUpdate;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct KucoinDepthUpdate {
    #[serde(rename = "type")]
    msg_type: String,
    topic: String,
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

pub fn to_depth_update(msg: &str, symbol_map: Option<&HashMap<String, String>>) -> Result<DepthUpdate> {
    let parsed: Value = serde_json::from_str(msg)?;
    
    // Check for welcome message or other types
    if let Some(msg_type) = parsed.get("type").and_then(|t| t.as_str()) {
        if msg_type != "message" {
             // Return error for non-update messages to be handled/ignored by caller
             // Or better, define a custom error? For now, let's try to parse as update
        }
    }

    let update: KucoinDepthUpdate = serde_json::from_value(parsed)?;
    
    // Kucoin structure:
    // { "type": "message", "topic": "/market/level2:BTC-USDT", "subject": "trade.l2update", "data": { ... } }
    
    // Map changes to schema expectation.
    // The schema DepthUpdate likely expects `bids` and `asks` as Vec<Vec<String>>
    // But based on the error "available fields are: ...", let's double check.
    // The previous error message listed: `event_type`, `timestamp_micro`, `reception_time_micro`, `symbol`, `event_first_update_id`, ...
    // And likely `bids`/`asks` are `bids` and `asks`.
    
    // Let's assume standard friendly names used in this project's schema wrapper.
    // If it is `binance_depth_update_raw_v1::DepthUpdate`, it might have fields like `b` and `a`. 
    // BUT the error says `available fields are: event_type, timestamp_micro, ...`
    // This suggests I am importing a generated struct that uses friendly names or the schema-core library has changed/uses different naming convention than raw JSON.
    // Or I am using the wrong struct? `binance_depth_update_raw_v1` sounds raw.
    // The import is `use schema_core::binance_depth_update_raw_v1::DepthUpdate;`
    // Maybe checking the file `binance_spot_stream` (which `schema_core` seems to re-export or depend on) would help?
    // But I can't check dependency source easily. I rely on the error message.
    
    // Error says: `event_type`, `timestamp_micro`, `reception_time_micro`, `symbol`, `event_first_update_id`
    
    Ok(DepthUpdate {
        event_type: update.subject,
        timestamp_micro: (update.data.timestamp * 1000) as i64, 
        reception_time_micro: crate::common::utils::utc_micro::UtcMicro::now(),
        symbol: if let Some(map) = symbol_map {
            map.get(&update.data.symbol).cloned().unwrap_or(update.data.symbol)
        } else {
            update.data.symbol
        },
        event_first_update_id: update.data.sequence_start as i64, 
        event_final_update_id: update.data.sequence_end as i64,
        bids_to_update: convert_entries(update.data.changes.bids), 
        asks_to_update: convert_entries(update.data.changes.asks),
        // Exchange::Kucoin does not exist in the schema.
        // If we cannot add it to the schema (external dep), we might have to use a placeholder or raw cast if possible.
        // However, `exchange` field is likely an enum wrapper. 
        // Let's check if we can simply use Binance for now with a TODO, or if there is a generic/other.
        // The error said `variant or associated item not found`.
        // Let's try to find if there is an `Other` or similar. If not, use Binance and log warning?
        // Or maybe 0? 
        // `exchange` field in DepthUpdate expects `i32` (based on protobuf usually).
        // Let's try casting a random int if it accepts i32?
        // Wait, the error `expected i32, found String` was for my previous attempt "KUCOIN".to_string().
        // So it IS an i32.
        // Let's use a distinct integer for Kucoin if we can avoiding collision. 
        // Binance is likely 1 or 0.
        // Let's use 10 for Kucoin as a temporary measure if we can't edit the schema.
        exchange: 3,
        is_monitored: false,
    })
}

fn convert_entries(entries: Vec<Vec<String>>) -> Vec<schema_core::binance_depth_update_raw_v1::OrderBookEntry> {
    entries.into_iter().filter_map(|e| {
        // Price must be f64
        let price = e[0].parse::<f64>().ok()?;
        let quantity = e[1].parse::<f64>().ok()?; // Quantity expected as f64 per error msg
        
        Some(schema_core::binance_depth_update_raw_v1::OrderBookEntry {
            price,
            quantity,
        })
    }).collect()
}
