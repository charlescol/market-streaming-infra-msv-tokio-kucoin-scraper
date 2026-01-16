use std::time::{SystemTime, UNIX_EPOCH};

use super::types::{BinanceDepthUpdateStream, BinanceWsEnvelope};
use schema_core::binance_depth_update_raw_v1::DepthUpdate;
use serde_json::Result;

impl From<BinanceDepthUpdateStream> for DepthUpdate {
    fn from(stream: BinanceDepthUpdateStream) -> Self {
        let reception_time_micro = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .try_into()
            .expect("timestamp overflow");

        let exchange = schema_core::exchange_depth_snapshot_raw_v1::Exchange::Binance.into();

        DepthUpdate {
            event_type: stream.event_type,
            timestamp_micro: stream.event_time * 1000,
            reception_time_micro,
            symbol: stream.symbol,
            event_first_update_id: stream.first_update_id,
            event_final_update_id: stream.final_update_id,
            bids_to_update: stream.bids_to_update,
            asks_to_update: stream.asks_to_update,
            exchange,
            is_monitored: false,
        }
    }
}

/**
 * Extract the binance depth update from the json string.
 * Return the extracted prost protobuf struct
 */
pub fn to_depth_update(json: &str) -> Result<DepthUpdate> {
    let envelope: BinanceWsEnvelope = serde_json::from_str(json)?;
    Ok(envelope.data.into())
}
