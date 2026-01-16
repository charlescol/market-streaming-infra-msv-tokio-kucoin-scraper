use super::utils::deserialize_orderbook_entries;
use schema_core::binance_depth_update_raw_v1::OrderBookEntry;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BinanceWsEnvelope {
    pub data: BinanceDepthUpdateStream,
}

#[derive(Debug, Deserialize)]
pub struct BinanceDepthUpdateStream {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: i64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "U")]
    pub first_update_id: i64,

    #[serde(rename = "u")]
    pub final_update_id: i64,

    #[serde(rename = "b", deserialize_with = "deserialize_orderbook_entries")]
    pub bids_to_update: Vec<OrderBookEntry>,

    #[serde(rename = "a", deserialize_with = "deserialize_orderbook_entries")]
    pub asks_to_update: Vec<OrderBookEntry>,
}
