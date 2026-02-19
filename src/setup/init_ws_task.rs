use anyhow::Result;
use fastwebsockets::Frame;
use schema_core::exchange_depth_update_raw_v1::DepthUpdate;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::Sender;

use crate::{
    prometheus::prometheus::Prometheus,
    websocket::{
        connect_classic::{
            connect_kucoin_classic_spot_public, create_kucoin_classic_spot_subscription_message,
        },
        connect_pro::{
            connect_kucoin_pro_spot_public, create_kucoin_pro_spot_orderbook_sub_messages,
            ws_send_text,
        },
    },
    workflow::{
        queued_event::QueuedEvent, read_ws_json_classic::read_ws_json_classic,
        read_ws_json_pro::read_ws_json_pro,
    },
};

/// Initialize the WebSocket connection for the pro API.
pub async fn init_ws_pro(
    symbols: &[String],
    process_tx: Arc<HashMap<String, Sender<QueuedEvent<DepthUpdate>>>>,
    group_id: String,
    prometheus: Arc<Prometheus>,
    ping_interval_seconds: u64,
) -> Result<()> {
    let mut ws = connect_kucoin_pro_spot_public().await?;

    let subs = create_kucoin_pro_spot_orderbook_sub_messages(symbols);
    for sub in subs {
        ws_send_text(&mut ws, &sub).await?;
    }

    read_ws_json_pro(ws, process_tx, group_id, prometheus, ping_interval_seconds).await?;

    Ok(())
}

/// Initialize the WebSocket connection for the Classic API.
pub async fn init_ws_classic(
    endpoint: &str,
    token: &str,
    symbols: &[String],
    process_tx: Arc<HashMap<String, Sender<QueuedEvent<DepthUpdate>>>>,
    group_id: String,
    prometheus: Arc<Prometheus>,
    ping_interval_seconds: u64,
) -> Result<()> {
    let mut ws = connect_kucoin_classic_spot_public(endpoint, token).await?;

    let sub_msg = create_kucoin_classic_spot_subscription_message(symbols);
    ws.write_frame(Frame::text(fastwebsockets::Payload::Owned(
        sub_msg.into_bytes(),
    )))
    .await?;

    read_ws_json_classic(ws, process_tx, group_id, prometheus, ping_interval_seconds).await?;

    Ok(())
}
