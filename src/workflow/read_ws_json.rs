use std::{collections::HashMap, sync::Arc};

use fastwebsockets::{Frame, OpCode, WebSocket};
use hyper_util::rt::TokioIo;
use schema_core::binance_depth_update_raw_v1::DepthUpdate;
use tokio::sync::mpsc::{Sender, error::TrySendError};
use tracing::{error, info};

use crate::{
    common::error::WebSocketJsonError, exchange::binance::json::mapper::to_depth_update,
    prometheus::prometheus::Prometheus, workflow::queued_event::QueuedEvent,
};

/// Read WebSocket frames deserializing them into depth update events.
/// The events are sent to an internal queue for routing.
///
/// # Parameters
/// - `ws`: The WebSocket connection.
/// - `tx`: The internal queue sender.
/// - `group_id`: The group identifier.
/// - `prometheus`: The Prometheus metrics collector.
///
/// # Returns
/// Ok(()) if the read was successful
/// Err(WebSocketError) if the read failed
pub async fn read_ws_json(
    mut ws: WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    tx_dispatch: HashMap<String, Sender<QueuedEvent<DepthUpdate>>>,
    group_id: String,
    prometheus: Arc<Prometheus>,
) -> Result<(), WebSocketJsonError> {
    info!("Start reading frames");
    // Read frames
    loop {
        let Frame {
            opcode, payload, ..
        } = ws
            .read_frame()
            .await
            .map_err(|e| WebSocketJsonError::CannotReceiveMessage(e.to_string()))?;
        match opcode {
            OpCode::Text => {
                let txt = std::str::from_utf8(&payload)
                    .map_err(|e| WebSocketJsonError::CannotParseMessage(e.to_string()))?;
                let event = match to_depth_update(&txt) {
                    Ok(e) => e,
                    Err(e) => {
                        error!("Failed to parse message: {:?}", e);
                        continue;
                    }
                };
                let tx = match tx_dispatch.get(&event.symbol) {
                    Some(tx) => tx,
                    None => {
                        error!("Received unknown symbol {}, ignoring event.", event.symbol);
                        continue;
                    }
                };
                let result = tx.try_send(QueuedEvent::new(event));
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        let (kind, val) = match e {
                            TrySendError::Full(val) => {
                                prometheus.monitor_process_buffer_full(&group_id)?;
                                ("full", val)
                            }
                            TrySendError::Closed(val) => ("closed", val),
                        };

                        error!(
                            "Internal buffer is {}, dropping message: symbol={}, first_id={}, final_id={}",
                            kind,
                            val.msg.symbol,
                            val.msg.event_first_update_id,
                            val.msg.event_final_update_id
                        );
                    }
                }
            }
            OpCode::Close => {
                info!("WebSocket closed by server");
                return Ok(());
            }
            _ => {}
        }
    }
}
