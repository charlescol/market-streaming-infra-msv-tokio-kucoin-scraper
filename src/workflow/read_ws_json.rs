use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use fastwebsockets::{Frame, OpCode, WebSocket};
use hyper_util::rt::TokioIo;
use schema_core::depth_update_raw_v1::DepthUpdate;
use serde_json::Value;
use tokio::sync::mpsc::{Sender, error::TrySendError};
use tracing::{debug, error, info, warn};

use crate::{
    common::error::WebSocketJsonError, exchange::kucoin::json::mapper::to_depth_update,
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
/// Ok(()) if the read was successful, or a WebSocketJsonError otherwise.
pub async fn read_ws_json(
    mut ws: WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    tx_dispatch: HashMap<String, Sender<QueuedEvent<DepthUpdate>>>,
    group_id: String,
    prometheus: Arc<Prometheus>,
    symbol_map: Option<HashMap<String, String>>,
) -> Result<(), WebSocketJsonError> {
    info!("Start reading frames (Kucoin)");

    let ping_interval = Duration::from_secs(18); // value recommanded by kucoin

    let mut ping_interval = tokio::time::interval(ping_interval);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Read frames
    loop {
        tokio::select! {
            // send ping at regular intervals
            _ = ping_interval.tick() => {
                 let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis();

                let ping_msg = format!("{{\"id\":\"{}\",\"type\":\"ping\"}}", now);
                debug!("Sending ping to Kucoin: {}", ping_msg);
                if let Err(e) = ws.write_frame(Frame::text(fastwebsockets::Payload::Owned(ping_msg.into_bytes()))).await {
                    error!("Failed to send ping: {}", e);
                    return Err(WebSocketJsonError::CannotReceiveMessage(e.to_string()));
                }
            }
            frame_res = ws.read_frame() => {
                // Check if frame_res is error
                 let Frame {
                    opcode, payload, ..
                } = match frame_res {
                    Ok(f) => f,
                    Err(e) => return Err(WebSocketJsonError::CannotReceiveMessage(e.to_string())),
                };

                match opcode {
                    OpCode::Text => {
                        let txt = std::str::from_utf8(&payload)
                            .map_err(|e| WebSocketJsonError::CannotParseMessage(e.to_string()))?;

                        if let Ok(json_val) = serde_json::from_str::<Value>(txt) {
                            if let Some(msg_type) = json_val.get("type").and_then(|t| t.as_str()) {
                                 match msg_type {
                                     "welcome" | "ack" | "pong" => {
                                         debug!("Received control message: {}", msg_type);
                                         continue;
                                     }
                                     "error" => {
                                         error!("Received error from Kucoin: {}", txt);
                                         continue;
                                     }
                                     _ => {}
                                 }
                            }
                        }

                        let event = match to_depth_update(txt, symbol_map.as_ref()) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!("Failed to parse Kucoin depth update: {} Text: {}", e, txt);
                                continue;
                            }
                        };

                        let tx = match tx_dispatch.get(&event.symbol) {
                            Some(tx) => tx,
                            None => {
                                debug!("Dropped message for unknown symbol: {}", event.symbol);
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
                                    "Internal buffer is {}, dropping message: symbol={}",
                                    kind,
                                    val.msg.symbol,
                                );
                            }
                        }
                    }
                    OpCode::Close => {
                        info!("WebSocket closed by server");
                        return Ok(());
                    }
                    OpCode::Ping => {
                    }
                    _ => {}
                }
            }
        }
    }
}
