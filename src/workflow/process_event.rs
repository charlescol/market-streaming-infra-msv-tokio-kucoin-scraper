use std::sync::Arc;

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};

use schema_core::binance_depth_update_raw_v1::DepthUpdate;
use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::{
    common::error::ProcessError, kafka::publisher::KafkaPublisher,
    prometheus::prometheus::Prometheus, workflow::queued_event::QueuedEvent,
};

/// Collect from the internal queue and publish to Kafka.
/// # Parameters
/// - `rx`: The internal queue receiver.
/// - `publisher`: The Kafka publisher.
/// - `symbols`: The list of symbols to process.
/// - `prometheus`: The Prometheus instance.
/// - `max_inflight`: The maximum number of inflight messages.
///
/// # Returns
/// Ok(()) if the dispatch was successful
/// Err(Error) if the dispatch failed
pub async fn process_event(
    mut rx: Receiver<QueuedEvent<DepthUpdate>>,
    publisher: Arc<KafkaPublisher>,
    symbols: &[String],
    prometheus: Arc<Prometheus>,
    max_inflight: usize,
) -> Result<(), ProcessError> {
    let formated_symbol_names = symbols.join(", ");

    let mut inflight = FuturesUnordered::new();

    loop {
        match rx.recv().await {
            None => {
                // The internal queue is closed
                return Err(ProcessError::ProcessClosed(formated_symbol_names));
            }
            Some(mut event) => {
                event.set_deq();

                event.msg.is_monitored = prometheus.should_monitor(&event.msg.symbol);

                // Encode the message
                let payload = match publisher.encode_payload(&event.msg).await {
                    Ok(payload) => payload,
                    Err(e) => {
                        error!("Error encoding message: {}", e);
                        continue;
                    }
                };

                // Record metrics and log if verbose enabled
                prometheus.monitor_process_stage(&event)?;

                // Publish to Kafka
                let future = KafkaPublisher::publish_static(
                    publisher.clone(),
                    payload,
                    event.msg.symbol.clone(),
                    None,
                );
                inflight.push(future);

                while inflight.len() >= max_inflight {
                    if let Some(res) = inflight.next().await {
                        if let Err((e, _msg)) = res {
                            error!("Error publishing message: {}", e);
                        }
                    }
                }

                // Check for completion without blocking
                loop {
                    match inflight.next().now_or_never() {
                        None => break,       // None is ready
                        Some(None) => break, // Empty or closed
                        Some(Some(res)) => {
                            if let Err((e, _)) = res {
                                error!("Error publishing message: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}
