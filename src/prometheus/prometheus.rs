use crate::{
    common::{
        config::MonitoringConfig, constants::*, error::MonitoringError, utils::utc_micro::UtcMicro,
    },
    workflow::queued_event::QueuedEvent,
};
use prometheus::{
    Gauge, GaugeVec, Histogram, IntCounterVec, IntGauge, IntGaugeVec, register_gauge,
    register_histogram, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
};
use rand::random_bool;
use rdkafka::Statistics;
use schema_core::depth_update_raw_v1::DepthUpdate;
use std::time::Duration;
use tokio_metrics::RuntimeMonitor;
use tracing::{Level, info, span};

/// Prometheus struct. Contains all the metrics used by the application.
/// Uses the default Prometheus registry.
pub struct Prometheus {
    config: MonitoringConfig,
    pub workflow_metrics: WorflowMetrics,
    pub tokio_metrics: TokioMetrics,
    pub rd_kafka_metrics: Option<RdKafkaMetrics>,
    pub monitor_msg_metrics: Option<MonitorMsgMetrics>,
    pub monitor_all_symbols: bool,
}

pub struct WorflowMetrics {
    pub latency_process_buffer: Histogram,
    pub process_buffer_full: IntGaugeVec,
    pub exchange_to_scraper_latency_ms_vec: HistogramVec,
    pub scraper_rdkadka_latency_us_vec: HistogramVec,
}

use prometheus::HistogramVec;

pub struct TokioMetrics {
    pub tokio_task_running: IntGauge,
    pub tokio_busy_ratio: Gauge,
    pub tokio_poll_duration: Histogram,
    pub tokio_num_threads: IntGauge,
}

pub struct RdKafkaMetrics {
    // Top-level gauges
    pub msgq_bytes: IntGauge,
    pub msg_cnt: IntGauge,
    pub msg_bytes_max: IntGauge,
    pub msg_cnt_max: IntGauge,
    pub tx_total: IntGauge,
    pub txbytes_total: IntGauge,
    pub txmsgs_total: IntGauge,
    pub txmsg_bytes_total: IntGauge,
    // Per-broker gauges
    pub broker_outbuf_msg_cnt: IntGaugeVec,
    pub broker_waitresp_msg_cnt: IntGaugeVec,
    pub broker_rtt_microseconds: GaugeVec,
    pub broker_throttle_ms: GaugeVec,
}

pub struct MonitorMsgMetrics {
    pub update_count_total: IntCounterVec,
}

impl Prometheus {
    /// Create a new instance of the Prometheus struct.
    /// This uses the default Prometheus registry.
    ///
    /// # Parameters
    /// - `config`: the monitoring configuration.
    ///
    /// # Returns
    /// - `Ok(Self)` if the Prometheus struct was successfully created.
    /// - `Err(prometheus::Error)` if the Prometheus struct could not be created.
    pub fn new(config: &MonitoringConfig) -> Result<Self, prometheus::Error> {
        let monitor_all_symbols = config.monitor_symbols.is_empty();
        let mut rd_kafka_metrics = None;
        if config.monitor_rdkafka {
            rd_kafka_metrics = Some(RdKafkaMetrics {
                msgq_bytes: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_MSGQ_BYTES_NAME),
                    RDK_MSGQ_BYTES_DESC
                )?,
                msg_cnt: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_MSG_CNT_NAME),
                    RDK_MSG_CNT_DESC
                )?,
                msg_bytes_max: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_MSG_BYTES_MAX_NAME),
                    RDK_MSG_BYTES_MAX_DESC
                )?,
                msg_cnt_max: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_MSG_CNT_NAME_MAX),
                    RDK_MSG_CNT_NAME_MAX_DESC
                )?,
                tx_total: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_TX_TOTAL_NAME),
                    RDK_TX_TOTAL_DESC
                )?,
                txbytes_total: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_TXBYTES_TOTAL_NAME),
                    RDK_TXBYTES_TOTAL_DESC
                )?,
                txmsgs_total: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_TXMSGS_TOTAL_NAME),
                    RDK_TXMSGS_TOTAL_DESC
                )?,
                txmsg_bytes_total: prometheus::register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, RDK_TXMSG_BYTES_TOTAL_NAME),
                    RDK_TXMSG_BYTES_TOTAL_DESC
                )?,

                // Per-broker gauges
                broker_outbuf_msg_cnt: prometheus::register_int_gauge_vec!(
                    &format!("{}{}", METRICS_PREFIX, RDK_BROKER_OUTBUF_MSG_CNT_NAME),
                    RDK_BROKER_OUTBUF_MSG_CNT_DESC,
                    &["broker"]
                )?,
                broker_waitresp_msg_cnt: prometheus::register_int_gauge_vec!(
                    &format!("{}{}", METRICS_PREFIX, RDK_BROKER_WAITRESP_MSG_CNT_NAME),
                    RDK_BROKER_WAITRESP_MSG_CNT_DESC,
                    &["broker"]
                )?,
                broker_rtt_microseconds: prometheus::register_gauge_vec!(
                    &format!("{}{}", METRICS_PREFIX, RDK_BROKER_RTT_MICROSECONDS_NAME),
                    RDK_BROKER_RTT_MICROSECONDS_DESC,
                    &["broker"]
                )?,
                broker_throttle_ms: prometheus::register_gauge_vec!(
                    &format!("{}{}", METRICS_PREFIX, RDK_BROKER_THROTTLE_MS_NAME),
                    RDK_BROKER_THROTTLE_MS_DESC,
                    &["broker"]
                )?,
            });
        }

        let mut monitor_msg_metrics = None;
        if config.monitor_msg {
            monitor_msg_metrics = Some(MonitorMsgMetrics {
                update_count_total: register_int_counter_vec!(
                    &format!("{}{}", METRICS_PREFIX, UPDATE_COUNT_TOTAL_NAME),
                    UPDATE_COUNT_TOTAL_DESC,
                    &["symbol", "exchange"]
                )?,
            });
        }

        Ok(Self {
            config: config.clone(),
            workflow_metrics: WorflowMetrics {
                latency_process_buffer: register_histogram!(
                    &format!(
                        "{}{}",
                        METRICS_PREFIX, LATENCY_PROCESS_BUFFER_HISTOGRAM_NAME
                    ),
                    LATENCY_PROCESS_BUFFER_HISTOGRAM_DESCRIPTION,
                    prometheus::exponential_buckets(1.0, 2.0, 20)?
                )?,
                process_buffer_full: register_int_gauge_vec!(
                    &format!("{}{}", METRICS_PREFIX, PROCESS_BUFFER_FULL_COUNTER_NAME),
                    PROCESS_BUFFER_FULL_COUNTER_DESC,
                    &["group_id"]
                )?,
                exchange_to_scraper_latency_ms_vec: prometheus::register_histogram_vec!(
                    &format!(
                        "{}{}",
                        METRICS_PREFIX, EXCHANGE_TO_SCRAPER_LATENCY_HISTOGRAM_NAME
                    ),
                    EXCHANGE_TO_SCRAPER_LATENCY_HISTOGRAM_DESCRIPTION,
                    &["exchange"],
                    prometheus::exponential_buckets(1.0, 3.0, 9)?
                )?,
                scraper_rdkadka_latency_us_vec: prometheus::register_histogram_vec!(
                    &format!(
                        "{}{}",
                        METRICS_PREFIX, SCRAPER_RDKADKA_LATENCY_HISTOGRAM_NAME
                    ),
                    SCRAPER_RDKADKA_LATENCY_HISTOGRAM_DESCRIPTION,
                    &["exchange"],
                    prometheus::exponential_buckets(10.0, 3.0, 9)?
                )?,
            },
            tokio_metrics: TokioMetrics {
                tokio_task_running: register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, TOKIO_TASKS_RUNNING_NAME),
                    TOKIO_TASKS_RUNNING_DESC
                )?,
                tokio_busy_ratio: register_gauge!(
                    &format!("{}{}", METRICS_PREFIX, TOKIO_BUSY_RATIO_NAME),
                    TOKIO_BUSY_RATIO_DESC
                )?,
                tokio_poll_duration: register_histogram!(
                    &format!("{}{}", METRICS_PREFIX, TOKIO_POLL_DURATION_NAME),
                    TOKIO_POLL_DURATION_DESC,
                    prometheus::exponential_buckets(0.00001, 2.0, 10)?
                )?,
                tokio_num_threads: register_int_gauge!(
                    &format!("{}{}", METRICS_PREFIX, TOKIO_NUM_WORKERS_GAUGE_NAME),
                    TOKIO_NUM_WORKERS_GAUGE_DESC
                )?,
            },
            monitor_msg_metrics,
            rd_kafka_metrics,
            monitor_all_symbols,
        })
    }

    /// Increment the process buffer full counter.
    ///
    /// # Parameters
    /// - `group_id`: The group id of the process buffer
    ///
    /// # Returns
    /// - `Ok(())`: if the monitoring was successful
    /// - `Err(MonitoringError)`: if the monitoring failed
    pub fn monitor_process_buffer_full(&self, group_id: &str) -> Result<(), MonitoringError> {
        self.workflow_metrics
            .process_buffer_full
            .with_label_values(&[group_id])
            .inc();
        Ok(())
    }

    /// Check if the symbol should be monitored.
    /// The result is based on the monitor_ratio and the monitor_symbols configuration.
    ///
    /// # Parameters
    /// - `symbol`: the symbol to check
    ///
    /// # Returns
    /// - `true` if the symbol should be monitored
    /// - `false` if the symbol should not be monitored
    pub fn should_monitor(&self, symbol: &str) -> bool {
        random_bool(self.config.monitoring_ratio)
            && (self.monitor_all_symbols || self.config.monitor_symbols.contains(symbol))
    }

    /// Monitor the Tokio runtime.
    ///
    /// # Parameters
    /// - `runtime_monitor`: the Tokio runtime monitor
    ///
    /// # Returns
    /// Ok(()) if the monitoring was successful
    /// Err(MonitoringError) if the monitoring failed
    pub async fn monitor_tokio(
        &self,
        runtime_monitor: RuntimeMonitor,
    ) -> Result<(), MonitoringError> {
        for interval in runtime_monitor.intervals() {
            self.tokio_metrics
                .tokio_task_running
                .set(interval.live_tasks_count as i64);
            self.tokio_metrics
                .tokio_busy_ratio
                .set(interval.busy_ratio());
            self.tokio_metrics
                .tokio_poll_duration
                .observe(interval.mean_poll_duration.as_secs_f64());
            self.tokio_metrics
                .tokio_num_threads
                .set(interval.workers_count as i64);

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    /// Monitor the process stage of the workflow.
    ///
    /// # Parameters
    /// - `queued_event`: the event to process
    ///
    /// # Returns
    /// - `Ok(())`: if the monitoring was successful
    /// - `Err(MonitoringError)`: if the monitoring failed
    pub fn monitor_process_stage(
        &self,
        queued_event: &QueuedEvent<DepthUpdate>,
    ) -> Result<(), MonitoringError> {
        // Log if verbose ebnabled
        if tracing::enabled!(tracing::Level::INFO)
            && self.config.enable_metrics_verbose
            && self.config.track_symbols.contains(&queued_event.msg.symbol)
        {
            let span = span!(Level::INFO, "symbol", symbol = %queued_event.msg.symbol);
            span.in_scope(|| {
                info!(
                    "Producing event {} - {}",
                    queued_event.msg.event_first_update_id, queued_event.msg.event_final_update_id,
                );
            });
        }
        if queued_event.msg.is_monitored {
            // Exchange to scraper latency in ms
            self.workflow_metrics
                .exchange_to_scraper_latency_ms_vec
                .with_label_values(&[KUCOIN_EXCHANGE_NAME])
                .observe(
                    (queued_event.msg.reception_time_micro - queued_event.msg.timestamp_micro)
                        as f64,
                );

            // Scraper to rdkafka latency in us
            let timestamp_us = UtcMicro::now();

            self.workflow_metrics
                .scraper_rdkadka_latency_us_vec
                .with_label_values(&[KUCOIN_EXCHANGE_NAME])
                .observe((timestamp_us - queued_event.msg.reception_time_micro) as f64);

            // Process buffer latency in us
            if let Some(deq) = queued_event.deq {
                let latency_us = deq.duration_since(queued_event.enq).as_micros() as i64;
                self.workflow_metrics
                    .latency_process_buffer
                    .observe(latency_us as f64);
            }
        }

        if let Some(metrics) = self.monitor_msg_metrics.as_ref() {
            // Monitor the number of updates
            metrics
                .update_count_total
                .with_label_values(&[queued_event.msg.symbol.as_str(), KUCOIN_EXCHANGE_NAME])
                .inc_by(
                    (queued_event.msg.asks_to_update.len() + queued_event.msg.bids_to_update.len())
                        as u64,
                );
        }

        Ok(())
    }

    /// Monitor the rdkafka metrics. <p>
    /// Note: this is only enabled if the `MONITOR_RDKAFKA` environment variable is set to `true`.
    /// # Parameters
    /// - `s`: the statistics to monitor.
    ///
    /// # Returns    
    /// - `Ok(())` if the monitoring was successful.
    /// - `Err(MonitoringError)` if the monitoring failed.
    pub fn monitor_rdkafka_metrics(&self, s: Statistics) -> Result<(), MonitoringError> {
        if self.config.monitor_rdkafka {
            let rd_kafka_metrics = self.rd_kafka_metrics.as_ref().unwrap();
            rd_kafka_metrics.msgq_bytes.set(s.msg_size);
            rd_kafka_metrics.msg_cnt.set(s.msg_cnt);
            rd_kafka_metrics.msg_bytes_max.set(s.msg_size_max);
            rd_kafka_metrics.msg_cnt_max.set(s.msg_max);

            rd_kafka_metrics.tx_total.set(s.tx);
            rd_kafka_metrics.txbytes_total.set(s.tx_bytes);
            rd_kafka_metrics.txmsgs_total.set(s.txmsgs);
            rd_kafka_metrics.txmsg_bytes_total.set(s.txmsg_bytes);

            for (name, b) in &s.brokers {
                let lbl = [name.as_str()];

                rd_kafka_metrics
                    .broker_outbuf_msg_cnt
                    .with_label_values(&lbl)
                    .set(b.outbuf_msg_cnt as i64);

                rd_kafka_metrics
                    .broker_waitresp_msg_cnt
                    .with_label_values(&lbl)
                    .set(b.waitresp_msg_cnt as i64);

                let rtt_microseconds = b.rtt.as_ref().map(|t| t.avg as f64).unwrap_or(0.0);
                rd_kafka_metrics
                    .broker_rtt_microseconds
                    .with_label_values(&lbl)
                    .set(rtt_microseconds as f64);

                let throttle_ms = b.throttle.as_ref().map(|t| t.avg as f64).unwrap_or(0.0);
                rd_kafka_metrics
                    .broker_throttle_ms
                    .with_label_values(&lbl)
                    .set(throttle_ms);
            }
        }
        Ok(())
    }
}
