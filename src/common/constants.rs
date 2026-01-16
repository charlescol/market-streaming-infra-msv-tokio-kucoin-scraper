pub const METRICS_PREFIX: &str = "tokio_binance_scraper_";

// ─────────────────────────────────────────────────────────────────────────────
// Workflow metrics
// ─────────────────────────────────────────────────────────────────────────────

pub const EXCHANGE_TO_SCRAPER_LATENCY_HISTOGRAM_NAME: &str =
    "exchange_to_scraper_latency_milliseconds";
pub const EXCHANGE_TO_SCRAPER_LATENCY_HISTOGRAM_DESCRIPTION: &str =
    "Latency in milliseconds for depth update from exchange to scraper";

pub const SCRAPER_RDKADKA_LATENCY_HISTOGRAM_NAME: &str = "scraper_rdkadka_latency_microseconds";
pub const SCRAPER_RDKADKA_LATENCY_HISTOGRAM_DESCRIPTION: &str = "Latency in microseconds for depth update from time received by scraper to time received by rdkadka";

pub const LATENCY_PROCESS_BUFFER_HISTOGRAM_NAME: &str = "process_buffer_latency_microseconds";
pub const LATENCY_PROCESS_BUFFER_HISTOGRAM_DESCRIPTION: &str =
    "Latency (queue - dequeue) in microseconds for process buffer";

pub const PROCESS_BUFFER_FULL_COUNTER_NAME: &str = "process_buffer_full";
pub const PROCESS_BUFFER_FULL_COUNTER_DESC: &str = "Number of times the process buffer was full";

// ─────────────────────────────────────────────────────────────────────────────
// Tokio native metrics
// ─────────────────────────────────────────────────────────────────────────────

pub const TOKIO_TASKS_RUNNING_NAME: &str = "tokio_tasks_running";
pub const TOKIO_TASKS_RUNNING_DESC: &str = "Number of currently active Tokio tasks";

pub const TOKIO_BUSY_RATIO_NAME: &str = "tokio_runtime_busy_ratio";
pub const TOKIO_BUSY_RATIO_DESC: &str = "Proportion of time worker threads were busy.";

pub const TOKIO_POLL_DURATION_NAME: &str = "tokio_poll_duration_seconds";
pub const TOKIO_POLL_DURATION_DESC: &str = "Mean duration of task polling in seconds";

pub const TOKIO_NUM_WORKERS_GAUGE_NAME: &str = "tokio_num_workers";
pub const TOKIO_NUM_WORKERS_GAUGE_DESC: &str = "Number of worker threads";

// ─────────────────────────────────────────────────────────────────────────────
// rdkafka metrics
// ─────────────────────────────────────────────────────────────────────────────

pub const RDK_MSGQ_BYTES_NAME: &str = "rdk_msgq_bytes";
pub const RDK_MSGQ_BYTES_DESC: &str = "Total size of messages currently in producer queue (bytes)";

pub const RDK_MSG_CNT_NAME: &str = "rdk_msg_cnt";
pub const RDK_MSG_CNT_DESC: &str = "Number of messages currently in producer queue";

pub const RDK_MSG_BYTES_MAX_NAME: &str = "rdk_msg_bytes_max";
pub const RDK_MSG_BYTES_MAX_DESC: &str =
    "Maximum size of messages currently in producer queue (bytes)";

pub const RDK_MSG_CNT_NAME_MAX: &str = "rdk_msg_cnt_max";
pub const RDK_MSG_CNT_NAME_MAX_DESC: &str =
    "Maximum number of messages currently in producer queue";

pub const RDK_TX_TOTAL_NAME: &str = "rdk_tx_total";
pub const RDK_TX_TOTAL_DESC: &str = "Total number of requests sent to Kafka brokers";

pub const RDK_TXBYTES_TOTAL_NAME: &str = "rdk_txbytes_total";
pub const RDK_TXBYTES_TOTAL_DESC: &str = "Total number of bytes transmitted to Kafka brokers";

pub const RDK_TXMSGS_TOTAL_NAME: &str = "rdk_txmsgs_total";
pub const RDK_TXMSGS_TOTAL_DESC: &str =
    "Total number of messages transmitted (produced) to Kafka brokers";

pub const RDK_TXMSG_BYTES_TOTAL_NAME: &str = "rdk_txmsg_bytes_total";
pub const RDK_TXMSG_BYTES_TOTAL_DESC: &str = "Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers";

pub const RDK_BROKER_OUTBUF_MSG_CNT_NAME: &str = "rdk_broker_outbuf_msg_cnt";
pub const RDK_BROKER_OUTBUF_MSG_CNT_DESC: &str = "Messages pending in socket buffer";

pub const RDK_BROKER_WAITRESP_MSG_CNT_NAME: &str = "rdk_broker_waitresp_msg_cnt";
pub const RDK_BROKER_WAITRESP_MSG_CNT_DESC: &str = "Messages awaiting broker response";

pub const RDK_BROKER_RTT_MICROSECONDS_NAME: &str = "rdk_broker_rtt_microseconds";
pub const RDK_BROKER_RTT_MICROSECONDS_DESC: &str = "Round-trip time to broker (µs)";

pub const RDK_BROKER_THROTTLE_MS_NAME: &str = "rdk_broker_throttle_ms";
pub const RDK_BROKER_THROTTLE_MS_DESC: &str = "Throttle time applied by broker (ms)";

// ─────────────────────────────────────────────────────────────────────────────
// Monitor message metrics
// ─────────────────────────────────────────────────────────────────────────────

pub const UPDATE_COUNT_TOTAL_NAME: &str = "update_count_total";
pub const UPDATE_COUNT_TOTAL_DESC: &str = "Total number of updates received";
