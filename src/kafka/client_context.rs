use std::sync::Arc;

use rdkafka::{client::ClientContext, config::RDKafkaLogLevel, statistics::Statistics};

use crate::prometheus::prometheus::Prometheus;

pub struct MetricsContext {
    prometheus: Arc<Prometheus>,
}

impl MetricsContext {
    pub fn new(prometheus: Arc<Prometheus>) -> Self {
        Self { prometheus }
    }
}

impl ClientContext for MetricsContext {
    fn stats(&self, s: Statistics) {
        if let Err(e) = self.prometheus.monitor_rdkafka_metrics(s) {
            tracing::warn!(target: "librdkafka", "monitor_rdkafka_metrics failed: {}", e);
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                tracing::error!(target: "librdkafka", "{}: {}", fac, log)
            }
            RDKafkaLogLevel::Warning => {
                tracing::warn!(target: "librdkafka", "{}: {}", fac, log)
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!(target: "librdkafka", "{}: {}", fac, log)
            }
            RDKafkaLogLevel::Debug => tracing::debug!(target: "librdkafka", "{}: {}", fac, log),
        }
    }

    fn error(&self, err: rdkafka::error::KafkaError, reason: &str) {
        tracing::error!(target:"librdkafka", "Kafka error: {err:?} - {reason}");
    }
}
