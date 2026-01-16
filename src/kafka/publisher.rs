use prost::{Message, bytes::BytesMut};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use schema_core::constants::BINANCEDEPTHUPDATERAW_V1_DEPTHUPDATE_SUBJECT;
use schema_registry_converter::{
    async_impl::{proto_raw::ProtoRawEncoder, schema_registry::SrSettings},
    schema_registry_common::SubjectNameStrategy,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::common::utils::utc_micro::UtcMicro;
use crate::common::{config::KafkaConfig, error::KafkaError};
use crate::kafka::client_context::MetricsContext;
use crate::prometheus::prometheus::Prometheus;

pub struct KafkaPublisher {
    producer: FutureProducer<MetricsContext>,
    topic: String,
    timeout: Duration,
    subject_strategy: SubjectNameStrategy,
    subject_name: String,
    encoder: ProtoRawEncoder<'static>,
}

impl KafkaPublisher {
    /// Create a new Kafka producer.
    /// # Arguments
    /// - `timeout`: the timeout for the Kafka producer
    /// - `config`: the Kafka configuration
    ///
    /// # Returns
    /// - `Self`: a new Kafka publisher
    /// - `Err(ConfigError)`: if the configuration is invalid
    pub fn new(
        timeout: Option<&Duration>,
        config: &KafkaConfig,
        prometheus: Arc<Prometheus>,
    ) -> Self {
        let mut config_builder: ClientConfig = ClientConfig::new();

        let context = MetricsContext::new(prometheus);

        for (key, value) in config.producer_config.iter() {
            info!("Kafka config: {} = {}", key, value);
            config_builder.set(key, value);
        }

        config_builder.set("bootstrap.servers", &config.broker_host);

        let subject_name = BINANCEDEPTHUPDATERAW_V1_DEPTHUPDATE_SUBJECT.to_string();

        KafkaPublisher {
            producer: config_builder
                .create_with_context(context)
                .expect("Producer creation error"),
            topic: config.topic.clone(),
            encoder: ProtoRawEncoder::new(SrSettings::new(config.schema_registry_url.clone())),
            subject_strategy: SubjectNameStrategy::RecordNameStrategy(subject_name.clone()),
            subject_name,
            timeout: *timeout.unwrap_or(&Duration::from_millis(2)),
        }
    }

    /// Encode a message using the schema registry.
    /// The message is serialized using the schema registry.
    /// # Arguments
    /// - `msg`: the message to publish
    ///
    /// # Returns
    /// - `kafkaError`: an error if the message could not be encoded
    /// - `Ok(Vec<u8>)`: if the message was successfully encoded
    pub async fn encode_payload<T>(&self, msg: &T) -> Result<Vec<u8>, KafkaError>
    where
        T: Message,
    {
        let mut buf = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut buf)
            .map_err(|e| KafkaError::CannotEncodeMessage(e.to_string()))?;

        let payload = self
            .encoder
            .encode(&buf, &self.subject_name, self.subject_strategy.clone())
            .await
            .map_err(|e| KafkaError::CannotEncodeMessage(e.to_string()))?;

        Ok(payload)
    }

    /// Publish a message to the kafka topic.
    /// # Arguments
    /// - `payload`: the payload to publish
    /// - `key`: the key to use for the message
    /// - `partition`: the partition to publish the message to
    /// - `timestamp_us`: the timestamp in microseconds since epoch to use for the message
    ///
    /// Note this function is not async, it returns a task that will be executed
    /// by rdkafka. This is useful to prevent bottlenecks when publishing many
    /// messages simultaneously.
    ///
    /// # Returns
    /// The future containing the result of the message production
    pub fn publish_static(
        this: Arc<Self>,
        payload: Vec<u8>,
        key: String,
        partition: Option<i32>,
    ) -> impl Future<Output = OwnedDeliveryResult> + Send + 'static {
        let topic = this.topic.clone();
        let timeout = this.timeout;

        let timestamp_us = UtcMicro::now();
        async move {
            let headers =
                OwnedHeaders::new().add("producer_enqueued_us", &timestamp_us.to_string());

            let record = FutureRecord::to(&topic)
                .payload(&payload)
                .key(&key)
                .timestamp(timestamp_us / 1000)
                .headers(headers)
                .partition(partition.unwrap_or(-1));

            this.producer.send(record, timeout).await
        }
    }
}
