use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Error, Result};

use msv_tokio_kucoin_scraper::common::config::Config;
use msv_tokio_kucoin_scraper::kafka::publisher::KafkaPublisher;

use fastwebsockets::Frame;
use msv_tokio_kucoin_scraper::prometheus::handler::Handler;
use msv_tokio_kucoin_scraper::prometheus::prometheus::Prometheus;
use msv_tokio_kucoin_scraper::websocket::assigner::{Assigner, Group};
use msv_tokio_kucoin_scraper::websocket::connect::{
    connect_kucoin, create_kucoin_subscription_message, get_public_token,
};
use msv_tokio_kucoin_scraper::workflow::process_event::process_event;
use msv_tokio_kucoin_scraper::workflow::queued_event::QueuedEvent;
use msv_tokio_kucoin_scraper::workflow::read_ws_json::read_ws_json;
use schema_core::depth_update_raw_v1::DepthUpdate;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tracing::{error, info};

#[cfg(debug_assertions)]
use dotenv;
use tracing_appender::non_blocking;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let (writer, _guard) = non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    #[cfg(debug_assertions)]
    dotenv::dotenv().ok();

    let config = Config::new()?;
    info!("Config: {:?}", config.symbols);

    let mut builder = Builder::new_multi_thread();

    if let Some(threads) = config.tokio_worker_threads {
        info!("Tokio worker threads: {}", threads);
        builder.worker_threads(threads);
    }
    if let Some(queue_interval) = config.tokio_queue_interval {
        builder.global_queue_interval(queue_interval);
    }
    if let Some(event_interval) = config.tokio_event_interval {
        builder.event_interval(event_interval);
    }
    let rt = builder.enable_all().build()?;

    rt.block_on(run_app(config))
}

/// Run the application.
/// # Parameters
/// - `config`: the configuration of the application.
///
/// # Returns
/// - `Result<()>` if the application was successfully run.
async fn run_app(config: Config) -> Result<()> {
    info!("Start the metrics server");
    let prometheus = Arc::new(Prometheus::new(&config.monitoring)?);

    Handler::start_metrics_server(config.monitoring.metrics_port).await?;

    if !config.enable_scraper {
        info!("Scraper is disabled");
        tokio::signal::ctrl_c().await?;
        return Ok(());
    }

    info!("Initialize Kafka source");
    let kafka_publisher = Arc::new(KafkaPublisher::new(None, &config.kafka, prometheus.clone()));

    info!("Create the topology");
    create_topology(&config, kafka_publisher, prometheus).await?;

    Ok(())
}

/// Create the topology for the application.
///
/// # Arguments
/// - `config`: the application configuration.
/// - `kafka_publisher`: the Kafka publisher.
/// - `prometheus`: the Prometheus instance``
///
/// # Returns
/// - `Result<()>`: an error if the topology could not be created.
async fn create_topology(
    config: &Config,
    kafka_publisher: Arc<KafkaPublisher>,
    prometheus: Arc<Prometheus>,
) -> Result<()> {
    let stream_groups =
        Assigner::assign_round_robin(&config.symbols, config.workflow.stream_group_count)?;
    let process_groups: Vec<Group<String>> =
        Assigner::assign_round_robin(&config.symbols, config.workflow.process_group_count)?;
    let mut tasks: JoinSet<std::result::Result<(), Error>> = JoinSet::new();
    let mut process_tx: HashMap<String, Sender<QueuedEvent<DepthUpdate>>> = HashMap::new();

    for (group_id, process_group) in process_groups.iter().enumerate() {
        let (tx, rx): (
            Sender<QueuedEvent<DepthUpdate>>,
            Receiver<QueuedEvent<DepthUpdate>>,
        ) = mpsc::channel(config.workflow.process_queue_capacity);
        for symbol in &process_group.values {
            process_tx.insert(symbol.to_string(), tx.clone());
        }
        info!(
            "Processing group {} : {}",
            group_id,
            process_group.values.clone().join(", ")
        );
        let kafka_publisher_local = kafka_publisher.clone();
        let symbols = process_group.values.clone();
        let process_prometheus = prometheus.clone();
        let max_inflight = config.workflow.max_inflight_by_process_group;
        tasks.spawn(async move {
            process_event(
                rx,
                kafka_publisher_local,
                &symbols,
                process_prometheus,
                max_inflight,
            )
            .await?;
            Ok(())
        });
    }

    let kucoin_config = config.kucoin.clone();

    // TODO: Fetch token once for simplicity, though it expires. Real world usage should refresh.
    info!("Fetching Kucoin public token from {}", kucoin_config.host);
    let (token, endpoint) = get_public_token(&kucoin_config.host).await?;
    info!("Got Kucoin token, using endpoint: {}", endpoint);

    for (group_id, stream_group) in stream_groups.iter().enumerate() {
        let process_tx_local = process_tx.clone();
        let prometheus = prometheus.clone();
        let token = token.clone();
        let endpoint = endpoint.clone();
        let ping_interval_seconds = config.kucoin.ping_interval_seconds;
        let symbols = stream_group.values.clone();
        tasks.spawn(async move {
            let mut ws = connect_kucoin(&endpoint, &token).await?;

            // Subscribe
            let sub_msg = create_kucoin_subscription_message(&symbols);
            ws.write_frame(Frame::text(fastwebsockets::Payload::Owned(
                sub_msg.into_bytes(),
            )))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send subscription: {}", e))?;

            read_ws_json(
                ws,
                process_tx_local,
                group_id.to_string(),
                prometheus,
                ping_interval_seconds,
            )
            .await?;
            Ok(())
        });
    }

    // Spawn the Tokio runtime monitor task
    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    tasks.spawn(async move {
        prometheus.monitor_tokio(runtime_monitor).await?;
        return Ok(());
    });

    // Wait for all tasks to complete
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {
                info!("A task completed successfully.");
            }
            Ok(Err(e)) => {
                error!("A task returned an error: {:?}", e);
            }
            Err(e) => {
                error!("A task panicked: {:?}", e);
            }
        }
    }

    Ok(())
}
