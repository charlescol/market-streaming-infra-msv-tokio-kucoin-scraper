use std::{convert::Infallible, net::SocketAddr};

use http_body_util::Full;
use hyper::{Request, Response, body::Bytes};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;

use prometheus::{Encoder, TextEncoder, gather};
use tokio::net::TcpListener;
use tracing::error;

use crate::common::error::HandlerError;

pub struct Handler;

impl Handler {
    /// Serve the metrics.
    /// # Parameters
    /// - `req`: the request.
    ///
    /// # Returns
    /// Ok(Response<Full<Bytes>>) if the request was served.
    async fn metrics_handler(
        req: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        match req.uri().path() {
            "/metrics" => {
                let encoder = TextEncoder::new();
                let metric_families = gather();
                let mut buffer = Vec::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                Ok(Response::builder()
                    .status(200)
                    .header("Content-Type", encoder.format_type())
                    .body(Full::new(Bytes::from(buffer)))
                    .unwrap())
            }
            "/health" => Ok(Response::builder()
                .status(200)
                .body(Full::new(Bytes::from("OK")))
                .unwrap()),

            _ => Ok(Response::builder()
                .status(404)
                .body(Full::new(Bytes::from("Not Found")))
                .unwrap()),
        }
    }

    /// Start the metrics server.
    /// # Parameters
    /// - `metric_port`: the port to listen on.
    ///
    /// # Returns    
    /// Ok(tokio::task::JoinHandle<()>) if the server was started.
    /// Start the metrics server.
    /// # Parameters
    /// - `metric_port`: the port to listen on.
    ///
    /// # Returns    
    /// Ok(tokio::task::JoinHandle<()>) if the server was started.
    pub async fn start_metrics_server(
        metric_port: u16,
    ) -> Result<tokio::task::JoinHandle<()>, HandlerError> {
        let addr = SocketAddr::from(([0, 0, 0, 0], metric_port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| HandlerError::CannotStartMetricsServer(e.to_string()))?;

        Ok(tokio::spawn(async move {
            if let Err(e) = Self::run_server(listener).await {
                tracing::error!("Metrics server failed: {:?}", e);
            }
        }))
    }

    /// Listen for incoming TCP connections on the specified port.
    /// # Parameters
    /// - `listener`: the tcp listener.
    ///
    /// # Returns
    /// - `Ok(())` if the server was successfully started.
    /// - `Err(HandlerError)` if the server could not be started.
    async fn run_server(listener: TcpListener) -> Result<(), HandlerError> {
        loop {
            // When an incoming TCP connection is received grab a TCP stream for
            // client-server communication.
            let (stream, _) = listener
                .accept()
                .await
                .map_err(|e| HandlerError::CannotReceiveTcpMessage(e.to_string()))?;
            let io = TokioIo::new(stream);

            // Spin up a new task in Tokio so we can continue to listen for new TCP connection on the
            // current task without waiting for the processing of the HTTP/2 connection we just received
            // to finish
            tokio::task::spawn(async move {
                // Execute the target function
                let status = http1::Builder::new()
                    .serve_connection(io, service_fn(Self::metrics_handler))
                    .await;
                match status {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error serving metrics: {:?}", e);
                    }
                }
            });
        }
    }
}
