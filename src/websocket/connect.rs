use fastwebsockets::{WebSocket, handshake};
use http_body_util::Empty;
use hyper::{
    Request,
    body::Bytes,
    header::{CONNECTION, HeaderValue, UPGRADE},
};
use hyper_util::rt::TokioIo;
use native_tls::TlsConnector;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector as TokioTls;

use crate::common::{enums::Format, error::WebSocketError, utils::spawn_exec::SpawnExec};

/// Connect to the WebSocket endpoint and return a WebSocket connection.
/// This function uses a TLS connection to the WebSocket endpoint.
///
/// # Arguments
/// - `host`: The WebSocket hostname including port.
/// - `port`: The WebSocket port.
/// - `req`: The ws request.
///
/// # Returns
/// Ok(WebSocket<TokioIo<hyper::upgrade::Upgraded>>) if the connection was successful.
/// Error(WebSocketError) if the connection failed
pub async fn connect_combined(
    host: &str,
    port: u16,
    req: Request<Empty<Bytes>>,
) -> Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>, WebSocketError> {
    // TCP + TLS
    let tcp = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;
    let tls = TokioTls::from(TlsConnector::new().map_err(map_err_ws)?)
        .connect(host, tcp)
        .await
        .map_err(map_err_ws)?;

    // Handshake WebSocket
    let (mut ws, _resp) = handshake::client(&SpawnExec, req, tls)
        .await
        .map_err(map_err_ws)?;
    ws.set_auto_pong(true);
    ws.set_auto_close(true);
    Ok(ws)
}

/// Connect to the WebSocket endpoint and return a WebSocket connection.
/// This function uses a direct TCP connection to the WebSocket endpoint.
///
/// # Arguments
/// - `host`: The WebSocket hostname including port.
/// - `port`: The WebSocket port.
/// - `req`: The ws request.
///
/// # Returns
/// Ok(WebSocket<TokioIo<hyper::upgrade::Upgraded>>) if the connection was successful.
pub async fn connect_combined_insecure(
    host: &str,
    port: u16,
    req: Request<Empty<Bytes>>,
) -> Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>, WebSocketError> {
    // TCP direct
    let tcp = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;

    let (ws, _) = handshake::client(&SpawnExec, req, tcp)
        .await
        .map_err(map_err_ws)?;
    Ok(ws)
}

/// Create a ws request.
/// # Arguments
/// - `symbols`: The list of symbols to subscribe to.
/// - `binance_ws_host`: The Binance WebSocket host.
/// - `format`: The format of the WebSocket messages.
/// - `api_key`: The Binance API key, required for SBE streams.
///
/// # Returns
/// Ok(Request<Empty<Bytes>>) if the request was created successfully.
/// Err(WebSocketError) if the request failed to be created.
pub fn create_binance_request(
    symbols: &[String],
    binance_ws_host: &str,
    format: &Format,
    api_key: &Option<String>,
) -> Result<Request<Empty<Bytes>>, WebSocketError> {
    // Compute separator depending on format
    let separator = match format {
        Format::Sbe => "@depth/",
        Format::Json => "@depth@100ms/",
    };

    // Build stream path
    let mut streams = String::new();
    for symbol in symbols {
        streams.push_str(&symbol.to_lowercase());
        streams.push_str(separator);
    }
    streams.truncate(streams.len() - 1);

    let path = format!("/stream?streams={streams}");

    // Build request
    let mut req = Request::builder()
        .method("GET")
        .uri(&path)
        .header("Host", binance_ws_host)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .map_err(map_err_ws)?;

    // Add API key if required
    if let Format::Sbe = format {
        let key = api_key.as_ref().ok_or(WebSocketError::ApiKeyRequired)?;
        req.headers_mut().insert(
            "X-MBX-APIKEY",
            HeaderValue::from_str(key).map_err(map_err_ws)?,
        );
    }

    Ok(req)
}

/// Map error to WebSocketError.
/// # Arguments
/// - `e`: The error to map.
///
/// # Returns
/// WebSocketError containing the error message.
fn map_err_ws<E: ToString>(e: E) -> WebSocketError {
    WebSocketError::CannotConnect(e.to_string())
}
