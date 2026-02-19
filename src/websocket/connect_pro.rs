use anyhow::Result;
use fastwebsockets::{Frame, Payload, WebSocket, handshake};
use http_body_util::Empty;
use hyper::{
    Request,
    body::Bytes,
    header::{CONNECTION, UPGRADE},
};
use hyper_util::rt::TokioIo;
use native_tls::TlsConnector;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector as TokioTls;

use crate::common::{error::WebSocketError, utils::spawn_exec::SpawnExec};

/// Connect to KuCoin Pro WebSocket (public spot market data, no token).
pub async fn connect_kucoin_pro_spot_public()
-> Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>, WebSocketError> {
    let url = "wss://x-push-spot.kucoin.com";

    let uri: http::Uri = url
        .parse()
        .map_err(|e: http::uri::InvalidUri| WebSocketError::CannotConnect(e.to_string()))?;

    let host = uri
        .host()
        .ok_or(WebSocketError::CannotConnect("No host in URI".into()))?;
    let port = uri.port_u16().unwrap_or(443);

    // TCP + TLS
    let tcp = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;

    let tls_connector =
        TlsConnector::new().map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;
    let tls = TokioTls::from(tls_connector)
        .connect(host, tcp)
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;

    let req = Request::builder()
        .method("GET")
        .uri(url)
        .header("Host", host)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header("Sec-WebSocket-Key", handshake::generate_key())
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;

    let (mut ws, _) = handshake::client(&SpawnExec, req, tls)
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))?;

    // WS-level ping/pong frames handled automatically.
    ws.set_auto_pong(true);

    Ok(ws)
}

pub fn create_pro_ping_message() -> String {
    json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "type": "ping"
    })
    .to_string()
}

/// Pro Spot orderbook subscription (one message per symbol).
/// depth examples (depending on what KuCoin enables in your doc version): "1", "5", "50", "increment"
pub fn create_kucoin_pro_spot_orderbook_sub_messages(kucoin_symbols: &[String]) -> Vec<String> {
    kucoin_symbols
        .iter()
        .map(|symbol| {
            json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "action": "SUBSCRIBE",
                "channel": "obu",
                "symbol": symbol,
                "tradeType": "SPOT",
                "depth": "increment"
            })
            .to_string()
        })
        .collect()
}

/// Small helper to send a JSON text message.

pub async fn ws_send_text(
    ws: &mut WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    msg: &str,
) -> Result<(), WebSocketError> {
    ws.write_frame(Frame::text(Payload::Owned(msg.as_bytes().to_vec())))
        .await
        .map_err(|e| WebSocketError::CannotConnect(e.to_string()))
}
