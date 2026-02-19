use anyhow::Result;
use fastwebsockets::{WebSocket, handshake};
use http_body_util::Empty;
use hyper::{
    Request,
    body::Bytes,
    header::{CONNECTION, UPGRADE},
};
use hyper_util::rt::TokioIo;
use native_tls::TlsConnector;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_native_tls::TlsConnector as TokioTls;

use crate::common::{error::WebSocketError, utils::spawn_exec::SpawnExec};

#[derive(Debug, Deserialize)]
struct BulletPublicResponse {
    data: BulletPublicData,
}

#[derive(Debug, Deserialize)]
struct BulletPublicData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<InstanceServer>,
}

#[derive(Debug, Deserialize)]
struct InstanceServer {
    endpoint: String,
    // pingInterval: u64,
    // pingTimeout: u64,
}

/// Get a public token and endpoint from Kucoin API.
pub async fn get_public_token(host: &str) -> Result<(String, String)> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/bullet-public", host);

    let resp = client
        .post(&url)
        .send()
        .await?
        .json::<BulletPublicResponse>()
        .await?;

    let token = resp.data.token;
    let endpoint = resp
        .data
        .instance_servers
        .first()
        .ok_or(anyhow::anyhow!("No instance servers found"))?
        .endpoint
        .clone();

    Ok((token, endpoint))
}

/// Connect to Kucoin WebSocket.
pub async fn connect_kucoin_classic_spot_public(
    endpoint: &str,
    token: &str,
) -> Result<WebSocket<TokioIo<hyper::upgrade::Upgraded>>, WebSocketError> {
    let url = format!(
        "{}?token={}&connectId={}",
        endpoint,
        token,
        uuid::Uuid::new_v4()
    );
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
        .uri(&url)
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

    ws.set_auto_pong(true);

    Ok(ws)
}

pub fn create_kucoin_classic_spot_subscription_message(kucoin_symbols: &[String]) -> String {
    // Topic: /market/level2:BTC-USDT,ETH-USDT
    let topic_symbols = kucoin_symbols.join(",");

    json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "type": "subscribe",
        "topic": format!("/market/level2:{}", topic_symbols),
        "privateChannel": false,
        "response": true
    })
    .to_string()
}
