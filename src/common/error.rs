use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingRequiredEnvironmentVariable(String),

    #[error("Configuration file not found: {0}")]
    ConfigFileNotFound(String),

    #[error("Failed to parse configuration: {0}")]
    ParseError(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("Internal dispatch queue is closed for symbols {0}")]
    DispatchClosed(String),

    #[error("Failed to monitor dispath stage: {0}")]
    FailedToMonitorDispatchStage(#[from] MonitoringError),
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("Internal processing queue is closed for symbols {0}")]
    ProcessClosed(String),

    #[error("Failed to monitor processing stage: {0}")]
    FailedToMonitorProcessingStage(#[from] MonitoringError),

    #[error("Failed to publish message: {0}")]
    CannotPublishMessage(#[from] KafkaError),
}

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("Failed to publish message: {0}")]
    CannotPublishMessage(String),

    #[error("Failed to encode message: {0}")]
    CannotEncodeMessage(String),
}

#[derive(Debug, Error)]
pub enum WebSocketError {
    #[error("Failed to connect to websocket: {0}")]
    CannotConnect(String),

    #[error("No symbol provided")]
    NoSymbolProvided,

    #[error("API key is required for the format")]
    ApiKeyRequired,
}

#[derive(Debug, Error)]
pub enum WebSocketJsonError {
    #[error("Failed to receive message: {0}")]
    CannotReceiveMessage(String),

    #[error("Failed to parse message: {0}")]
    CannotParseMessage(String),

    #[error("Failed to monitor websocket: {0}")]
    FailedToMonitorWebSocket(#[from] MonitoringError),
}

#[derive(Debug, Error)]
pub enum WebSocketSbeError {
    #[error("Failed to validate SBE header: {0}")]
    HeaderSbeError(#[from] HeaderSbeError),

    #[error("Failed to receive message: {0}")]
    CannotReceiveMessage(String),

    #[error("Failed to decode SBE message: {0}")]
    SbeError(#[from] SbeError),

    #[error("Failed to monitor websocket: {0}")]
    FailedToMonitorWebSocket(#[from] MonitoringError),
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("No channel found for symbol: {0}")]
    NoChannelFoundForSymbol(String),

    #[error("Failed to start metrics server: {0}")]
    CannotStartMetricsServer(String),

    #[error("Failed to receive TCP message: {0}")]
    CannotReceiveTcpMessage(String),
}

#[derive(Error, Debug)]
pub enum MonitoringError {
    #[error("Failed to monitor tokio: {0}")]
    FailedToMonitorTokio(String),

    #[error("Failed to monitor dispatch stage: {0}")]
    FailedToMonitorDispatchStage(String),

    #[error("Failed to monitor process stage: {0}")]
    FailedToMonitorProcessStage(String),
}

#[derive(Error, Debug)]
pub enum GroupAssignerError {
    #[error("Failed to assign groups: {0}")]
    FailedToAssignGroups(String),
}

#[derive(Error, Debug)]
pub enum TimestampError {
    #[error("Invalid timestamp format {0}")]
    InvalidTimestamp(String),
}

#[derive(Error, Debug)]
pub enum HeaderSbeError {
    #[error("Invalid version received {0} expected {1}")]
    InvalidVersion(u16, u16),

    #[error("Invalid schema id received {0} expected {1}")]
    InvalidSchemaId(u16, u16),

    #[error("Invalid template id received {0} expected {1}")]
    InvalidTemplateId(u16, u16),
}

#[derive(Debug, Error)]
pub enum SbeError {
    #[error("Failed to decode SBE message: {0}")]
    CannotDecodeMessage(String),
}
