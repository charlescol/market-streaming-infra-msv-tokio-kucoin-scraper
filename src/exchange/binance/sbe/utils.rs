use binance_spot_stream::{
    ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, depth_diff_stream_event_codec::SBE_TEMPLATE_ID,
    message_header_codec::MessageHeaderDecoder,
};

use crate::common::error::HeaderSbeError;

/// Verify wether the SBE message header match the expected schema.
/// # Arguments
/// - `header`: The SBE message header.
///
/// # Returns
/// Ok(()) if the header is valid.
/// Err(SBEError) if the header is invalid.
#[inline]
pub fn verify_header<'a>(header: &MessageHeaderDecoder<ReadBuf<'a>>) -> Result<(), HeaderSbeError> {
    if header.template_id() != SBE_TEMPLATE_ID {
        return Err(HeaderSbeError::InvalidTemplateId(
            header.template_id(),
            SBE_TEMPLATE_ID,
        ));
    }
    if header.schema_id() != SBE_SCHEMA_ID {
        return Err(HeaderSbeError::InvalidSchemaId(
            header.schema_id(),
            SBE_SCHEMA_ID,
        ));
    }
    if header.version() != SBE_SCHEMA_VERSION {
        return Err(HeaderSbeError::InvalidVersion(
            header.version(),
            SBE_SCHEMA_VERSION,
        ));
    }
    Ok(())
}

/// Build the Binance WebSocket stream.
/// # Arguments
/// - `symbols`: The list of symbols to subscribe to.
///
/// # Returns
/// String containing the Binance WebSocket stream.
pub fn build_binance_streams(symbols: &[String]) -> String {
    let mut stream = String::new();
    let separator = "@depth/";
    for symbol in symbols {
        stream.push_str(&symbol.to_lowercase());
        stream.push_str(separator);
    }
    stream.truncate(stream.len() - 1);
    return stream;
}
