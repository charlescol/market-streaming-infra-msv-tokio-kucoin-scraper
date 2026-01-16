use lexical_core::parse as fast_parse;
use schema_core::binance_depth_update_raw_v1::OrderBookEntry;
use serde::{Deserialize, Deserializer};

/// Deserialize a list of OrderBookEntry from a raw string.
/// The raw string is a list of tuple (price, quantity).
///
/// # Arguments
/// - `deserializer`: The deserializer to use.
///
/// # Returns
/// A list of OrderBookEntry.
pub fn deserialize_orderbook_entries<'de, D>(
    deserializer: D,
) -> Result<Vec<OrderBookEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<(String, String)> = Deserialize::deserialize(deserializer)?;
    raw.into_iter()
        .map(|(price, quantity)| {
            let price = fast_parse::<f64>(price.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            let quantity = fast_parse::<f64>(quantity.as_bytes())
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
            Ok(OrderBookEntry { price, quantity })
        })
        .collect()
}

/// Build the Binance WebSocket stream.
/// # Arguments
/// - `symbols`: The list of symbols to subscribe to.
///
/// # Returns
/// String containing the Binance WebSocket stream.
pub fn build_binance_streams(symbols: &[String]) -> String {
    let mut stream = String::new();
    let separator = "@depth@100ms/";
    for symbol in symbols {
        stream.push_str(&symbol.to_lowercase());
        stream.push_str(separator);
    }
    stream.truncate(stream.len() - 1);
    return stream;
}
