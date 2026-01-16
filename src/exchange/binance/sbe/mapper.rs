use binance_spot_stream::depth_diff_stream_event_codec::DepthDiffStreamEventDecoder;
use schema_core::binance_depth_update_raw_v1::{DepthUpdate, OrderBookEntry};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::error::SbeError;

/// Trait to decode SBE messages into DepthUpdate prost struct
pub trait SbeDecode<'a> {
    /// Decode the SBE message into DepthUpdate struct
    /// # Arguments
    /// - `decoder`: The SBE decoder
    ///
    /// # Returns
    /// Ok(DepthUpdate) if the decoding was successful
    /// Err(SbeError) if the decoding failed
    fn decode_from_sbe(decoder: DepthDiffStreamEventDecoder<'a>) -> Result<Self, SbeError>
    where
        Self: Sized;

    /// Decode the bids from the SBE message
    /// # Arguments
    /// - `decoder`: The SBE decoder
    /// - `price_scale`: The price scale
    /// - `qty_scale`: The quantity scale
    ///
    /// # Returns
    /// Ok(Vec<OrderBookEntry>) if the decoding was successful
    /// Err(SbeError) if the decoding failed
    fn decode_bids(
        decoder: DepthDiffStreamEventDecoder<'a>,
        price_scale: f64,
        qty_scale: f64,
    ) -> Result<(Vec<OrderBookEntry>, DepthDiffStreamEventDecoder<'a>), SbeError>
    where
        Self: Sized;

    /// Decode the asks from the SBE message
    /// # Arguments
    /// - `decoder`: The SBE decoder
    /// - `price_scale`: The price scale
    /// - `qty_scale`: The quantity scale
    ///
    /// # Returns
    /// Ok(Vec<OrderBookEntry>) if the decoding was successful
    /// Err(SbeError) if the decoding failed
    fn decode_asks(
        decoder: DepthDiffStreamEventDecoder<'a>,
        price_scale: f64,
        qty_scale: f64,
    ) -> Result<(Vec<OrderBookEntry>, DepthDiffStreamEventDecoder<'a>), SbeError>
    where
        Self: Sized;
}
impl<'a> SbeDecode<'a> for DepthUpdate {
    fn decode_from_sbe(decoder: DepthDiffStreamEventDecoder<'a>) -> Result<Self, SbeError> {
        let reception_time_micro = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SbeError::CannotDecodeMessage(e.to_string()))?
            .as_micros()
            .try_into()
            .expect("timestamp overflow");

        let price_scale = 10f64.powi(decoder.price_exponent() as i32);
        let qty_scale = 10f64.powi(decoder.qty_exponent() as i32);

        let (bids_to_update, decoder) = Self::decode_bids(decoder, price_scale, qty_scale)?;
        let (asks_to_update, mut decoder) = Self::decode_asks(decoder, price_scale, qty_scale)?;

        let symbol_coords = decoder.symbol_decoder();
        let bytes = decoder.symbol_slice(symbol_coords);
        let symbol = unsafe { std::str::from_utf8_unchecked(bytes) }.to_string();

        let exchange = schema_core::exchange_depth_snapshot_raw_v1::Exchange::Binance.into();
        Ok(DepthUpdate {
            event_type: "depthUpdate".to_string(),
            timestamp_micro: decoder.event_time(),
            reception_time_micro,
            symbol,
            event_first_update_id: decoder.first_book_update_id(),
            event_final_update_id: decoder.last_book_update_id(),
            bids_to_update,
            asks_to_update,
            exchange,
            is_monitored: false,
        })
    }

    fn decode_bids(
        decoder: DepthDiffStreamEventDecoder<'a>,
        price_scale: f64,
        qty_scale: f64,
    ) -> Result<(Vec<OrderBookEntry>, DepthDiffStreamEventDecoder<'a>), SbeError> {
        let mut bids = decoder.bids_decoder();
        let mut entries = Vec::with_capacity(bids.count() as usize);

        while let Some(_) = bids
            .advance()
            .map_err(|e| SbeError::CannotDecodeMessage(e.to_string()))?
        {
            let price = (bids.price() as f64) * price_scale;
            let quantity = (bids.qty() as f64) * qty_scale;
            entries.push(OrderBookEntry { price, quantity });
        }

        let decoder = bids
            .parent()
            .map_err(|e| SbeError::CannotDecodeMessage(e.to_string()))?;

        Ok((entries, decoder))
    }

    fn decode_asks(
        decoder: DepthDiffStreamEventDecoder<'a>,
        price_scale: f64,
        qty_scale: f64,
    ) -> Result<(Vec<OrderBookEntry>, DepthDiffStreamEventDecoder<'a>), SbeError> {
        let mut asks = decoder.asks_decoder();
        let mut entries = Vec::with_capacity(asks.count() as usize);

        while let Some(_) = asks
            .advance()
            .map_err(|e| SbeError::CannotDecodeMessage(e.to_string()))?
        {
            let price = (asks.price() as f64) * price_scale;
            let quantity = (asks.qty() as f64) * qty_scale;
            entries.push(OrderBookEntry { price, quantity });
        }

        let decoder = asks
            .parent()
            .map_err(|e| SbeError::CannotDecodeMessage(e.to_string()))?;

        Ok((entries, decoder))
    }
}
