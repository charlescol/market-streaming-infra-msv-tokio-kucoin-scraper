use chrono::{DateTime, Utc};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::error::TimestampError;

pub struct UtcMicro;
impl UtcMicro {
    /// Get the current time in microseconds since epoch.
    ///
    /// # Returns
    /// The current time in microseconds since epoch.
    pub fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .try_into()
            .expect("Cannot retrieve current time in microseconds since epoch")
    }

    /// Convert a timestamp in microseconds to a DateTime.
    ///
    /// # Arguments
    /// * `micros`: The timestamp in microseconds since epoch.
    ///
    /// # Returns
    /// The corresponding DateTime.
    pub fn to_datetime(micros: i64) -> Result<DateTime<Utc>, TimestampError> {
        let seconds = micros / 1_000_000;
        let micros_rem = (micros % 1_000_000) as u32;
        let nanos = micros_rem * 1_000;

        match DateTime::<Utc>::from_timestamp(seconds, nanos) {
            Some(dt) => Ok(dt),
            None => Err(TimestampError::InvalidTimestamp(micros.to_string())),
        }
    }
}
