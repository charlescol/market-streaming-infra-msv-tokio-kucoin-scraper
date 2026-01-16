use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Format {
    Json,
    Sbe,
}

impl FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Format::Json),
            "sbe" => Ok(Format::Sbe),
            other => Err(format!("Invalid format: {}", other)),
        }
    }
}
