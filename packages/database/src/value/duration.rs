use {crate::Value, std::time::Duration};

use super::{DeserializeAs, Serialize};

#[derive(Clone, Copy, Debug)]
pub struct DurationSeconds(pub Option<Duration>);

impl DurationSeconds {
	pub fn validate(value: Option<Duration>) -> Result<(), &'static str> {
		if let Some(value) = value {
			if value.subsec_nanos() != 0 {
				return Err("duration must be a whole number of seconds");
			}
			i64::try_from(value.as_secs()).map_err(|_| "duration exceeds bounds")?;
		}
		Ok(())
	}
}

impl Serialize for DurationSeconds {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		Self::validate(self.0)?;
		let Some(value) = self.0 else {
			return Ok(Value::Null);
		};
		let seconds = i64::try_from(value.as_secs())?;
		Ok(Value::Integer(seconds))
	}
}

impl DeserializeAs<Duration> for DurationSeconds {
	fn deserialize_as(
		value: Value,
	) -> Result<Duration, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let seconds = value.try_unwrap_integer()?;
		let seconds = u64::try_from(seconds)?;
		Ok(Duration::from_secs(seconds))
	}
}
