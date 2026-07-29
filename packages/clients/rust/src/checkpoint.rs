use std::collections::BTreeMap;

mod continue_;
mod unwatch;

pub mod wait;
pub mod watch;

#[derive(Clone, Debug, derive_more::From, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum ParamValue {
	#[from]
	Bool(bool),
	#[from(i8, i16, i32, i64, isize, serde_json::Number, u8, u16, u32, u64, usize)]
	Number(serde_json::Number),
	#[from]
	String(String),
}

pub type Params = BTreeMap<String, ParamValue>;

impl TryFrom<f32> for ParamValue {
	type Error = crate::Error;

	fn try_from(value: f32) -> Result<Self, Self::Error> {
		Self::try_from(f64::from(value))
	}
}

impl TryFrom<f64> for ParamValue {
	type Error = crate::Error;

	fn try_from(value: f64) -> Result<Self, Self::Error> {
		let number = serde_json::Number::from_f64(value)
			.ok_or_else(|| crate::error!(%value, "invalid checkpoint parameter number"))?;
		let value = number.into();
		Ok(value)
	}
}

impl From<&str> for ParamValue {
	fn from(value: &str) -> Self {
		value.to_owned().into()
	}
}

fn validate(checkpoint: &str) -> crate::Result<()> {
	if checkpoint.is_empty() || checkpoint.contains('/') {
		return Err(crate::error!(%checkpoint, "invalid checkpoint"));
	}
	Ok(())
}
