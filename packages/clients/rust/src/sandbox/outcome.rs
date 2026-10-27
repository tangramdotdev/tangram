use crate::prelude::*;

pub use self::data::Data;

pub mod data;

#[derive(Clone, Debug)]
pub struct Outcome {
	pub error: Option<tg::Error>,
}

impl Outcome {
	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let error = data.error.map(tg::Error::try_from).transpose()?;
		Ok(Self { error })
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		Data {
			error: self.error.as_ref().map(tg::Error::to_data_or_id),
		}
	}
}

impl TryFrom<Data> for Outcome {
	type Error = tg::Error;

	fn try_from(value: Data) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}
