use crate::prelude::*;

pub use self::data::Data;

pub mod data;

#[derive(Clone, Debug)]
pub struct Outcome {
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

impl Outcome {
	pub(crate) fn inherit_location(&self, location: Option<&tg::Location>) {
		if let Some(error) = &self.error {
			error.state().inherit_location(location);
		}
		if let Some(output) = &self.output {
			output.inherit_location(location);
		}
	}

	pub(crate) fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		if let Some(error) = &self.error {
			error.state().inherit_tokens(tokens);
		}
		if let Some(output) = &self.output {
			output.inherit_tokens(tokens);
		}
	}

	pub fn into_output(self) -> tg::Result<tg::Value> {
		if let Some(error) = self.error {
			return Err(error);
		}
		match self.exit {
			0 => (),
			1..128 => {
				return Err(tg::error!("the process exited with code {}", self.exit));
			},
			128.. => {
				let signal = self.exit - 128;
				return Err(tg::error!("the process exited with signal {signal}"));
			},
		}
		let output = self.output.unwrap_or(tg::Value::Null);
		Ok(output)
	}
}

impl Outcome {
	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let error = data
			.error
			.map(|either| match either {
				tg::Either::Left(data) => {
					let object = tg::error::Object::try_from_data(data)?;
					Ok::<_, tg::Error>(tg::Error::with_object(object))
				},
				tg::Either::Right(error) => Ok(tg::Error::with_referent(error)),
			})
			.transpose()?;
		Ok(Self {
			error,
			exit: data.exit,
			output: data.output.map(TryInto::try_into).transpose()?,
		})
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		Data {
			error: self
				.error
				.as_ref()
				.map(|error| error.to_data_or_id().map_right(|_| error.to_referent())),
			exit: self.exit,
			output: self.output.as_ref().map(tg::Value::to_data),
		}
	}
}

impl TryFrom<Data> for Outcome {
	type Error = tg::Error;

	fn try_from(value: Data) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}
