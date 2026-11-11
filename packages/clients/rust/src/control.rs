use {crate::prelude::*, std::ops::ControlFlow};

#[cfg(test)]
mod tests;

/// An event from a reconnecting control stream.
#[derive(Clone, Debug)]
pub enum Event<T> {
	Message(T),
	Reconnect,
}

/// A response to a control request, which carries the request's output, an error, or a reason to send the request again.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Response<T, R> {
	#[serde(skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0)]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[serde(skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 2)]
	pub output: Option<T>,

	#[serde(skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 3)]
	pub retry: Option<R>,
}

/// The reason that a holder of a process or a sandbox asks the sender of a request to send the request again.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Retry {
	/// The responder does not hold the item, so the request did nothing.
	#[tangram_serialize(id = 0)]
	NotHolder,

	/// The responder holds the item but cannot act on the request until the item starts, so the request did nothing.
	#[tangram_serialize(id = 1)]
	NotReady,
}

impl<T, R> Response<T, R> {
	#[must_use]
	pub fn with_error(id: String, error: tg::error::Data) -> Self {
		Self {
			error: Some(error),
			id,
			output: None,
			retry: None,
		}
	}

	#[must_use]
	pub fn with_output(id: String, output: T) -> Self {
		Self {
			error: None,
			id,
			output: Some(output),
			retry: None,
		}
	}

	#[must_use]
	pub fn with_retry(id: String, retry: R) -> Self {
		Self {
			error: None,
			id,
			output: None,
			retry: Some(retry),
		}
	}

	/// Decode the response, whose output breaks, whose retry continues, and whose error fails. A response that sets none, or more than one, fails, because it is malformed.
	pub fn into_result(self) -> tg::Result<ControlFlow<T, R>> {
		match (self.error, self.output, self.retry) {
			(None, Some(output), None) => Ok(ControlFlow::Break(output)),
			(None, None, Some(retry)) => Ok(ControlFlow::Continue(retry)),
			(Some(error), None, None) => {
				let error = tg::Error::try_from(error)
					.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
				Err(error)
			},
			(None, None, None) => Err(tg::error!(id = %self.id, "the response is empty")),
			_ => Err(tg::error!(
				id = %self.id,
				"the response has more than one of an output, an error, and a retry"
			)),
		}
	}
}
