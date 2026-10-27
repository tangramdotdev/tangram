use {bytes::Bytes, tangram_client::prelude::*, tangram_messenger::relay};

pub(super) use crate::__tangram_server_process_stdio_relay_attempt as attempt;

/// A stdio protocol message from the side that reads the stdio data back to the side that writes it.
pub(super) struct Ack(pub(super) tg::process::stdio::read::ClientMessage);

/// The stdio data carried on a relay.
pub(super) struct Data(pub(super) tg::process::stdio::write::Data);

/// Get the subject on which the side that writes the stdio data sends it.
pub(super) fn data_subject(process: &tg::process::Id, id: &str) -> String {
	format!("processes.{process}.stdio.{id}.data")
}

/// Get the subject on which the side that reads the stdio data sends its acknowledgments.
pub(super) fn ack_subject(process: &tg::process::Id, id: &str) -> String {
	format!("processes.{process}.stdio.{id}.ack")
}

pub(super) fn abort(error: &tg::Error) -> Bytes {
	serde_json::to_vec(&error.to_data_or_id())
		.map(Bytes::from)
		.unwrap_or_default()
}

/// Determine whether a relay error ends a relay that the relay's peer can reopen, which a closed, failed, reset, or timed out relay does.
pub(super) fn is_retry(error: &relay::Error) -> bool {
	match error {
		relay::Error::Closed
		| relay::Error::Messenger(_)
		| relay::Error::Reset
		| relay::Error::Timeout => true,
		relay::Error::Aborted(_) | relay::Error::Options(_) => false,
	}
}

pub(super) fn error(error: relay::Error) -> tg::Error {
	match error {
		relay::Error::Aborted(payload) => {
			serde_json::from_slice::<tg::Either<tg::error::Data, tg::error::Id>>(&payload)
				.ok()
				.and_then(|error| error.try_into().ok())
				.unwrap_or_else(|| tg::error!("the stdio relay was aborted"))
		},
		relay::Error::Closed => tg::error!("the stdio relay closed"),
		relay::Error::Messenger(source) => {
			tg::error!(!source, "the stdio relay failed")
		},
		relay::Error::Options(message) => tg::error!(%message, "invalid stdio relay options"),
		relay::Error::Reset => tg::error!("the stdio relay was reset"),
		relay::Error::Timeout => tg::error!("the stdio relay timed out"),
	}
}

impl tangram_messenger::Payload for Ack {
	fn serialize(&self) -> Result<Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}
}

impl tangram_messenger::Payload for Data {
	fn serialize(&self) -> Result<Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let data = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(data))
	}
}

/// Get the value of a relay result, continuing the attempt if the relay ended in a way that its peer can reopen and failing it otherwise.
#[macro_export]
#[doc(hidden)]
macro_rules! __tangram_server_process_stdio_relay_attempt {
	($result:expr) => {
		match $result {
			Ok(value) => value,
			Err(error) if $crate::stdio::relay::is_retry(&error) => {
				let error = $crate::stdio::relay::error(error);
				return Ok(::std::ops::ControlFlow::Continue(error));
			},
			Err(error) => return Err($crate::stdio::relay::error(error)),
		}
	};
}
