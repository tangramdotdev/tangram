use {bytes::Bytes, tangram_client::prelude::*, tangram_messenger::relay};

/// A stdio protocol message from the side that reads the stdio data back to the side that writes it.
pub(super) struct Ack(pub(super) tg::process::stdio::read::ClientMessage);

/// The stdio data carried on a relay.
pub(super) struct Data(pub(super) tg::process::stdio::write::Data);

/// The relay that carries a process's stdout and stderr from the runner, which accepts, to a reader, which connects.
pub(super) struct Output;

/// The relay that carries a process's stdin from a writer, which connects, to the runner, which accepts.
pub(super) struct Input;

pub(super) fn subject(process: &tg::process::Id, id: &str) -> String {
	format!("processes.{process}.stdio.{id}")
}

pub(super) fn abort(error: &tg::Error) -> Bytes {
	serde_json::to_vec(&error.to_data_or_id())
		.map(Bytes::from)
		.unwrap_or_default()
}

pub(super) fn error(error: relay::Error) -> tg::Error {
	match error {
		relay::Error::Aborted(payload) => {
			serde_json::from_slice::<tg::Either<tg::error::Data, tg::error::Id>>(&payload)
				.ok()
				.and_then(|error| error.try_into().ok())
				.unwrap_or_else(|| tg::error!("the stdio relay was aborted"))
		},
		relay::Error::Closed => tg::error!(unavailable, "the stdio relay closed"),
		relay::Error::Messenger(source) => {
			tg::error!(unavailable, !source, "the stdio relay failed")
		},
		relay::Error::Options(message) => tg::error!(%message, "invalid stdio relay options"),
		relay::Error::Reset => tg::error!(unavailable, "the stdio relay was reset"),
		relay::Error::Timeout => tg::error!(unavailable, "the stdio relay timed out"),
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

impl relay::Protocol for Output {
	type Accept = Data;
	type Connect = Ack;
}

impl relay::Protocol for Input {
	type Accept = Ack;
	type Connect = Data;
}
