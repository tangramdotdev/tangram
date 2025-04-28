use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_messenger::{self as messenger, prelude::*};

mod close;
mod create;
mod read;
mod write;

impl Server {
	pub(crate) async fn write_pipe_event(
		&self,
		id: &tg::pipe::Id,
		event: tg::pipe::Event,
	) -> tg::Result<u64> {
		let name = id.to_string();
		let payload: Bytes = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		loop {
			let result = self
				.messenger
				.stream_publish(name.clone(), payload.clone())
				.await
				.inspect_err(|e| tracing::error!("failed to write pipe: {e}"))
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?
				.await;
			match result {
				Ok(info) => return Ok(info),
				Err(messenger::Error::MaxBytes | messenger::Error::MaxMessages) => {
					tokio::time::sleep(std::time::Duration::from_millis(100)).await;
				},
				Err(source) => return Err(tg::error!(!source, "failed to publish the message")),
			}
		}
	}
}
