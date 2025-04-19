use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_messenger::{self as messenger, Messenger as _};

mod create;
mod delete;
mod read;
mod size;
mod write;

impl Server {
	pub(crate) async fn write_pty_event(
		&self,
		pty: &tg::pty::Id,
		event: tg::pty::Event,
		master: bool,
	) -> tg::Result<messenger::StreamPublishInfo> {
		let name = if master {
			format!("{pty}_master")
		} else {
			format!("{pty}_slave")
		};
		let payload: Bytes = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		loop {
			let result = self
				.messenger
				.stream_publish(name.clone(), payload.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?
				.await;
			match result {
				Ok(info) => return Ok(info),
				Err(messenger::Error::MaxBytes | messenger::Error::MaxMessages) => {
					tokio::task::yield_now().await;
				},
				Err(source) => return Err(tg::error!(!source, "failed to publish message")),
			}
		}
	}
}
