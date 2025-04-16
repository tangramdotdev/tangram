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
	) -> tg::Result<
		impl Future<Output = Result<messenger::StreamPublishInfo, messenger::Error>> + Send,
	> {
		let name = if master {
			format!("{pty}_master")
		} else {
			format!("{pty}_slave")
		};
		let payload: Bytes = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		let future = self
			.messenger
			.stream_publish(name, payload.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		Ok(future)
	}
}
