use crate::Server;
use tangram_client as tg;
use tangram_messenger::{self as messenger, Messenger as _};

mod create;
mod delete;
mod read;
mod write;

impl Server {
	pub(crate) async fn write_pipe_event(
		&self,
		id: &tg::pipe::Id,
		event: tg::pipe::Event,
	) -> tg::Result<
		impl Future<Output = Result<messenger::StreamPublishInfo, messenger::Error>> + Send,
	> {
		let name = id.to_string();
		let payload = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		let future = self
			.messenger
			.stream_publish(name, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		Ok(future)
	}
}
