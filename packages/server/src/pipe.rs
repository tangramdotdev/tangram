use crate::Server;
use tangram_client as tg;
use tangram_either::Either;

mod create;
mod delete;
mod get;
mod post;

impl Server {
	pub(crate) async fn send_pipe_event(
		&self,
		pipe: &tg::pipe::Id,
		event: tg::pipe::Event,
	) -> tg::Result<()> {
		let payload = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();
		match &self.messenger {
			Either::Left(messenger) => {
				messenger
					.streams()
					.publish(pipe.to_string(), payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?;
			},
			Either::Right(messenger) => {
				messenger
					.jetstream
					.publish(pipe.to_string(), payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?;
			},
		}
		Ok(())
	}
}
