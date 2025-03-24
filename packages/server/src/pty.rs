use crate::Server;
use tangram_client as tg;
use tangram_either::Either;

mod create;
mod delete;
mod read;
mod write;

impl Server {
	pub(crate) async fn send_pty_event(
		&self,
		pty: &tg::pty::Id,
		event: tg::pty::Event,
		master: bool,
	) -> tg::Result<()> {
		let payload = serde_json::to_vec(&event)
			.map_err(|source| tg::error!(!source, "failed to serialize the event"))?
			.into();

		match &self.messenger {
			Either::Left(messenger) => {
				let subject = if master {
					format!("{pty}.master")
				} else {
					format!("{pty}.slave")
				};
				eprintln!("(send) {subject}: {payload:?}");
				messenger
					.streams()
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?;
			},
			Either::Right(messenger) => {
				let subject = if master {
					format!("{pty}_master")
				} else {
					format!("{pty}_slave")
				};
				messenger
					.jetstream
					.publish(subject, payload)
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?
					.await
					.map_err(|source| tg::error!(!source, "failed to send the pipe event"))?;
			},
		}
		Ok(())
	}
}
