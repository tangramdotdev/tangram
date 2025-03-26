use crate::Server;
use futures::{StreamExt as _, TryStreamExt as _, future};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database as _, Query as _};
use tangram_either::Either;
use tangram_messenger::Messenger;
use tokio_stream::wrappers::IntervalStream;

mod create;
mod delete;
mod read;
mod write;

impl Server {
	pub(crate) fn pty_deleted(
		&self,
		pty: tg::pty::Id,
	) -> impl Future<Output = tg::Result<()>> + 'static {
		let server = self.clone();
		async move {
			// Create a stream that listens for delete notifications.
			let messenger = server
				.messenger
				.subscribe(format!("ptys.{pty}.deleted"), None)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pty.deleted stream"))?
				.map(|_message| Ok::<_, tg::Error>(()));

			// Create a timer to check if the pty still exists, in case we miss a notification message.
			let id = pty.clone();
			let interval = tokio::time::interval(std::time::Duration::from_secs(5));
			let timer = IntervalStream::new(interval)
				.then(move |_| {
					let server = server.clone();
					let id = id.clone();
					async move {
						let connection = server.database.connection().await.map_err(|source| {
							tg::error!(!source, "failed to get database connection")
						})?;
						let p = connection.p();
						let statement = formatdoc!(
							"
							select count(*) != 0
							from ptys
							where id = {p}1;
						"
						);
						let params = db::params![id];
						let exists = connection
							.query_one_value_into::<bool>(statement.into(), params)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to check if pty exists")
							})?;
						Ok::<_, tg::Error>(exists)
					}
				})
				.try_filter_map(|exists| future::ok((!exists).then_some(())));

			// Merge the streams.
			let stream = tokio_stream::StreamExt::merge(messenger, timer);

			// Wait for the pty to be deleted.
			std::pin::pin!(stream).try_next().await?;

			Ok::<_, tg::Error>(())
		}
	}

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
