use {
	crate::{
		Component,
		request::{ArchiveRequestArg, RequestArg},
	},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_server_core::Server,
};

impl Component {
	pub(crate) async fn enqueue_object_archive(
		&self,
		server: &Server,
		object: tg::object::Id,
		put: [u8; 16],
	) -> tg::Result<()> {
		let start = rand::random::<u64>();
		let mut attempt = 0u64;
		let retry =
			tangram_futures::retry::Options::from(server.config.indexer.batch.retry.clone());
		tangram_futures::retry(&retry, || {
			let index = start.wrapping_add(attempt);
			let refresh = attempt > 0;
			attempt = attempt.wrapping_add(1);
			let object = object.clone();
			async move {
				let indexer = match self.select_indexer(server, index, refresh).await {
					Ok(indexer) => indexer,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				let result = self
					.enqueue_object_archive_with_indexer(server, &indexer, object, put)
					.await;
				match result {
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => Ok(ControlFlow::Continue(error)),
				}
			}
		})
		.await?;

		Ok(())
	}

	async fn enqueue_object_archive_with_indexer(
		&self,
		server: &Server,
		indexer: &tg::indexer::Id,
		object: tg::object::Id,
		put: [u8; 16],
	) -> tg::Result<()> {
		let arg = RequestArg::Archive(ArchiveRequestArg { object, put });
		let request = self.send_indexer_request(server, Some(indexer), arg);
		let output = tokio::time::timeout(server.config.indexer.batch.timeout, request)
			.await
			.map_err(|source| tg::error!(!source, "timed out enqueueing an object for archiving"))?
			.map_err(|source| tg::error!(!source, "failed to send an archive request"))?
			.map_err(|source| {
				tg::error!(!source, "the indexer failed to enqueue an archive entry")
			})?;
		output
			.try_unwrap_archive()
			.map_err(|_| tg::error!("expected an archive response"))?;

		Ok(())
	}
}
