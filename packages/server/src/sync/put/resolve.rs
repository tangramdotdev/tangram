use {
	crate::{Session, sync::put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
};

pub struct Item {
	pub specifier: tg::Specifier,
}

impl Session {
	pub(super) async fn sync_put_resolve(
		&self,
		state: Arc<State>,
		receiver: async_channel::Receiver<Item>,
	) -> tg::Result<()> {
		let batch_size = self.server.config.sync.put.resolve.batch_size;
		let batch_timeout = self.server.config.sync.put.resolve.batch_timeout;
		tokio_stream::StreamExt::chunks_timeout(receiver, batch_size, batch_timeout)
			.map(Ok)
			.try_for_each(|items| {
				let state = state.clone();
				async move { self.sync_put_resolve_batch(&state, items).await }
			})
			.await?;

		Ok(())
	}

	async fn sync_put_resolve_batch(&self, state: &State, items: Vec<Item>) -> tg::Result<()> {
		// Resolve the specifiers through the index.
		let specifiers = items
			.iter()
			.map(|item| item.specifier.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_ids_for_specifiers_from_index(&specifiers)
			.await?;

		// Route resolved specifiers and report missing specifiers.
		for (item, output) in std::iter::zip(items, outputs) {
			let request = state
				.graph
				.lock()
				.unwrap()
				.resolve_remote_selector(&item.specifier)
				.ok_or_else(
					|| tg::error!(specifier = %item.specifier, "missing the selector request"),
				)?;
			if let Some(id) = output {
				state.graph.lock().unwrap().insert_remote_root(id.clone());
				let selector = tg::Selector::Specifier(item.specifier);
				state
					.queue
					.enqueue_database(crate::sync::queue::DatabaseItem {
						descendants: request.descendants,
						eager: request.eager,
						id,
						selector,
						token: request.token,
					});
			} else {
				let selector = tg::Selector::Specifier(item.specifier);
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
					selector,
					token: None,
				});
				state.sender.send(Ok(message)).await.ok();
			}
		}
		if state.graph.lock().unwrap().end_remote() {
			state.queue.close();
		}

		Ok(())
	}
}
