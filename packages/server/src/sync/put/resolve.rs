use {
	crate::{Session, sync::put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
};

pub struct Node {
	pub specifier: tg::Specifier,
}

impl Session {
	pub(super) async fn sync_put_resolve(
		&self,
		state: Arc<State>,
		receiver: async_channel::Receiver<Node>,
	) -> tg::Result<()> {
		let batch_size = self.server.config.sync.put.resolve.batch_size;
		let batch_timeout = self.server.config.sync.put.resolve.batch_timeout;
		tokio_stream::StreamExt::chunks_timeout(receiver, batch_size, batch_timeout)
			.map(Ok)
			.try_for_each(|nodes| {
				let state = state.clone();
				async move { self.sync_put_resolve_batch(&state, nodes).await }
			})
			.await?;

		Ok(())
	}

	async fn sync_put_resolve_batch(&self, state: &State, nodes: Vec<Node>) -> tg::Result<()> {
		// Resolve the specifiers through the index.
		let specifiers = nodes
			.iter()
			.map(|node| node.specifier.clone())
			.collect::<Vec<_>>();
		let outputs = self
			.try_get_ids_for_specifiers_from_index(&specifiers)
			.await?;

		// Route resolved specifiers and report missing specifiers.
		for (node, output) in std::iter::zip(nodes, outputs) {
			let missing = output.is_none();
			state.queue.resolve(&node.specifier, output)?;
			if missing {
				let selector = tg::Selector::Specifier(node.specifier);
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
					selector,
					token: None,
				});
				state.sender.send(Ok(message)).await.ok();
			}
		}
		state.queue.close_if_end();

		Ok(())
	}
}
