use {
	crate::{Component, put::State},
	futures::{StreamExt as _, TryStreamExt as _},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

pub struct Node {
	pub specifier: tg::Specifier,
}

impl Component {
	pub(super) async fn sync_put_resolve(
		&self,
		session: &Session,
		state: Arc<State>,
		receiver: async_channel::Receiver<Node>,
	) -> tg::Result<()> {
		let batch_size = session.server.config.sync.put.resolve.batch_size;
		let batch_timeout = session.server.config.sync.put.resolve.batch_timeout;
		tokio_stream::StreamExt::chunks_timeout(receiver, batch_size, batch_timeout)
			.map(Ok)
			.try_for_each(|nodes| {
				let state = state.clone();
				async move { self.sync_put_resolve_batch(session, &state, nodes).await }
			})
			.await?;

		Ok(())
	}

	async fn sync_put_resolve_batch(
		&self,
		session: &Session,
		state: &State,
		nodes: Vec<Node>,
	) -> tg::Result<()> {
		// Resolve the specifiers through the index.
		let specifiers = nodes
			.iter()
			.map(|node| node.specifier.clone())
			.collect::<Vec<_>>();
		let outputs = session
			.server
			.components
			.node
			.try_get_ids_for_specifiers_from_index(session, &specifiers)
			.await?;

		// Route resolved specifiers and report missing specifiers.
		for (node, output) in std::iter::zip(nodes, outputs) {
			let missing = output.is_none();
			state.queue.resolve(&node.specifier, output)?;
			if missing {
				let selector = tg::Selector::Specifier(node.specifier);
				let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
					selector,
					tokens: Vec::new(),
				});
				state.sender.send(Ok(message)).await.ok();
			}
		}
		state.queue.close_if_end();

		Ok(())
	}
}
