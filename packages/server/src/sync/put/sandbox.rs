use {
	crate::{Session, sync::put::State},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

pub struct Item {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub send: bool,
	pub token: Option<tg::grant::Token>,
}

impl Session {
	pub(super) async fn sync_put_sandbox(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Item>,
	) -> tg::Result<()> {
		while let Some(item) = receiver.recv().await {
			self.sync_put_sandbox_item(&state, item).await?;
		}

		Ok(())
	}

	async fn sync_put_sandbox_item(&self, state: &State, item: Item) -> tg::Result<()> {
		// Authorize the sandbox.
		let permission =
			tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Read);
		let resource = tg::Referent::with_item_and_token(item.id.clone(), item.token.clone());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			self.sync_put_sandbox_finish_missing(state, &item).await;
			return Ok(());
		}

		// Read and validate the sandbox.
		let Some(sandbox) = self.try_get_sandbox_from_index(&item.id).await? else {
			self.sync_put_sandbox_finish_missing(state, &item).await;
			return Ok(());
		};
		let mut data = sandbox
			.data
			.ok_or_else(|| tg::error!(id = %item.id, "missing the sandbox data"))?;
		if data.id != item.id {
			return Err(tg::error!(
				expected = %item.id,
				actual = %data.id,
				"invalid sandbox id"
			));
		}
		if !data.status.is_destroyed() {
			return Err(tg::error!(id = %item.id, "cannot sync a running sandbox"));
		}
		data.token = None;

		// Get the processes.
		let children = if item.descendants && state.arg.sandbox_processes {
			self.server
				.index
				.get_sandbox_processes(&item.id)
				.await?
				.into_iter()
				.map(|(id, _)| tg::Id::from(id))
				.collect()
		} else {
			Vec::new()
		};

		// Send the sandbox.
		if item.send {
			let message = tg::sync::PutItemSandboxMessage {
				created_at: sandbox.created_at,
				data,
				id: item.id.clone(),
			};
			let message = tg::sync::PutMessage::Item(tg::sync::PutItemMessage::Sandbox(message));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the sandbox"))?;
		}

		// Update the graph and enqueue the processes.
		let id = item.id.clone().into();
		if item.send {
			state.graph.lock().unwrap().finish_item_remote_found(&id);
		}
		if item.descendants {
			state
				.graph
				.lock()
				.unwrap()
				.finish_item_remote_descendants(&id, &children);
			for child in children {
				state.queue.enqueue(item.eager, child, None)?;
			}
		}
		if state.graph.lock().unwrap().end_remote() {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_put_sandbox_finish_missing(&self, state: &State, item: &Item) {
		let id = tg::Id::from(item.id.clone());
		if item.send {
			let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
				selector: tg::Selector::Id(id.clone()),
				token: None,
			});
			state.sender.send(Ok(message)).await.ok();
			state.graph.lock().unwrap().finish_item_remote_missing(&id);
		}
		if item.descendants {
			state
				.graph
				.lock()
				.unwrap()
				.finish_item_remote_descendants(&id, &[]);
		}
		if state.graph.lock().unwrap().end_remote() {
			state.queue.close();
		}
	}
}
