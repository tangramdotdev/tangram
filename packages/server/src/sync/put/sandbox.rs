use {
	crate::{Session, sync::put::State},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

pub struct Node {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

impl Session {
	pub(super) async fn sync_put_sandbox(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Node>,
	) -> tg::Result<()> {
		while let Some(node) = receiver.recv().await {
			self.sync_put_sandbox_node(&state, node).await?;
		}

		Ok(())
	}

	async fn sync_put_sandbox_node(&self, state: &State, node: Node) -> tg::Result<()> {
		// Authorize the sandbox.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Read,
		);
		let resource = tg::Referent::with_node_and_token(node.id.clone(), node.token.clone());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			self.sync_put_sandbox_finish_missing(state, &node).await;
			return Ok(());
		}

		// Read and validate the sandbox.
		let Some(sandbox) = self.try_get_sandbox_from_index(&node.id).await? else {
			self.sync_put_sandbox_finish_missing(state, &node).await;
			return Ok(());
		};
		let mut data = sandbox
			.data
			.ok_or_else(|| tg::error!(id = %node.id, "missing the sandbox data"))?;
		if data.data.id != node.id {
			return Err(tg::error!(
				expected = %node.id,
				actual = %data.data.id,
				"invalid sandbox id"
			));
		}
		if !data.data.status.is_destroyed() {
			return Err(tg::error!(id = %node.id, "cannot sync a running sandbox"));
		}
		data.tokens.clear();

		// Get the processes.
		let children = if node.descendants && state.arg.sandbox_processes {
			self.server
				.index
				.get_sandbox_processes(&node.id)
				.await?
				.into_iter()
				.map(|(id, _)| tg::Id::from(id))
				.collect()
		} else {
			Vec::new()
		};

		// Send the sandbox.
		if node.send {
			let message = tg::sync::PutNodeSandboxMessage {
				created_at: sandbox.created_at,
				data,
				id: node.id.clone(),
			};
			let message = tg::sync::PutMessage::Node(tg::sync::PutNodeMessage::Sandbox(message));
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the sandbox"))?;
		}

		// Update the graph and enqueue the processes.
		let id = node.id.clone().into();
		if node.send {
			state.graph.lock().unwrap().finish_node_remote_found(&id);
		}
		if node.descendants {
			for child in &children {
				state.queue.enqueue(node.eager, child.clone(), None)?;
			}
			state
				.graph
				.lock()
				.unwrap()
				.finish_node_remote_descendants(&id, &children);
		}
		state.queue.finish_node();

		Ok(())
	}

	async fn sync_put_sandbox_finish_missing(&self, state: &State, node: &Node) {
		let id = tg::Id::from(node.id.clone());
		if node.send {
			let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
				selector: tg::Selector::Id(id.clone()),
				token: None,
			});
			state.sender.send(Ok(message)).await.ok();
			state.graph.lock().unwrap().finish_node_remote_missing(&id);
		}
		if node.descendants {
			state
				.graph
				.lock()
				.unwrap()
				.finish_node_remote_descendants(&id, &[]);
		}
		state.queue.finish_node();
	}
}
