use {
	crate::{
		Session,
		sync::{
			get::State,
			graph::{Graph, Node},
		},
	},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn sync_get_complete(&self, state: &State) -> tg::Result<()> {
		if state.arg.get.is_empty() {
			return Ok(());
		}

		// Create the nodes with tokens that prove the permissions derived from the graph.
		let expires_at = self.server.clock.unix_timestamp()?
			+ self
				.server
				.config
				.sync
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let nodes = {
			let graph = state.graph.lock().unwrap();
			state
				.arg
				.get
				.iter()
				.filter_map(|node| match &node.node {
					tg::Selector::Id(id) => Some(id),
					tg::Selector::Specifier(_) => None,
				})
				.map(|id| self.sync_get_complete_node(&graph, id, expires_at))
				.collect::<tg::Result<Vec<_>>>()?
		};

		// Send the complete message.
		let message = tg::sync::GetCompleteMessage { nodes };
		state
			.sender
			.send(Ok(tg::sync::GetMessage::Complete(message)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the complete message"))?;

		Ok(())
	}

	fn sync_get_complete_node(
		&self,
		graph: &Graph,
		id: &tg::Id,
		expires_at: i64,
	) -> tg::Result<tg::Referent<tg::Id>> {
		let permissions = match graph.nodes().get(id) {
			Some(Node::Object(node)) => {
				let proven = graph.object_local_permissions(&id.clone().try_into()?);
				let permission = tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				);
				if node.marked() || proven.contains(permission) {
					let subtree = node
						.local_availability()
						.is_some_and(|availability| availability.subtree);
					tg::authorization::permission::Set::Object(Graph::object_permissions(subtree))
				} else {
					proven
				}
			},
			Some(Node::Process(node)) => {
				let mut permissions = if node.marked() {
					let availability = node.local_availability().cloned().unwrap_or_default();
					tg::authorization::permission::Set::Process(Graph::process_permissions(
						&availability,
					))
				} else {
					tg::authorization::permission::Set::Process(
						tg::authorization::permission::process::Set::empty(),
					)
				};
				permissions.insert(graph.process_local_permissions(&id.clone().try_into()?));
				permissions
			},
			Some(
				Node::Group(_)
				| Node::Organization(_)
				| Node::Sandbox(_)
				| Node::Tag(_)
				| Node::User(_),
			)
			| None => return Ok(tg::Referent::with_node(id.clone())),
		};
		if permissions.is_empty() {
			return Ok(tg::Referent::with_node(id.clone()));
		}
		let token = self.create_token(id.clone(), permissions.iter().collect(), expires_at)?;
		let node = tg::Referent::with_node_and_local_tokens(id.clone(), token);

		Ok(node)
	}
}
