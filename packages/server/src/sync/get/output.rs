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
	pub(super) async fn sync_get_output(
		&self,
		state: &State,
		get: &[tg::Referent<tg::Selector<tg::Id>>],
	) -> tg::Result<()> {
		if get.is_empty() {
			return Ok(());
		}

		// Create the nodes with authorization tokens for permissions derived from the graph.
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
			get.iter()
				.filter_map(|node| match &node.node {
					tg::Selector::Id(id) => Some(id),
					tg::Selector::Specifier(_) => None,
				})
				.map(|id| self.sync_get_output_node(&graph, id, expires_at))
				.collect::<tg::Result<Vec<_>>>()?
		};

		// Send the get output.
		let message = tg::sync::GetOutputMessage { nodes };
		state
			.sender
			.send(Ok(tg::sync::GetMessage::Output(message)))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the get output"))?;

		Ok(())
	}

	fn sync_get_output_node(
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
			Some(Node::Process(_)) => graph.process_local_permissions(&id.clone().try_into()?),
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
