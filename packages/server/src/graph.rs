use {
	crate::{Server, Session},
	std::sync::Arc,
	tangram_client::prelude::*,
};

pub(crate) type Cache = moka::sync::Cache<tg::graph::Id, Arc<tg::graph::Data>>;

const CACHE_CAPACITY: u64 = 1_000_000;

#[must_use]
pub(crate) fn cache() -> Cache {
	moka::sync::Cache::builder()
		.max_capacity(CACHE_CAPACITY)
		.weigher(|_, data: &Arc<tg::graph::Data>| {
			u32::try_from(data.nodes.len()).unwrap_or(u32::MAX)
		})
		.build()
}

/// Resolve an internal edge using its containing graph.
pub fn resolve_edge<T>(
	server: &Server,
	edge: tg::graph::data::Edge<T>,
	graph: Option<&tg::graph::Id>,
) -> tg::Result<tg::graph::data::Edge<T>> {
	let tg::graph::data::Edge::Index(index) = edge else {
		return Ok(edge);
	};
	let graph = graph.ok_or_else(|| tg::error!("missing graph"))?;
	let data = get_data(server, graph)?;
	let kind = data
		.nodes
		.get(index)
		.ok_or_else(|| tg::error!("invalid node index"))?
		.kind();
	let pointer = tg::graph::data::Pointer {
		graph: graph.clone(),
		index,
		kind,
	};
	Ok(tg::graph::data::Edge::Pointer(pointer))
}

/// Load a graph node without resolving its edges.
pub fn get_node(
	server: &Server,
	graph: &tg::graph::Id,
	index: usize,
) -> tg::Result<tg::graph::data::Node> {
	let data = get_data(server, graph)?;
	let node = data
		.nodes
		.get(index)
		.ok_or_else(|| tg::error!("invalid node index"))?
		.clone();
	Ok(node)
}

/// Load a graph's data from the graph cache, or decode it from the object cache and add it to the graph cache.
pub fn get_data(server: &Server, graph: &tg::graph::Id) -> tg::Result<Arc<tg::graph::Data>> {
	// Get the data from the graph cache.
	if let Some(data) = server.graphs.get(graph) {
		return Ok(data);
	}

	// Decode the data from the object cache.
	let (_, data) = server
		.cache
		.try_get_object_data_sync(&graph.clone().into())
		.map_err(|error| tg::error!(!error, "failed to get the graph"))?
		.ok_or_else(|| tg::error!("missing graph"))?;
	let data: tg::graph::Data = data
		.try_into()
		.map_err(|_| tg::error!("expected a graph"))?;
	let data = Arc::new(data);
	server.graphs.insert(graph.clone(), data.clone());

	Ok(data)
}

impl Session {
	/// Get the data of a locally stored graph that the session is authorized to read, decoding each graph at most once while it remains in the graph cache.
	pub(crate) async fn try_get_graph_data_local(
		&self,
		id: &tg::graph::Id,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<(Arc<tg::graph::Data>, crate::authorization::Output)>> {
		// Authorize the read of the graph.
		let object = tg::object::Id::from(id.clone());
		let resource = tg::Referent::with_node_and_local_tokens(
			object.clone(),
			tokens.local_authorization().to_vec(),
		);
		let authorization = self
			.authorize_object_read(resource, false)
			.await?
			.check_exhaustion()?;
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		if !authorization.permissions.contains(node) {
			return Ok(None);
		}

		// Get the data from the graph cache.
		if let Some(data) = self.server.graphs.get(id) {
			return Ok(Some((data, authorization)));
		}

		// Read and decode the data, then add it to the graph cache.
		let Some(output) = self.server.try_get_object_local(&object, false).await? else {
			return Ok(None);
		};
		let data = tg::graph::Data::deserialize(output.bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the graph"))?;
		let data = Arc::new(data);
		self.server.graphs.insert(id.clone(), data.clone());

		Ok(Some((data, authorization)))
	}

	/// Add a refreshed token for a graph, created from the authorization of its read, to the tokens that authorized the read.
	pub(crate) fn refresh_graph_tokens(
		&self,
		id: &tg::graph::Id,
		authorization: &crate::authorization::Output,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<tg::authorization::Tokens> {
		let resource = tg::Id::from(id.clone());
		let expires_at = self.object_token_expiration(authorization)?;
		let permissions = authorization.permissions.iter().collect();
		let mut tokens = tokens.clone();
		if let Some(token) = self.create_token(resource.clone(), permissions, expires_at)? {
			tokens.insert_local_authorization(token);
		}
		tokens.normalize(Some(&resource));
		Ok(tokens)
	}
}
