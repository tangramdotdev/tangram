use {
	crate::{Context, Server},
	futures::TryFutureExt as _,
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::prelude::*,
};

impl Server {
	pub async fn put_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::object::put::Arg {
				bytes: arg.bytes,
				metadata: arg.metadata,
				local: None,
				remotes: None,
			};
			client
				.put_object(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the object on remote"))?;
			return Ok(());
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let put_arg = crate::store::PutObjectArg {
			id: id.clone(),
			bytes: Some(arg.bytes.clone()),
			touched_at: now,
			cache_pointer: None,
		};
		self.store
			.put_object(put_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the object"))?;

		let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
		let mut children = BTreeSet::new();
		data.children(&mut children);

		let (node_solvable, node_solved) = match data {
			tg::object::Data::File(file) => match file {
				tg::file::Data::Pointer(_) => (false, true),
				tg::file::Data::Node(node) => (node.solvable(), node.solved()),
			},
			tg::object::Data::Graph(graph) => {
				graph
					.nodes
					.iter()
					.fold((false, true), |(solvable, solved), node| {
						if let tg::graph::data::Node::File(file) = node {
							(solvable || file.solvable(), solved && file.solved())
						} else {
							(solvable, solved)
						}
					})
			},
			_ => (false, true),
		};

		let metadata = if let Some(metadata) = arg.metadata {
			metadata
		} else {
			tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: arg.bytes.len().to_u64().unwrap(),
					solvable: node_solvable,
					solved: node_solved,
				},
				..Default::default()
			}
		};
		let message = crate::index::Message::PutObject(crate::index::message::PutObject {
			cache_entry: None,
			children,
			id: id.clone(),
			metadata,
			stored: crate::index::ObjectStored::default(),
			touched_at: now,
		});
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					let result = server
						.messenger
						.stream_publish(
							"index".to_owned(),
							crate::index::message::Messages(vec![message]),
						)
						.map_err(|source| tg::error!(!source, "failed to publish the message"))
						.and_then(|future| {
							future.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})
						})
						.await;
					if let Err(error) = result {
						tracing::error!(?error, "failed to publish the put object index message");
					}
				}
			})
			.detach();

		Ok(())
	}

	pub(crate) async fn handle_put_object_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id
			.parse::<tg::object::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;
		let query_arg: tg::object::put::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let bytes = request
			.bytes()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the request body"))?;

		let actual = tg::object::Id::new(id.kind(), &bytes);
		if id != actual {
			let error = tg::error::Data {
				message: Some("invalid object id".into()),
				values: [
					("expected".into(), id.to_string()),
					("actual".into(), actual.to_string()),
				]
				.into(),
				..Default::default()
			};
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.json(error)
				.map_err(|source| tg::error!(!source, "failed to serialize the error"))?
				.unwrap();
			return Ok(response);
		}

		let arg = tg::object::put::Arg {
			bytes,
			metadata: query_arg.metadata,
			local: query_arg.local,
			remotes: query_arg.remotes,
		};
		self.put_object_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
