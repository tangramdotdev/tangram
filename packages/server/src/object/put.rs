use {
	crate::{Context, Server},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
	tangram_object_store::prelude::*,
};

impl Server {
	pub async fn put_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		let location = self.location(arg.location.as_ref())?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.put_object_local(id, arg).await?;
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.put_object_region(id, arg, region).await?;
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.put_object_remote(id, arg, remote, region).await?;
			},
		}

		Ok(())
	}

	async fn put_object_local(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let put_arg = crate::object::store::PutObjectArg {
			id: id.clone(),
			bytes: Some(arg.bytes.clone()),
			touched_at: now,
			cache_pointer: None,
		};
		self.object_store
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
		let arg = tangram_index::PutObjectArg {
			cache_entry: None,
			children,
			id: id.clone(),
			metadata,
			stored: tangram_index::ObjectStored::default(),
			touched_at: now,
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.put(tangram_index::PutArg {
							objects: vec![arg],
							..Default::default()
						})
						.await
					{
						tracing::error!(error = %error.trace(), "failed to put object to index");
					}
				}
			})
			.detach();

		Ok(())
	}

	async fn put_object_region(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, %id, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::object::put::Arg {
			location: Some(location.into()),
			..arg
		};
		client.put_object(id, arg).await.map_err(
			|source| tg::error!(!source, %id, region = %region, "failed to put the object"),
		)?;
		Ok(())
	}

	async fn put_object_remote(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::object::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		client.put_object(id, arg).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote, "failed to put the object"),
		)?;
		Ok(())
	}

	pub(crate) async fn handle_put_object_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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
				.unwrap()
				.boxed_body();
			return Ok(response);
		}

		let arg = tg::object::put::Arg { bytes, ..query_arg };
		self.put_object_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
