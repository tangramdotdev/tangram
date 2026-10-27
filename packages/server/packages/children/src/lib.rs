use {
	futures::{FutureExt as _, future::BoxFuture},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { position: u64 },
}

#[serde_with::serde_as]
#[derive(Default, serde::Deserialize)]
struct Arg {
	cursor: Option<String>,
	#[serde_as(as = "Option<serde_with::PickFirst<(_, serde_with::DisplayFromStr)>>")]
	limit: Option<u64>,
	#[serde(flatten)]
	options: tg::referent::Options,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn children(
		&self,
		session: &Session,
		arg: tg::children::Arg,
	) -> tg::Result<tg::children::Output> {
		session.verify_request_with_network_access()?;
		let location_arg = arg.node.options.location.clone().map(Into::into);
		let location = session.server.location(location_arg.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.children_region(session, arg, &region).await
			},
			tg::Location::Local(_) => self.children_local(session, arg).await,
			tg::Location::Remote(remote) => self.children_remote(session, arg, &remote).await,
		}
	}

	async fn children_local(
		&self,
		session: &Session,
		arg: tg::children::Arg,
	) -> tg::Result<tg::children::Output> {
		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let cursor = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let position = cursor.map_or(0, |cursor| match cursor {
			Cursor::V0 { position } => position,
		});

		// Revalidate the node and obtain a fresh token.
		let id = arg.node.node;
		let location = tg::Location::Local(tg::location::Local::default());
		let parent_options = self
			.revalidate_children_node_local(session, &id, arg.node.options)
			.await?;

		// Get the children.
		let nodes = match id.kind() {
			tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User => {
				let arg = tg::list::Arg {
					cached: false,
					cursor: None,
					groups: true,
					limit: None,
					location: Some(location.into()),
					node: Some(tg::Referent::new(id, parent_options)),
					organizations: false,
					recursive: false,
					reverse: false,
					tags: true,
					ttl: tg::remote::cache::Ttl::default(),
					users: false,
				};
				session
					.server
					.components
					.list
					.list_inner(session, arg)
					.await?
					.into_iter()
					.map(|entry| entry.node)
					.collect()
			},
			tg::id::Kind::Process => {
				let id = id.try_into()?;
				let nodes = self
					.try_get_process_children_and_objects_from_index(session, &id)
					.await?;
				inherit_child_options(nodes, &parent_options)
			},
			tg::id::Kind::Tag => {
				let id = id.try_into()?;
				let output = session
					.server
					.components
					.tag
					.try_get_tag_local(session, &id, parent_options.tokens.clone())
					.await?
					.ok_or_else(|| tg::error!(%id, "failed to find the tag"))?;
				let target = match output.data.target {
					tg::tag::data::Target::Object(id) => id.into(),
					tg::tag::data::Target::Process(id) => id.into(),
				};
				let token = session
					.server
					.components
					.get
					.create_tag_target_token(session, &id, &target, &parent_options.tokens)
					.await?;
				let options = tg::referent::Options {
					location: Some(location),
					tokens: tg::authorization::Tokens::with_authorization(token),
					..Default::default()
				};
				vec![tg::Referent::new(target, options)]
			},
			kind if kind.is_object() => {
				let id = tg::object::Id::try_from(id)?;
				let nodes = self
					.try_get_object_children_from_index(session, &id)
					.await?;
				nodes
					.into_iter()
					.map(|id| tg::Referent::new(id.into(), parent_options.clone()))
					.collect()
			},
			kind => {
				return Err(
					tg::error!(%kind, "getting children is not supported for the node kind"),
				);
			},
		};
		let mut data = merge_children(nodes);

		// Select the page and create the continuation cursor.
		let position = usize::try_from(position)
			.unwrap_or(usize::MAX)
			.min(data.len());
		let end = position
			.saturating_add(usize::try_from(limit).unwrap())
			.min(data.len());
		let cursor = if end < data.len() {
			let cursor = Cursor::V0 {
				position: u64::try_from(end).unwrap(),
			};
			Some(tangram_server_core::cursor::serialize(&cursor)?)
		} else {
			None
		};
		let data = data.drain(position..end).collect();
		let output = tg::children::Output { cursor, data };

		Ok(output)
	}

	async fn revalidate_children_node_local(
		&self,
		session: &Session,
		id: &tg::Id,
		options: tg::referent::Options,
	) -> tg::Result<tg::referent::Options> {
		let (permissions, required, time_to_live) = if id.kind().is_object() {
			let mut permissions = tg::authorization::permission::object::Set::empty();
			permissions.insert(tg::authorization::permission::object::Set::NODE);
			permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
			let permissions = tg::authorization::permission::Set::Object(permissions);
			let required = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			);
			(
				permissions,
				required,
				session.server.config.object.permission_time_to_live,
			)
		} else if id.kind() == tg::id::Kind::Process {
			let permissions = tg::authorization::permission::Set::Process(
				tg::authorization::permission::process::Set::all(),
			);
			let required = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			);
			(
				permissions,
				required,
				session.server.config.process.permission_time_to_live,
			)
		} else {
			let location = tg::Location::Local(tg::location::Local::default());
			let location_arg = location.into();
			let selector = tg::Selector::Id(id.clone());
			let Some(output) = session
				.server
				.components
				.get
				.try_get_with_selector(
					session,
					&selector,
					Some(&location_arg),
					&options.tokens,
					false,
					tg::remote::cache::Ttl::default(),
				)
				.await?
			else {
				return Err(tg::error!(%id, "failed to find the node"));
			};
			let tg::get::Node::Id(output_id) = output.referent.node else {
				unreachable!();
			};
			if output_id != *id {
				return Err(tg::error!(%id, %output_id, "the node ID changed during lookup"));
			}

			return Ok(output.referent.options);
		};
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			options.tokens.local_authorization().to_vec(),
		);
		let mut authorizations = session
			.authorize_batch_with_required([(resource, permissions)], required.into(), None)
			.await?;
		let permissions = authorizations
			.pop()
			.unwrap()
			.check_exhaustion()?
			.permissions;
		if !permissions.contains(required) {
			return Err(tg::error!(%id, "failed to find the node"));
		}
		let expires_at = session
			.server
			.clock
			.unix_timestamp()
			.checked_add(i64::try_from(time_to_live.as_secs()).map_err(|error| {
				tg::error!(!error, "failed to convert the permission time to live")
			})?)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let token = session.server.components.authorization.create_token(
			session,
			id.clone(),
			permissions.iter().collect(),
			expires_at,
		)?;
		let mut tokens = options.tokens;
		if let Some(token) = token {
			tokens.insert_local_authorization(token);
		}
		let options = tg::referent::Options {
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
			..Default::default()
		};

		Ok(options)
	}

	async fn try_get_object_children_from_index(
		&self,
		session: &Session,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(children) = session.server.index.try_get_object_children(id).await? {
			return Ok(children);
		}
		session
			.server
			.components
			.indexer
			.index(session)
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		session
			.server
			.index
			.try_get_object_children(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the object"))
	}

	async fn try_get_process_children_and_objects_from_index(
		&self,
		session: &Session,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let output = session
			.server
			.index
			.try_get_process_children_and_objects(id)
			.await?;
		if output.as_ref().is_some_and(|output| output.complete) {
			return Ok(output.unwrap().nodes);
		}
		session
			.server
			.components
			.indexer
			.index(session)
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		session
			.server
			.index
			.try_get_process_children_and_objects(id)
			.await?
			.map(|output| output.nodes)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))
	}

	async fn children_region(
		&self,
		session: &Session,
		mut arg: tg::children::Arg,
		region: &str,
	) -> tg::Result<tg::children::Output> {
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		arg.node.options.location = Some(tg::Location::Local(tg::location::Local::default()));
		arg.node.options.tokens = arg.node.options.tokens.for_location(&location);
		let client = session.get_region_session(region).await?;
		let mut output = client.children(arg).await?;
		for node in &mut output.data {
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut node.options.tokens,
					Some(&mut node.options.location),
					&location,
					false,
				)?;
		}

		Ok(output)
	}

	async fn children_remote(
		&self,
		session: &Session,
		mut arg: tg::children::Arg,
		remote: &tg::location::Remote,
	) -> tg::Result<tg::children::Output> {
		let location = tg::Location::Remote(remote.clone());
		arg.node.options.location = Some(tg::Location::Local(tg::location::Local {
			region: remote.region.clone(),
		}));
		arg.node.options.tokens = arg.node.options.tokens.for_location(&location);
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await?;
		let trusted = client.trusted();
		let mut output = client.children(arg).await?;
		for node in &mut output.data {
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut node.options.tokens,
					Some(&mut node.options.location),
					&location,
					trusted,
				)?;
		}

		Ok(output)
	}

	pub(crate) async fn children_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the request.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the node ID"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: Arg = arg.unwrap_or_default();

		// Get the children.
		let arg = tg::children::Arg {
			cursor: arg.cursor,
			limit: arg.limit,
			node: tg::Referent::new(id, arg.options),
		};
		let output = self.children(session, arg).await?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::GET, ["children", id]) => {
				self.children_request(session, request, id).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::children::Component for Component {
	fn children<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::children::Arg,
	) -> BoxFuture<'a, tg::Result<tg::children::Output>> {
		self.children(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}

fn inherit_child_options(
	children: Vec<tg::Referent<tg::Id>>,
	parent: &tg::referent::Options,
) -> Vec<tg::Referent<tg::Id>> {
	children
		.into_iter()
		.map(|mut child| {
			child.options.tokens.inherit(&parent.tokens);
			if child.options.location.is_none() {
				child.options.location.clone_from(&parent.location);
			}
			child
		})
		.collect()
}

fn merge_children(children: Vec<tg::Referent<tg::Id>>) -> Vec<tg::Referent<tg::Id>> {
	let mut output = BTreeMap::new();
	for child in children {
		let options = tg::referent::Options {
			location: child.options.location,
			tokens: child.options.tokens,
			..Default::default()
		};
		let child = tg::Referent::new(child.node, options);
		output
			.entry(child.node.clone())
			.and_modify(|existing: &mut tg::Referent<tg::Id>| {
				existing.options.tokens.inherit(&child.options.tokens);
				if existing.options.location.is_none() {
					existing
						.options
						.location
						.clone_from(&child.options.location);
				}
			})
			.or_insert(child);
	}

	output.into_values().collect()
}
