use {
	crate::Session,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn children(
		&self,
		arg: tg::children::Arg,
	) -> tg::Result<tg::children::Output> {
		self.verify_request_with_network_access()?;
		let location_arg = arg.node.options.location.clone().map(Into::into);
		let location = self.server.location(location_arg.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.children_local(arg).await
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.children_region(arg, &region).await,
			tg::Location::Remote(remote) => self.children_remote(arg, &remote).await,
		}
	}

	async fn children_local(&self, arg: tg::children::Arg) -> tg::Result<tg::children::Output> {
		// Revalidate the node and obtain a fresh token.
		let id = arg.node.node;
		let location = tg::Location::Local(tg::location::Local::default());
		let parent_options = self
			.revalidate_children_node_local(&id, arg.node.options)
			.await?;

		// Get the children.
		let nodes = match id.kind() {
			tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User => {
				let arg = tg::list::Arg {
					cached: false,
					groups: true,
					length: None,
					location: Some(location.into()),
					node: Some(tg::Referent::new(id, parent_options)),
					organizations: false,
					position: None,
					recursive: false,
					reverse: false,
					tags: true,
					ttl: tg::remote::cache::Ttl::default(),
					users: false,
				};
				self.list(arg)
					.await?
					.data
					.into_iter()
					.map(|entry| entry.node)
					.collect()
			},
			tg::id::Kind::Process => {
				let id = id.try_into()?;
				let nodes = self.try_get_process_node_children_from_index(&id).await?;
				inherit_child_options(nodes, &parent_options)
			},
			tg::id::Kind::Tag => {
				let id = id.try_into()?;
				let output = self
					.try_get_tag_local(&id, parent_options.tokens.clone())
					.await?
					.ok_or_else(|| tg::error!(%id, "failed to find the tag"))?;
				let target = match output.data.target {
					tg::tag::data::Target::Object(id) => id.into(),
					tg::tag::data::Target::Process(id) => id.into(),
				};
				let token = self
					.create_tag_target_token_with_permissions(&target, output.data.permissions)?;
				let options = tg::referent::Options {
					location: Some(location),
					tokens: tg::authorization::Tokens::with_local(token),
					..Default::default()
				};
				vec![tg::Referent::new(target, options)]
			},
			kind if kind.is_object() => {
				let id = tg::object::Id::try_from(id)?;
				let nodes = self.try_get_object_children_from_index(&id).await?;
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
		let nodes = merge_children(nodes);

		Ok(tg::children::Output { nodes })
	}

	async fn revalidate_children_node_local(
		&self,
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
				self.server.config.object.grant_time_to_live,
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
				self.server.config.process.grant_time_to_live,
			)
		} else {
			let location = tg::Location::Local(tg::location::Local::default());
			let location_arg = location.into();
			let selector = tg::Selector::Id(id.clone());
			let Some(output) = self
				.try_get_with_selector(
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
		let resource =
			tg::Referent::with_node_and_token(id.clone(), options.tokens.local().cloned());
		let Some(permissions) = self.authorize(resource, permissions).await? else {
			return Err(tg::error!(%id, "failed to find the node"));
		};
		if !permissions.contains(required) {
			return Err(tg::error!(%id, "failed to find the node"));
		}
		let expires_at =
			self.server
				.clock
				.unix_timestamp()?
				.checked_add(i64::try_from(time_to_live.as_secs()).map_err(|error| {
					tg::error!(!error, "failed to convert the grant time to live")
				})?)
				.ok_or_else(|| tg::error!("the grant expiration overflowed"))?;
		let token = self.create_token(id.clone(), permissions.iter().collect(), expires_at)?;
		let mut tokens = options.tokens;
		if let Some(token) = token {
			tokens.set_local(token);
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
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		if let Some(children) = self.server.index.try_get_object_children(id).await? {
			return Ok(children);
		}
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		self.server
			.index
			.try_get_object_children(id)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the object"))
	}

	async fn try_get_process_node_children_from_index(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let output = self.server.index.try_get_process_node_children(id).await?;
		if output.as_ref().is_some_and(|output| output.complete) {
			return Ok(output.unwrap().nodes);
		}
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		self.server
			.index
			.try_get_process_node_children(id)
			.await?
			.map(|output| output.nodes)
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))
	}

	async fn children_region(
		&self,
		mut arg: tg::children::Arg,
		region: &str,
	) -> tg::Result<tg::children::Output> {
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		arg.node.options.location = Some(tg::Location::Local(tg::location::Local::default()));
		arg.node.options.tokens = arg.node.options.tokens.for_location(&location);
		let session = self.get_region_session(region).await?;
		let mut output = session.children(arg).await?;
		for node in &mut output.nodes {
			self.update_tokens_and_location(
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
		mut arg: tg::children::Arg,
		remote: &tg::location::Remote,
	) -> tg::Result<tg::children::Output> {
		let location = tg::Location::Remote(remote.clone());
		arg.node.options.location = Some(tg::Location::Local(tg::location::Local {
			region: remote.region.clone(),
		}));
		arg.node.options.tokens = arg.node.options.tokens.for_location(&location);
		let session = self.get_remote_session(&remote.name).await?;
		let trusted = session.trusted();
		let mut output = session.children(arg).await?;
		for node in &mut output.nodes {
			self.update_tokens_and_location(
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
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the request.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the node ID"))?;
		let options = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the children.
		let arg = tg::children::Arg {
			node: tg::Referent::new(id, options),
		};
		let output = self.children(arg).await?;

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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
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
