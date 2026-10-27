use {
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future, future::BoxFuture, stream,
		stream::BoxStream,
	},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

mod follow;
mod selector;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn try_get(
		&self,
		session: &Session,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let stream = match reference.node() {
			tg::reference::Node::Id(id)
				if reference.options().follow
					&& matches!(
						id.kind(),
						tg::id::Kind::Group
							| tg::id::Kind::Organization
							| tg::id::Kind::Tag | tg::id::Kind::User
					) =>
			{
				self.try_get_with_follow(session, reference, arg).await?
			},
			tg::reference::Node::Id(id) => {
				self.try_get_with_id(session, id, reference.options(), &arg)
					.await?
			},
			tg::reference::Node::Path(path) => {
				self.try_get_with_path(session, path, reference.options(), arg)
					.await?
			},
			tg::reference::Node::Pointer(pointer) => {
				self.try_get_with_pointer(session, pointer, reference.options())
					.await?
			},
			tg::reference::Node::Specifier(specifier)
				if reference.options().follow || specifier.contains_operators() =>
			{
				self.try_get_with_follow(session, reference, arg).await?
			},
			tg::reference::Node::Specifier(specifier) => {
				self.try_get_with_specifier(session, specifier, reference.options(), &arg)
					.await?
			},
		};
		Ok(stream)
	}

	async fn try_get_with_id(
		&self,
		session: &Session,
		id: &tg::Id,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		if matches!(
			id.kind(),
			tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User
		) || (options.tokens.is_empty() && id.kind() == tg::id::Kind::Tag)
		{
			let output = self
				.try_get_with_selector(
					session,
					&tg::Selector::Id(id.clone()),
					options.location.as_ref(),
					&options.tokens,
					arg.cached,
					arg.ttl,
				)
				.await?;
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(future::ok(event));

			return Ok(stream.boxed());
		}
		if id.kind() == tg::id::Kind::Sandbox {
			let id = tg::sandbox::Id::try_from(id.clone())?;
			let entry = tg::sandbox::get::Arg {
				cached: arg.cached,
				location: options.location.clone(),
				source: tg::sandbox::Source::Auto,
				tokens: options.tokens.clone(),
				ttl: arg.ttl,
			};
			let sandbox = session.try_get_sandbox(&id, entry).await?;
			let output = sandbox.map(|sandbox| {
				let options = tg::referent::Options {
					location: sandbox.location,
					tokens: sandbox.tokens,
					..tg::referent::Options::default()
				};
				let referent =
					tg::Referent::new(tg::get::Node::Id(sandbox.data.id.into()), options);
				tg::get::Output { referent }
			});
			let event = tg::progress::Event::Output(output);
			let stream = stream::once(future::ok(event));

			return Ok(stream.boxed());
		}
		let referent_options: tg::referent::Options = options.clone().into();
		let referent = tg::Referent::new(tg::get::Node::Id(id.clone()), referent_options);
		let output = tg::get::Output { referent };
		let output = self
			.try_get_apply_get(session, output, options.get.as_deref())
			.await?;
		let event = tg::progress::Event::Output(output);
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_with_path(
		&self,
		session: &Session,
		path: &Path,
		options: &tg::reference::Options,
		arg: tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let checkin_arg = tg::checkin::Arg {
			options: arg.checkin.clone(),
			path: path.to_owned(),
			updates: Vec::new(),
		};
		let options = options.clone();
		let stream = session
			.checkin(checkin_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the path"))?
			.and_then({
				let component = self.clone();
				let session = session.clone();
				move |event| {
					let component = component.clone();
					let session = session.clone();
					let options = options.clone();
					async move {
						match event {
							tg::progress::Event::Log(log) => Ok(tg::progress::Event::Log(log)),
							tg::progress::Event::Diagnostic(diagnostic) => {
								Ok(tg::progress::Event::Diagnostic(diagnostic))
							},
							tg::progress::Event::Indicators(indicators) => {
								Ok(tg::progress::Event::Indicators(indicators))
							},
							tg::progress::Event::Output(checkin_output) => {
								let get = options.get;
								let id = checkin_output.artifact.node.into();
								let mut referent_options = checkin_output.artifact.options;
								referent_options.location =
									Some(tg::Location::Local(tg::location::Local::default()));
								let referent =
									tg::Referent::new(tg::get::Node::Id(id), referent_options);
								let output = tg::get::Output { referent };
								let output = component
									.try_get_apply_get(&session, output, get.as_deref())
									.await?;
								Ok::<_, tg::Error>(tg::progress::Event::Output(output))
							},
						}
					}
				}
			});
		Ok(stream.boxed())
	}

	async fn try_get_with_pointer(
		&self,
		session: &Session,
		pointer: &tg::graph::data::Pointer,
		options: &tg::reference::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let referent_options: tg::referent::Options = options.clone().into();
		let referent = tg::Referent::new(tg::get::Node::Pointer(pointer.clone()), referent_options);
		let output = tg::get::Output { referent };
		if options.path.is_some() {
			return Err(tg::error!("cannot get path in pointer"));
		}
		let output = self
			.try_get_apply_get(session, output, options.get.as_deref())
			.await?;
		let event = tg::progress::Event::Output(output);
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_with_specifier(
		&self,
		session: &Session,
		specifier: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		if specifier.is_empty() || specifier.contains_operators() {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		}
		let specifier = specifier.to_specifier();
		let output = self
			.try_get_with_selector(
				session,
				&tg::Selector::Specifier(specifier),
				options.location.as_ref(),
				&options.tokens,
				arg.cached,
				arg.ttl,
			)
			.await?;
		let Some(output) = output else {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		};
		let output = self
			.try_get_apply_get(session, output, options.get.as_deref())
			.await?;
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
		Ok(stream.boxed())
	}

	pub(crate) async fn try_get_apply_get(
		&self,
		session: &Session,
		mut output: tg::get::Output,
		get: Option<&Path>,
	) -> tg::Result<Option<tg::get::Output>> {
		let Some(get) = get else {
			return Ok(Some(output));
		};
		match &output.referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Directory => {
				let directory = tg::directory::Id::try_from(id.clone())?;
				let referent = output.referent.clone().map(|_| directory);
				let directory = tg::Directory::with_referent(referent);
				let Some(artifact) = directory.try_get_with_instance(session, get).await? else {
					return Ok(None);
				};
				let id = artifact
					.store_with_instance(session)
					.await
					.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
				output.referent.node = tg::get::Node::Id(id.into());
				output.referent.options.id = Some(directory.id().into());
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Node::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Directory => {
				let graph = pointer.graph.clone();
				let options = tg::referent::Options {
					location: output.referent.options.location.clone(),
					tokens: output.referent.options.tokens.clone(),
					..tg::referent::Options::default()
				};
				let graph = tg::Referent::new(graph, options);
				let graph = tg::Graph::with_referent(graph);
				let directory = tg::Directory::with_pointer(tg::graph::Pointer {
					graph,
					index: pointer.index,
					kind: pointer.kind,
				});
				let Some(edge) = directory.try_get_edge_with_instance(session, get).await? else {
					return Ok(None);
				};
				let edge = match edge {
					tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
					tg::graph::Edge::Object(artifact) => tg::get::Node::Id(artifact.id().into()),
					tg::graph::Edge::Pointer(pointer) => {
						tg::get::Node::Pointer(tg::graph::data::Pointer {
							graph: pointer.graph.id(),
							index: pointer.index,
							kind: pointer.kind,
						})
					},
				};
				output.referent.node = edge;
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Node::Pointer(pointer) => {
				output.referent.node = tg::get::Node::Pointer(pointer.clone());
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				Err(tg::error!("cannot apply a get option to a process"))
			},
			tg::get::Node::Id(_) => Err(tg::error!("unexpected reference get option")),
		}
	}

	pub(crate) async fn try_get_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		let node = path
			.join("/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the node"))?;

		// Get the reference options and arg.
		let (arg, _) = request
			.arg::<tg::get::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let reference = tg::Reference::with_node_and_options(node, arg.options.clone());

		let stream = self
			.try_get(session, &reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
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
			(http::Method::GET, ["_", path @ ..]) => {
				self.try_get_request(session, request, path).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::get::Component for Component {
	fn create_tag_target_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::tag::Id,
		target: &'a tg::Id,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::authorization::Token>>> {
		self.create_tag_target_token(session, id, target, tokens)
			.boxed()
	}

	fn create_tag_target_token_with_permissions<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		target: &'a tg::Id,
		permissions: Vec<tg::authorization::Permission>,
	) -> tg::Result<Option<tg::authorization::Token>> {
		Self::create_tag_target_token_with_permissions(session, target, permissions)
	}

	fn match_tags_for_get<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		pattern: &'a tg::specifier::Pattern,
		location: Option<&'a tg::location::Arg>,
		cached: bool,
		length: Option<u64>,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<tg::match_::Output>> {
		self.match_tags_for_get(session, pattern, location, cached, length, ttl)
			.boxed()
	}

	fn try_get_with_selector<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		selector: &'a tg::Selector<tg::Id>,
		location: Option<&'a tg::location::Arg>,
		tokens: &'a tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<Option<tg::get::Output>>> {
		self.try_get_with_selector(session, selector, location, tokens, cached, ttl)
			.boxed()
	}

	fn try_get<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		reference: &'a tg::Reference,
		arg: tg::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>>,
	> {
		self.try_get(session, reference, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
