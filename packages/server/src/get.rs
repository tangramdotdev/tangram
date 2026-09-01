use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream, stream::BoxStream},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

mod follow;
mod selector;

impl Session {
	pub(crate) async fn try_get(
		&self,
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
				self.try_get_with_follow(reference, arg).await?
			},
			tg::reference::Node::Id(id) => {
				self.try_get_with_id(id, reference.options(), &arg).await?
			},
			tg::reference::Node::Path(path) => {
				self.try_get_with_path(path, reference.options(), arg)
					.await?
			},
			tg::reference::Node::Pointer(pointer) => {
				self.try_get_with_pointer(pointer, reference.options())
					.await?
			},
			tg::reference::Node::Specifier(specifier)
				if reference.options().follow || specifier.contains_operators() =>
			{
				self.try_get_with_follow(reference, arg).await?
			},
			tg::reference::Node::Specifier(specifier) => {
				self.try_get_with_specifier(specifier, reference.options(), &arg)
					.await?
			},
		};
		Ok(stream)
	}

	async fn try_get_with_id(
		&self,
		id: &tg::Id,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		if options.tokens.is_empty()
			&& matches!(
				id.kind(),
				tg::id::Kind::Group
					| tg::id::Kind::Organization
					| tg::id::Kind::Tag
					| tg::id::Kind::User
			) {
			let output = self
				.try_get_with_selector(
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
		if options.tokens.is_empty() && id.kind() == tg::id::Kind::Sandbox {
			let id = tg::sandbox::Id::try_from(id.clone())?;
			let entry = tg::sandbox::get::Arg {
				cached: arg.cached,
				location: options.location.clone(),
				ttl: arg.ttl,
			};
			let sandbox = self.try_get_sandbox(&id, entry).await?;
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
			.try_get_apply_get(output, options.get.as_deref())
			.await?;
		let event = tg::progress::Event::Output(output);
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_with_path(
		&self,
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
		let stream = self
			.checkin(checkin_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the path"))?
			.and_then({
				let session = self.clone();
				move |event| {
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
								let output =
									session.try_get_apply_get(output, get.as_deref()).await?;
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
			.try_get_apply_get(output, options.get.as_deref())
			.await?;
		let event = tg::progress::Event::Output(output);
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_with_specifier(
		&self,
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
			.try_get_apply_get(output, options.get.as_deref())
			.await?;
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
		Ok(stream.boxed())
	}

	pub(crate) async fn try_get_apply_get(
		&self,
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
				let Some(artifact) = directory.try_get_with_handle(self, get).await? else {
					return Ok(None);
				};
				let id = artifact
					.store_with_handle(self)
					.await
					.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
				output.referent.node = tg::get::Node::Id(id.into());
				output.referent.options.id = Some(directory.id().into());
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Node::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Directory => {
				let graph = pointer
					.graph
					.clone()
					.ok_or_else(|| tg::error!("missing graph"))?;
				let options = tg::referent::Options {
					location: output.referent.options.location.clone(),
					tokens: output.referent.options.tokens.clone(),
					..tg::referent::Options::default()
				};
				let graph = tg::Referent::new(graph, options);
				let graph = tg::Graph::with_referent(graph);
				let directory = tg::Directory::with_pointer(tg::graph::Pointer {
					graph: Some(graph),
					index: pointer.index,
					kind: pointer.kind,
				});
				let Some(edge) = directory.try_get_edge_with_handle(self, get).await? else {
					return Ok(None);
				};
				let edge = match edge {
					tg::graph::Edge::Object(artifact) => tg::get::Node::Id(artifact.id().into()),
					tg::graph::Edge::Pointer(pointer) => {
						tg::get::Node::Pointer(tg::graph::data::Pointer {
							graph: pointer.graph.as_ref().map(tg::Graph::id),
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
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		let node = path
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the node"))?;

		// Get the reference options and arg.
		let arg: tg::get::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let reference = tg::Reference::with_node_and_options(node, arg.options.clone());

		let stream = self
			.try_get(&reference, arg)
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
}
