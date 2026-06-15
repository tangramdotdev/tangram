use {
	crate::Session,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream, stream::BoxStream},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + use<>,
	> {
		if matches!(self.context.principal, Some(tg::Principal::Process(_))) {
			return Err(tg::error!("unauthorized"));
		}
		let stream = match reference.item() {
			tg::reference::Item::Id(id) => self.try_get_with_id(id, reference.options()).await?,
			tg::reference::Item::Path(path) => {
				self.try_get_with_path(path, reference.options(), arg)
					.await?
			},
			tg::reference::Item::Pointer(pointer) => {
				self.try_get_with_pointer(pointer, reference.options())
					.await?
			},
			tg::reference::Item::Specifier(specifier) => {
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
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		if id.kind() == tg::id::Kind::Process && options.path.is_some() {
			return Err(tg::error!("cannot get path in process"));
		}
		let referent = tg::Referent::with_item(tg::get::Item::Id(id.clone()));
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
								let id = checkin_output.artifact.item.into();
								let referent = tg::Referent::new(
									tg::get::Item::Id(id),
									checkin_output.artifact.options,
								);
								let output = tg::get::Output { referent };
								let output = session
									.try_get_apply_get(output, options.get.as_deref())
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
		pointer: &tg::graph::data::Pointer,
		options: &tg::reference::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let referent = tg::Referent::with_item(tg::get::Item::Pointer(pointer.clone()));
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
		if arg.resolve {
			return self.try_resolve_specifier(specifier, options, arg).await;
		}
		if specifier.is_empty() || specifier.contains_operators() {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		}
		let specifier = specifier.to_specifier();
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(node) =
			Self::try_get_node_by_specifier_with_transaction(&transaction, &specifier).await?
		else {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		};
		let visible = self
			.server
			.index
			.visible(
				std::slice::from_ref(&node.id),
				self.context.principal.as_ref(),
			)
			.await?
			.pop()
			.unwrap() || self
			.authorize(
				tg::grant::Resource::Id(node.id.clone()),
				tg::grant::Permission::Read,
			)
			.await? == Some(true);
		if !visible {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		}
		let output = tg::get::Output {
			referent: tg::Referent::with_item(tg::get::Item::Id(node.id)),
		};
		let output = self
			.try_get_apply_get(output, options.get.as_deref())
			.await?;
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
		Ok(stream.boxed())
	}

	async fn try_resolve_specifier(
		&self,
		specifier: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let mut pattern = specifier.clone();
		if !specifier.is_empty() && !specifier.contains_operators() {
			let specifier = specifier.to_specifier();
			let mut connection = self
				.server
				.database
				.connection()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
			let transaction = connection
				.transaction()
				.await
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			if let Some(node) =
				Self::try_get_node_by_specifier_with_transaction(&transaction, &specifier).await?
			{
				let visible = self
					.server
					.index
					.visible(
						std::slice::from_ref(&node.id),
						self.context.principal.as_ref(),
					)
					.await?
					.pop()
					.unwrap() || self
					.authorize(
						tg::grant::Resource::Id(node.id.clone()),
						tg::grant::Permission::Read,
					)
					.await? == Some(true);
				if !visible {
					let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
					return Ok(stream.boxed());
				}
				if node.kind == tg::id::Kind::Tag {
					let data = Self::get_tag_data_with_transaction(&transaction, &node).await?;
					let id = tag_data_item_to_id(data.item);
					let output = tg::get::Output {
						referent: tg::Referent::new(
							tg::get::Item::Id(id),
							tg::referent::Options {
								tag: Some(specifier),
								..tg::referent::Options::default()
							},
						),
					};
					let output = self
						.try_get_apply_get(output, options.get.as_deref())
						.await?;
					let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
					return Ok(stream.boxed());
				}
				pattern = tg::specifier::Pattern::any_in_parent(Some(specifier.clone()));
			}
		}
		let list_arg = tg::list::Arg {
			cached: arg.cached,
			groups: false,
			length: Some(1),
			location: options.location.clone(),
			pattern,
			recursive: false,
			reverse: true,
			tags: true,
			ttl: arg.ttl,
		};
		let tg::list::Output { data } = self
			.list(list_arg)
			.await
			.map_err(|error| tg::error!(!error, %specifier, "failed to list entries"))?;
		let output = data.into_iter().find_map(|entry| {
			let tg::list::Entry::Tag { item, tag, .. } = entry else {
				return None;
			};
			Some(tg::get::Output {
				referent: tg::Referent::new(
					tg::get::Item::Id(list_item_to_id(item)),
					tg::referent::Options {
						tag: Some(tag),
						..tg::referent::Options::default()
					},
				),
			})
		});
		let output = if let Some(output) = output {
			self.try_get_apply_get(output, options.get.as_deref())
				.await?
		} else {
			None
		};
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
		Ok(stream.boxed())
	}

	async fn try_get_apply_get(
		&self,
		mut output: tg::get::Output,
		get: Option<&Path>,
	) -> tg::Result<Option<tg::get::Output>> {
		let Some(get) = get else {
			return Ok(Some(output));
		};
		match &output.referent.item {
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Directory => {
				let directory = tg::directory::Id::try_from(id.clone())?;
				let directory = tg::Directory::with_id(directory.clone());
				let Some(artifact) = directory.try_get_with_handle(self, get).await? else {
					return Ok(None);
				};
				let id = artifact
					.store_with_handle(self)
					.await
					.map_err(|error| tg::error!(!error, "failed to store the artifact"))?;
				output.referent.item = tg::get::Item::Id(id.into());
				output.referent.options.id = Some(directory.id().into());
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Item::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Directory => {
				let graph = pointer
					.graph
					.clone()
					.map(tg::Graph::with_id)
					.ok_or_else(|| tg::error!("missing graph"))?;
				let directory = tg::Directory::with_pointer(tg::graph::Pointer {
					graph: Some(graph),
					index: pointer.index,
					kind: pointer.kind,
				});
				let Some(edge) = directory.try_get_edge_with_handle(self, get).await? else {
					return Ok(None);
				};
				let edge = match edge {
					tg::graph::Edge::Object(artifact) => tg::get::Item::Id(artifact.id().into()),
					tg::graph::Edge::Pointer(pointer) => {
						tg::get::Item::Pointer(tg::graph::data::Pointer {
							graph: pointer.graph.as_ref().map(tg::Graph::id),
							index: pointer.index,
							kind: pointer.kind,
						})
					},
				};
				output.referent.item = edge;
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Item::Pointer(pointer) => {
				output.referent.item = tg::get::Item::Pointer(pointer.clone());
				output.referent.options.path = Some(get.to_owned());
				Ok(Some(output))
			},
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => {
				Err(tg::error!("cannot get path in process"))
			},
			tg::get::Item::Id(_) => Err(tg::error!("unexpected reference get option")),
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

		let item = path
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the item"))?;

		// Get the reference options and arg.
		let arg: tg::get::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let reference = tg::Reference::with_item_and_options(item, arg.options.clone());

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

fn list_item_to_id(item: tg::Either<tg::object::Id, tg::process::Id>) -> tg::Id {
	match item {
		tg::Either::Left(id) => id.into(),
		tg::Either::Right(id) => id.into(),
	}
}

fn tag_data_item_to_id(item: tg::tag::data::Item) -> tg::Id {
	match item {
		tg::tag::data::Item::Object(id) => id.into(),
		tg::tag::data::Item::Process(id) => id.into(),
	}
}
