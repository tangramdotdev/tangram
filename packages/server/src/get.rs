use {
	crate::{Context, Server},
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream, stream::BoxStream},
	std::path::Path,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn try_get_with_context(
		&self,
		context: &Context,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + use<>,
	> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let stream = match reference.item() {
			tg::reference::Item::Object(object) => {
				self.try_get_with_object(object, reference.options())
					.await?
			},
			tg::reference::Item::Path(path) => {
				self.try_get_with_path(path, reference.options(), arg)
					.await?
			},
			tg::reference::Item::Process(process) => {
				Self::try_get_with_process(process, reference.options())?
			},
			tg::reference::Item::Tag(tag) => {
				self.try_get_with_tag(context, tag, reference.options(), arg)
					.await?
			},
		};
		Ok(stream)
	}

	async fn try_get_with_object(
		&self,
		object: &tg::object::Id,
		options: &tg::reference::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let referent = tg::Referent::with_item(tg::Either::Left(object.clone()));
		let output = tg::get::Output { referent };
		let output = self
			.try_get_apply_path(output, options.path.as_deref())
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
			.map_err(|source| tg::error!(!source, "failed to check in the path"))?
			.and_then({
				let server = self.clone();
				move |event| {
					let server = server.clone();
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
								let referent = tg::Referent::new(
									tg::Either::Left(checkin_output.artifact.item.into()),
									checkin_output.artifact.options,
								);
								let output = tg::get::Output { referent };
								let output = server
									.try_get_apply_path(output, options.path.as_deref())
									.await?;
								Ok::<_, tg::Error>(tg::progress::Event::Output(output))
							},
						}
					}
				}
			});
		Ok(stream.boxed())
	}

	fn try_get_with_process(
		process: &tg::process::Id,
		options: &tg::reference::Options,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let referent = tg::Referent::with_item(tg::Either::Right(process.clone()));
		let output = tg::get::Output { referent };
		if options.path.is_some() {
			return Err(tg::error!("cannot get path in process"));
		}
		let event = tg::progress::Event::Output(Some(output));
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_with_tag(
		&self,
		context: &Context,
		pattern: &tg::tag::Pattern,
		options: &tg::reference::Options,
		arg: tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		let list_arg = tg::tag::list::Arg {
			length: Some(1),
			local: arg.local,
			pattern: pattern.clone(),
			recursive: false,
			remotes: arg.remotes.clone(),
			reverse: true,
			ttl: None,
		};
		let tg::tag::list::Output { data } =
			self.list_tags_with_context(context, list_arg)
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to list tags"))?;
		let Some(tg::tag::get::Output { item, tag, .. }) = data.into_iter().next() else {
			let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
			return Ok(stream.boxed());
		};
		let output = match item {
			Some(item) => {
				let id = item.as_ref().left().cloned();
				let referent = tg::Referent {
					item,
					options: tg::referent::Options {
						id,
						tag: Some(tag),
						..tg::referent::Options::default()
					},
				};
				let output = tg::get::Output { referent };
				self.try_get_apply_path(output, options.path.as_deref())
					.await?
			},
			None => None,
		};
		let event = tg::progress::Event::Output(output);
		let stream = stream::once(future::ok(event));
		Ok(stream.boxed())
	}

	async fn try_get_apply_path(
		&self,
		mut output: tg::get::Output,
		path: Option<&Path>,
	) -> tg::Result<Option<tg::get::Output>> {
		let Some(path) = path else {
			return Ok(Some(output));
		};
		let tg::Either::Left(tg::object::Id::Directory(directory)) = &output.referent.item else {
			return Err(tg::error!("unexpected reference path option"));
		};
		let directory = tg::Directory::with_id(directory.clone());
		let Some(artifact) = directory.try_get(self, path).await? else {
			return Ok(None);
		};
		let id = artifact
			.store(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the artifact"))?;
		output.referent.item = tg::Either::Left(id.into());
		output.referent.options.id = Some(directory.id().into());
		output.referent.options.path = Some(path.to_owned());
		Ok(Some(output))
	}

	pub(crate) async fn handle_get_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let item = path
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the item"))?;

		// Get the reference options and arg.
		let arg: tg::get::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let options = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let reference = tg::Reference::with_item_and_options(item, options);

		let stream = self
			.try_get_with_context(context, &reference, arg)
			.await
			.map_err(|source| tg::error!(!source, %reference, "failed to get the reference"))?;

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
