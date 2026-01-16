use {
	crate::{Context, Server},
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
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
		let reference = reference.clone();
		let stream = match &reference.item() {
			tg::reference::Item::Object(object) => {
				let output = match (&reference.options().path, object) {
					(Some(path), tg::object::Id::Directory(directory)) => {
						let directory = tg::Directory::with_id(directory.clone());
						directory.try_get(self, path).await.map(|ok| {
							ok.map(|artifact| {
								let referent = tg::Referent {
									item: tg::Either::Left(artifact.id().into()),
									options: tg::referent::Options {
										id: Some(directory.id().into()),
										..tg::referent::Options::default()
									},
								};
								tg::get::Output { referent }
							})
						})
					},
					(Some(_), _) => Err(tg::error!("unexpected reference path option")),
					(None, object) => {
						let referent = tg::Referent::with_item(tg::Either::Left(object.clone()));
						Ok(Some(tg::get::Output { referent }))
					},
				};
				let event = output.map(tg::progress::Event::Output);
				let stream = stream::once(future::ready(event));
				stream.boxed()
			},

			tg::reference::Item::Process(process) => {
				if reference.options().path.is_some() {
					return Err(tg::error!("unexpected reference path option"));
				}
				let item = tg::Either::Right(process.clone());
				let output = tg::get::Output {
					referent: tg::Referent::with_item(item),
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				stream.boxed()
			},

			tg::reference::Item::Path(path) => {
				let arg = tg::checkin::Arg {
					options: arg.checkin.clone(),
					path: path.clone(),
					updates: Vec::new(),
				};
				let stream = self
					.checkin(arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to check in the path"))?
					.and_then({
						let server = self.clone();
						let subpath = reference.options().path.clone();
						move |event| {
							let server = server.clone();
							let subpath = subpath.clone();
							async move {
								match event {
									tg::progress::Event::Log(log) => {
										Ok(tg::progress::Event::Log(log))
									},
									tg::progress::Event::Diagnostic(diagnostic) => {
										Ok(tg::progress::Event::Diagnostic(diagnostic))
									},
									tg::progress::Event::Indicators(indicators) => {
										Ok(tg::progress::Event::Indicators(indicators))
									},
									tg::progress::Event::Output(output) => {
										let item = output.artifact.item;
										let options = output.artifact.options;
										let output = match (subpath, item) {
											(
												Some(path),
												tg::artifact::Id::Directory(directory),
											) => {
												let directory =
													tg::Directory::with_id(directory.clone());
												directory.try_get(&server, path).await?.map(
													|artifact| {
														let referent = tg::Referent {
															item: tg::Either::Left(
																artifact.id().into(),
															),
															options: tg::referent::Options {
																id: Some(directory.id().into()),
																..options
															},
														};
														tg::get::Output { referent }
													},
												)
											},
											(Some(_), _) => {
												return Err(tg::error!(
													"unexpected reference path option"
												));
											},
											(None, artifact) => {
												let referent = tg::Referent::new(
													tg::Either::Left(artifact.into()),
													options,
												);
												Some(tg::get::Output { referent })
											},
										};
										Ok::<_, tg::Error>(tg::progress::Event::Output(output))
									},
								}
							}
						}
					});
				stream.boxed()
			},

			tg::reference::Item::Tag(tag) => {
				let tag_arg = tg::tag::get::Arg {
					local: arg.local,
					remotes: arg.remotes.clone(),
				};
				let Some(tg::tag::get::Output { item, tag, .. }) = self
					.try_get_tag(tag, tag_arg)
					.await
					.map_err(|source| tg::error!(!source, %tag, "failed to get the tag"))?
				else {
					let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
					return Ok::<_, tg::Error>(stream.boxed());
				};
				let output = match (&reference.options().path, item) {
					(Some(path), Some(tg::Either::Left(tg::object::Id::Directory(directory)))) => {
						let directory = tg::Directory::with_id(directory).clone();
						directory.try_get(self, path).await.map(|ok| {
							ok.map(|artifact| {
								let referent = tg::Referent {
									item: tg::Either::Left(artifact.id().into()),
									options: tg::referent::Options {
										id: Some(directory.id().into()),
										tag: Some(tag),
										..tg::referent::Options::default()
									},
								};
								tg::get::Output { referent }
							})
						})
					},
					(Some(_path), _) => Err(tg::error!("unexpected path reference option")),
					(None, item) => {
						let output = item.map(|item| {
							let id = item.as_ref().left().cloned();
							let referent = tg::Referent {
								item,
								options: tg::referent::Options {
									id,
									tag: Some(tag),
									..tg::referent::Options::default()
								},
							};
							tg::get::Output { referent }
						});
						Ok(output)
					},
				};
				let event = output.map(tg::progress::Event::Output);
				let stream = stream::once(future::ready(event));
				stream.boxed()
			},
		};

		Ok(stream)
	}

	pub(crate) async fn handle_get_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		path: &[&str],
	) -> tg::Result<http::Response<Body>> {
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
			.map_err(|source| tg::error!(!source, "failed to get the reference"))?;

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
				(Some(content_type), Body::with_sse_stream(stream))
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
