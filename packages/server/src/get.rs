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
		match &reference.item() {
			tg::reference::Item::Object(object) => {
				let item = tg::Either::Left(object.clone());
				let output = tg::get::Output {
					referent: tg::Referent::with_item(item),
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},

			tg::reference::Item::Process(process) => {
				let item = tg::Either::Right(process.clone());
				let output = tg::get::Output {
					referent: tg::Referent::with_item(item),
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},

			tg::reference::Item::Path(path) => {
				let arg = tg::checkin::Arg {
					options: arg.checkin.clone(),
					path: path.clone(),
					updates: Vec::new(),
				};
				let stream = self.checkin(arg).await?.map_ok(move |event| match event {
					tg::progress::Event::Log(log) => tg::progress::Event::Log(log),
					tg::progress::Event::Diagnostic(diagnostic) => {
						tg::progress::Event::Diagnostic(diagnostic)
					},
					tg::progress::Event::Indicators(indicators) => {
						tg::progress::Event::Indicators(indicators)
					},
					tg::progress::Event::Output(output) => {
						let referent = tg::Referent {
							item: tg::Either::Left(output.artifact.item.into()),
							options: output.artifact.options,
						};
						let output = Some(tg::get::Output { referent });
						tg::progress::Event::Output(output)
					},
				});
				Ok::<_, tg::Error>(stream.boxed())
			},

			tg::reference::Item::Tag(tag) => {
				let tag_arg = tg::tag::get::Arg {
					local: arg.local,
					remotes: arg.remotes.clone(),
				};
				let Some(tg::tag::get::Output { item, tag, .. }) =
					self.try_get_tag(tag, tag_arg).await?
				else {
					let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
					return Ok::<_, tg::Error>(stream.boxed());
				};
				let item = item.ok_or_else(|| tg::error!("expected the tag to have an item"))?;
				let id = item.as_ref().left().cloned();
				let output = tg::get::Output {
					referent: tg::Referent {
						item,
						options: tg::referent::Options {
							artifact: None,
							id,
							name: None,
							path: None,
							tag: Some(tag),
						},
					},
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},
		}
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
			.transpose()?;

		let item = path.join("/").parse()?;

		// Get the reference options and arg.
		let arg: tg::get::Arg = request.query_params().transpose()?.unwrap_or_default();
		let options = request.query_params().transpose()?.unwrap_or_default();
		let reference = tg::Reference::with_item_and_options(item, options);

		let stream = self.try_get_with_context(context, &reference, arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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
