use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream};
use tangram_client as tg;
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _};

impl Server {
	pub async fn try_get(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		let reference = reference.clone();
		match &reference.item() {
			tg::reference::Item::Process(process) => {
				let item = Either::Left(process.clone());
				let output = tg::get::Output {
					referent: tg::Referent {
						item,
						path: None,
						subpath: None,
						tag: None,
					},
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},
			tg::reference::Item::Object(object) => {
				let item = Either::Right(object.clone());
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::get::Output {
					referent: tg::Referent {
						item,
						path: None,
						subpath,
						tag: None,
					},
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},
			tg::reference::Item::Path(path) => {
				let arg = tg::checkin::Arg {
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					lockfile: true,
					path: path.clone(),
				};
				let stream = self.checkin(arg).await?.map_ok(move |event| match event {
					tg::progress::Event::Log(log) => tg::progress::Event::Log(log),
					tg::progress::Event::Diagnostic(diagnostic) => {
						tg::progress::Event::Diagnostic(diagnostic)
					},
					tg::progress::Event::Start(indicator) => tg::progress::Event::Start(indicator),
					tg::progress::Event::Update(indicator) => {
						tg::progress::Event::Update(indicator)
					},
					tg::progress::Event::Finish(indicator) => {
						tg::progress::Event::Finish(indicator)
					},
					tg::progress::Event::Output(output) => {
						let subpath = output
							.referent
							.subpath
							.map(|subpath| {
								if let Some(p) = reference
									.options()
									.and_then(|options| options.subpath.as_ref())
								{
									subpath.join(p)
								} else {
									subpath
								}
							})
							.or_else(|| {
								reference
									.options()
									.and_then(|options| options.subpath.clone())
							});
						let referent = tg::Referent {
							item: Either::Right(output.referent.item.into()),
							path: output.referent.path,
							tag: output.referent.tag,
							subpath,
						};
						let output = Some(tg::get::Output { referent });
						tg::progress::Event::Output(output)
					},
				});
				Ok::<_, tg::Error>(stream.boxed())
			},
			tg::reference::Item::Tag(tag) => {
				let Some(tg::tag::get::Output { item, tag }) = self.try_get_tag(tag).await? else {
					let stream = stream::once(future::ok(tg::progress::Event::Output(None)));
					return Ok::<_, tg::Error>(stream.boxed());
				};
				let subpath = reference
					.options()
					.and_then(|options| options.subpath.clone());
				let output = tg::get::Output {
					referent: tg::Referent {
						item,
						path: None,
						subpath,
						tag: Some(tag),
					},
				};
				let event = tg::progress::Event::Output(Some(output));
				let stream = stream::once(future::ok(event));
				Ok::<_, tg::Error>(stream.boxed())
			},
		}
	}

	pub(crate) async fn handle_get_request<H>(
		handle: &H,
		request: http::Request<Body>,
		path: &[&str],
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		let path = path.join("/").parse()?;
		let query = request.query_params().transpose()?;
		let reference = tg::Reference::with_item_and_options(&path, query.as_ref());

		let stream = handle.try_get(&reference).await?;

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
