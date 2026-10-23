use {
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream},
	num::ToPrimitive as _,
	std::{
		panic::AssertUnwindSafe,
		pin::{Pin, pin},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
	tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _},
};

mod reader;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn try_read_stream(
		&self,
		session: &Session,
		arg: tg::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::read::Event>>>> {
		// Create the reader.
		let blob = tg::Blob::with_id(arg.blob.clone());
		blob.state().set_tokens(arg.tokens.clone());
		let reader = self
			.reader(session, blob)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the reader"))?;

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let task = Task::spawn({
			let session = session.clone();
			|_| async move {
				let result = AssertUnwindSafe(Self::try_read_blob_task(
					&session,
					arg,
					reader,
					sender.clone(),
				))
				.catch_unwind()
				.await;
				match result {
					Ok(Ok(())) => (),
					Ok(Err(error)) => {
						sender
							.send(Err(error))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export error");
							})
							.ok();
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						sender
							.send(Err(tg::error!(?message, "the task panicked")))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export panic");
							})
							.ok();
					},
				}
			}
		});

		// Create the stream.
		let stream = receiver
			.attach(task)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		Ok(Some(stream))
	}

	async fn try_read_blob_task(
		session: &Session,
		arg: tg::read::Arg,
		reader: impl AsyncRead + AsyncSeek + Send + 'static,
		sender: async_channel::Sender<tg::Result<tg::read::Event>>,
	) -> tg::Result<()> {
		let position = arg.options.position.unwrap_or(std::io::SeekFrom::Start(0));
		let default_size = session.server.config.read.default_size;
		let size = arg.options.size.unwrap_or(default_size).to_usize().unwrap();
		let mut length = 0;
		let mut buffer = vec![0u8; size];
		let mut reader = pin!(reader);
		reader
			.seek(position)
			.await
			.map_err(|error| tg::error!(!error, "failed to seek in the blob file"))?;
		loop {
			let position = reader
				.stream_position()
				.await
				.map_err(|error| tg::error!(!error, "failed to get the stream position"))?;
			let mut n = 0;
			while n < buffer.len() {
				let n_ = reader
					.read(&mut buffer[n..])
					.await
					.map_err(|error| tg::error!(!error, "failed to read the blob"))?;
				n += n_;
				if n_ == 0 {
					break;
				}
			}
			buffer.truncate(n);
			if let Some(l) = arg.options.length {
				buffer.truncate(l.saturating_sub(length).to_usize().unwrap());
			}
			let data = tg::read::Chunk {
				position,
				bytes: buffer.clone().into(),
			};
			if data.bytes.is_empty() || arg.options.length.is_some_and(|l| l <= length) {
				break;
			}
			length += data.bytes.len().to_u64().unwrap();
			let result = sender.try_send(Ok(tg::read::Event::Chunk(data)));
			if result.is_err() {
				return Ok(());
			}
		}
		sender.try_send(Ok(tg::read::Event::End)).ok();
		Ok(())
	}

	pub(crate) async fn try_read_stream_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the query.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;

		// Get the stream.
		let Some(stream) = self.try_read_stream(session, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Validate the accept header.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		}

		let mut stream = stream.peekable();

		let mut position = None;
		if let Some(Ok(tg::read::Event::Chunk(chunk))) = Pin::new(&mut stream).peek().await {
			position.replace(chunk.position);
		}

		// Create the frame stream.
		let frames = stream.map(|event| match event {
			Ok(tg::read::Event::Chunk(chunk)) => {
				Ok::<_, tg::Error>(hyper::body::Frame::data(chunk.bytes))
			},
			Ok(tg::read::Event::End) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
				Ok(hyper::body::Frame::trailers(trailers))
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error.to_data_or_id()).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				Ok(hyper::body::Frame::trailers(trailers))
			},
		});
		let body = BoxBody::with_stream(frames);

		// Create the response.
		let mut response = http::Response::builder().header(
			http::header::CONTENT_TYPE,
			mime::APPLICATION_OCTET_STREAM.to_string(),
		);
		if let Some(position) = position {
			response = response.header("x-tg-position", position);
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
			(http::Method::GET, ["read"]) => self.try_read_stream_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::read::Component for Component {
	fn reader<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		blob: tg::Blob,
	) -> BoxFuture<'a, tg::Result<tangram_server_core::read::Reader>> {
		self.reader(session, blob).boxed()
	}

	fn reader_sync(
		&self,
		session: &tangram_server_core::Session,
		blob: tg::Blob,
	) -> tg::Result<tangram_server_core::read::Reader> {
		Self::reader_sync(session, blob)
	}

	fn try_read_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::read::Event>>>>> {
		self.try_read_stream(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
