use crate::Server;
use futures::{Stream, StreamExt as _};
use num::ToPrimitive as _;
use std::pin::Pin;
use tangram_client::{self as tg, Handle as _};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::InspectReader;

impl Server {
	pub(crate) async fn import_object(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		if arg.remote.is_some() {
			let stream = self.import_object_remote(arg, reader).await?.left_stream();
			Ok(stream)
		} else {
			let stream = self.import_object_local(arg, reader).await?.right_stream();
			Ok(stream)
		}
	}

	async fn import_object_remote(
		&self,
		mut arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		// Lookup the remote.
		let remote = arg.remote.take().unwrap();
		let remote = self
			.remotes
			.get(&remote)
			.ok_or_else(|| tg::error!(%remote, "the remote does not exist"))?;

		// Import the object on the remote.
		remote
			.import_object(arg, reader)
			.await
			.map(futures::StreamExt::boxed)
	}

	async fn import_object_local(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn a task for the import.
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				server
					.import_object_local_inner(arg, reader, progress)
					.await
			}
		});

		// Spawn a task to wait for progress.
		tokio::spawn({
			let progress = progress.clone();
			async move {
				match task.await {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(source) => {
						progress.error(tg::error!(!source, "the task panicked"));
					},
				};
			}
		});

		// Create the stream.
		Ok(progress.stream())
	}

	async fn import_object_local_inner(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
		progress: crate::progress::Handle<tg::object::import::Output>,
	) -> tg::Result<tg::object::import::Output> {
		progress.start(
			"bytes".into(),
			"Bytes".into(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		// Create a reader that inspects progress.
		let reader = InspectReader::new(reader, move |chunk| {
			progress.increment("bytes", chunk.len().to_u64().unwrap());
		});

		// Decompress the reader if necessary. TODO: move to accept header where all requests should accept an encoding.
		let reader = BufReader::new(reader);
		let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match arg.compress {
			Some(tg::blob::compress::Format::Bz2) => {
				Box::pin(async_compression::tokio::bufread::BzDecoder::new(reader))
			},
			Some(tg::blob::compress::Format::Gz) => {
				Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
			},
			Some(tg::blob::compress::Format::Xz) => {
				Box::pin(async_compression::tokio::bufread::XzDecoder::new(reader))
			},
			Some(tg::blob::compress::Format::Zstd) => {
				Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
			},
			None => Box::pin(reader),
		};

		// Create an artifact.
		let object = match arg.format {
			tg::artifact::archive::Format::Tar => self.extract_tar(reader).await?.into(),
			tg::artifact::archive::Format::Tgar => self.extract_tgar(reader).await?,
			tg::artifact::archive::Format::Zip => {
				return Err(tg::error!(%format = arg.format, "unsupported archive format"))
			},
		};

		let object = object.id(self).await?.into();
		let output = tg::object::import::Output { object };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_object_import_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("failed to get the args"))?;

		// Get the body
		let body = request.reader();

		// Get the stream.
		let stream = handle.import_object(arg, body).await?;

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
				(Some(content_type), Outgoing::sse(stream))
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
