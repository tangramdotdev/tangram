use crate::Server;
use futures::{Stream, StreamExt};
use hyper::body::Incoming;
use num::ToPrimitive;
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Outgoing};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _};

#[allow(dead_code)]
enum Reader {
	File(tokio::fs::File),
	Blob(tg::blob::Reader<Server>),
}

impl Server {
	pub(crate) async fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::blob::read::Event>>>> {
		// Create the reader.
		let blob = tg::Blob::with_id(id.clone());
		let reader = blob.reader(self).await?;
		let reader = Reader::Blob(reader);

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.try_read_blob_task(arg, reader, sender.clone()).await;
				if let Err(error) = result {
					sender.try_send(Err(error)).ok();
				}
			}
		});

		Ok(Some(receiver))
	}

	async fn try_read_blob_task(
		&self,
		arg: tg::blob::read::Arg,
		reader: impl AsyncRead + AsyncSeek + Send + 'static,
		sender: async_channel::Sender<tg::Result<tg::blob::read::Event>>,
	) -> tg::Result<()> {
		let position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
		let size = arg.size.unwrap_or(4096).to_usize().unwrap();
		let mut length = 0;
		let mut buf = vec![0u8; size];
		let mut reader = pin!(reader);
		reader
			.seek(position)
			.await
			.map_err(|source| tg::error!(!source, "failed to seek in blob file"))?;
		loop {
			let position = reader
				.stream_position()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;
			let mut n = 0;
			while n < buf.len() {
				let n_ = reader
					.read(&mut buf[n..])
					.await
					.map_err(|source| tg::error!(!source, "failed to read blob file"))?;
				n += n_;
				if n_ == 0 {
					break;
				}
			}
			buf.truncate(n);
			let data = tg::blob::read::Chunk {
				position,
				bytes: buf.clone().into(),
			};
			if data.bytes.is_empty()
				|| matches!(arg.length, Some(arg_length) if length >= arg_length)
			{
				break;
			}
			length += data.bytes.len().to_u64().unwrap();
			let result = sender.try_send(Ok(tg::blob::read::Event::Chunk(data)));
			if result.is_err() {
				return Ok(());
			}
		}
		sender.try_send(Ok(tg::blob::read::Event::End)).ok();
		Ok(())
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(
		self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).start_seek(position),
			Reader::File(reader) => std::pin::Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::File(reader) => std::pin::Pin::new(reader).poll_complete(cx),
		}
	}
}

impl Server {
	pub(crate) async fn handle_read_blob_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_read_blob_stream(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
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
				let body = Outgoing::sse(stream);
				(content_type, body)
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}
