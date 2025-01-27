use crate::Server;
use bytes::{Buf as _, Bytes};
use futures::{future::BoxFuture, FutureExt as _, Stream, StreamExt};
use num::ToPrimitive;
use std::{io::Cursor, pin::pin};
use sync_wrapper::SyncWrapper;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::{stream::Ext, task::Stop};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _};
use tokio_util::task::AbortOnDropHandle;

pub enum Reader {
	File(File),
	Object(Object),
}

pub struct File {
	reader: tokio::io::BufReader<tokio::fs::File>,
}

pub struct Object {
	blob: tg::Blob,
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
	server: Server,
	size: u64,
}

type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;

impl Server {
	pub(crate) async fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::blob::read::Event>> + Send + 'static>>
	{
		// Create the reader.
		let blob = tg::Blob::with_id(id.clone());
		let reader = Reader::new(self, blob).await?;

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let task = tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.try_read_blob_task(arg, reader, sender.clone()).await;
				if let Err(error) = result {
					sender.try_send(Err(error)).ok();
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);

		Ok(Some(receiver.attach(abort_handle)))
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
		let mut buffer = vec![0u8; size];
		let mut reader = pin!(reader);
		reader
			.seek(position)
			.await
			.map_err(|source| tg::error!(!source, "failed to seek in the blob file"))?;
		loop {
			let position = reader
				.stream_position()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;
			let mut n = 0;
			while n < buffer.len() {
				let n_ = reader
					.read(&mut buffer[n..])
					.await
					.map_err(|source| tg::error!(!source, "failed to read blob file"))?;
				n += n_;
				if n_ == 0 {
					break;
				}
			}
			buffer.truncate(n);
			let data = tg::blob::read::Chunk {
				position,
				bytes: buffer.clone().into(),
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

impl Reader {
	pub async fn new(server: &Server, blob: tg::Blob) -> tg::Result<Self> {
		let blob_path = server.blobs_path().join(blob.id(server).await?.to_string());
		let file = match tokio::fs::File::open(blob_path).await {
			Ok(file) => Some(file),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
			Err(error) => {
				return Err(tg::error!(!error, "failed to open the file"));
			},
		};
		let reader = if let Some(file) = file {
			Self::File(File::new(file))
		} else {
			Self::Object(Object::new(server, blob).await?)
		};
		Ok(reader)
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::File(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
			Reader::Object(reader) => std::pin::Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncBufRead for Reader {
	fn poll_fill_buf(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<&[u8]>> {
		match self.get_mut() {
			Reader::File(reader) => std::pin::Pin::new(reader).poll_fill_buf(cx),
			Reader::Object(reader) => std::pin::Pin::new(reader).poll_fill_buf(cx),
		}
	}

	fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
		match self.get_mut() {
			Reader::File(reader) => std::pin::Pin::new(reader).consume(amt),
			Reader::Object(reader) => std::pin::Pin::new(reader).consume(amt),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(
		self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::File(reader) => std::pin::Pin::new(reader).start_seek(position),
			Reader::Object(reader) => std::pin::Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::File(reader) => std::pin::Pin::new(reader).poll_complete(cx),
			Reader::Object(reader) => std::pin::Pin::new(reader).poll_complete(cx),
		}
	}
}

impl File {
	fn new(file: tokio::fs::File) -> Self {
		let reader = tokio::io::BufReader::new(file);
		Self { reader }
	}
}

impl AsyncRead for File {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
	}
}

impl AsyncBufRead for File {
	fn poll_fill_buf(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<&[u8]>> {
		std::pin::Pin::new(&mut self.get_mut().reader).poll_fill_buf(cx)
	}

	fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
		std::pin::Pin::new(&mut self.get_mut().reader).consume(amt);
	}
}

impl AsyncSeek for File {
	fn start_seek(
		self: std::pin::Pin<&mut Self>,
		position: std::io::SeekFrom,
	) -> std::io::Result<()> {
		std::pin::Pin::new(&mut self.get_mut().reader).start_seek(position)
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		std::pin::Pin::new(&mut self.get_mut().reader).poll_complete(cx)
	}
}

impl Object {
	async fn new(server: &Server, blob: tg::Blob) -> tg::Result<Self> {
		let cursor = None;
		let position = 0;
		let read = None;
		let size = blob.size(server).await?;
		let server = server.clone();
		Ok(Self {
			blob,
			cursor,
			position,
			read,
			server,
			size,
		})
	}
}

impl AsyncRead for Object {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let server = this.server.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = SyncWrapper::new(
				async move { poll_read_inner(&server, blob, position).await }.boxed(),
			);
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				std::task::Poll::Pending => return std::task::Poll::Pending,
				std::task::Poll::Ready(Err(error)) => {
					this.read.take();
					return std::task::Poll::Ready(Err(std::io::Error::other(error)));
				},
				std::task::Poll::Ready(Ok(None)) => {
					this.read.take();
					return std::task::Poll::Ready(Ok(()));
				},
				std::task::Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			};
		}

		// Read.
		let cursor = this.cursor.as_mut().unwrap();
		let bytes = cursor.get_ref();
		let position = cursor.position().to_usize().unwrap();
		let n = std::cmp::min(buf.remaining(), bytes.len() - position);
		buf.put_slice(&bytes[position..position + n]);
		this.position += n as u64;
		let position = position + n;
		cursor.set_position(position as u64);
		if position == cursor.get_ref().len() {
			this.cursor.take();
		}

		std::task::Poll::Ready(Ok(()))
	}
}

impl AsyncBufRead for Object {
	fn poll_fill_buf(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<&[u8]>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read.is_none() {
			let server = this.server.clone();
			let blob = this.blob.clone();
			let position = this.position;
			let read = SyncWrapper::new(
				async move { poll_read_inner(&server, blob, position).await }.boxed(),
			);
			this.read.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				std::task::Poll::Pending => return std::task::Poll::Pending,
				std::task::Poll::Ready(Err(error)) => {
					this.read.take();
					return std::task::Poll::Ready(Err(std::io::Error::other(error)));
				},
				std::task::Poll::Ready(Ok(None)) => {
					this.read.take();
					return std::task::Poll::Ready(Ok(&[]));
				},
				std::task::Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			};
		}

		// Read.
		let cursor = this.cursor.as_ref().unwrap();
		let bytes = &cursor.get_ref()[cursor.position().to_usize().unwrap()..];

		std::task::Poll::Ready(Ok(bytes))
	}

	fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
		let this = self.get_mut();
		this.position += amt.to_u64().unwrap();
		let cursor = this.cursor.as_mut().unwrap();
		cursor.advance(amt);
		let empty = cursor.position() == cursor.get_ref().len().to_u64().unwrap();
		if empty {
			this.cursor.take();
		}
	}
}

impl AsyncSeek for Object {
	fn start_seek(self: std::pin::Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => this.size.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => this.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"attempted to seek to a negative or overflowing position",
		))?;
		if position > this.size {
			return Err(std::io::Error::other(
				"attempted to seek to a position beyond the end",
			));
		}
		if let Some(cursor) = this.cursor.as_mut() {
			let leaf_position = position.to_i64().unwrap()
				- (this.position.to_i64().unwrap() - cursor.position().to_i64().unwrap());
			if leaf_position >= 0 && leaf_position < cursor.get_ref().len().to_i64().unwrap() {
				cursor.set_position(leaf_position.to_u64().unwrap());
			} else {
				this.cursor.take();
			}
		}
		this.position = position;
		Ok(())
	}

	fn poll_complete(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		std::task::Poll::Ready(Ok(self.position))
	}
}

async fn poll_read_inner(
	server: &Server,
	blob: tg::Blob,
	position: u64,
) -> tg::Result<Option<Cursor<Bytes>>> {
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		match current_blob {
			tg::Blob::Leaf(leaf) => {
				let (id, object) = {
					let state = leaf.state().read().unwrap();
					(
						state.id.clone(),
						state.object.as_ref().map(|object| object.as_ref().clone()),
					)
				};
				let bytes = if let Some(object) = object {
					object.bytes.clone()
				} else {
					server.get_object(&id.unwrap().into()).await?.bytes.clone()
				};
				if position < current_blob_position + bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			tg::Blob::Branch(branch) => {
				for child in branch.children(server).await?.iter() {
					if position < current_blob_position + child.size {
						current_blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.size;
				}
				return Ok(None);
			},
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
