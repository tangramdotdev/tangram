use crate::Server;
use bytes::{Buf as _, Bytes};
use futures::{FutureExt as _, Stream, StreamExt as _, future::BoxFuture};
use num::ToPrimitive;
use std::{
	io::Cursor,
	panic::AssertUnwindSafe,
	pin::{Pin, pin},
	sync::Arc,
	task::Poll,
};
use sync_wrapper::SyncWrapper;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tokio::io::{
	AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _,
};
use tokio_util::task::AbortOnDropHandle;

pub enum Reader {
	File(File),
	Object(Object),
}

pub struct File {
	current: u64,
	length: u64,
	position: u64,
	reader: tokio::io::BufReader<tokio::fs::File>,
	seeking: bool,
}

pub struct Object {
	blob: tg::Blob,
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	read: Option<SyncWrapper<ReadFuture>>,
	server: Server,
	length: u64,
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
				let result =
					AssertUnwindSafe(server.try_read_blob_task(arg, reader, sender.clone()))
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
					.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
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

	pub(crate) async fn handle_read_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let Some(stream) = handle.try_read_blob_stream(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let mut stream = stream.take_until(stop).boxed().peekable();

		let mut position = None;
		if let Some(Ok(tg::blob::read::Event::Chunk(chunk))) =
			std::pin::Pin::new(&mut stream).peek().await
		{
			position.replace(chunk.position);
		}

		// Create the frame stream.
		let frames = stream.map(|event| match event {
			Ok(tg::blob::read::Event::Chunk(chunk)) => {
				Ok::<_, tg::Error>(hyper::body::Frame::data(chunk.bytes))
			},
			Ok(tg::blob::read::Event::End) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
				Ok(hyper::body::Frame::trailers(trailers))
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				Ok(hyper::body::Frame::trailers(trailers))
			},
		});
		let body = Body::with_stream(frames);

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(position) = position {
			response = response.header("x-tg-position", position);
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Reader {
	pub async fn new(server: &Server, blob: tg::Blob) -> tg::Result<Self> {
		let id = blob.id(server).await?;
		let cache_reference = server.store.try_get_cache_reference(&id.into()).await?;
		let reader = if let Some(cache_reference) = cache_reference {
			let mut path = server
				.cache_path()
				.join(cache_reference.artifact.to_string());
			if let Some(subpath) = &cache_reference.subpath {
				path.push(subpath);
			}
			let file = tokio::fs::File::open(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to open the file"))?;
			let reader = File::new(file, cache_reference.position, cache_reference.length).await?;
			Self::File(reader)
		} else {
			let reader = Object::new(server, blob).await?;
			Self::Object(reader)
		};
		Ok(reader)
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_read(cx, buf),
			Reader::Object(reader) => Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncBufRead for Reader {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_fill_buf(cx),
			Reader::Object(reader) => Pin::new(reader).poll_fill_buf(cx),
		}
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).consume(amt),
			Reader::Object(reader) => Pin::new(reader).consume(amt),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).start_seek(position),
			Reader::Object(reader) => Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::File(reader) => Pin::new(reader).poll_complete(cx),
			Reader::Object(reader) => Pin::new(reader).poll_complete(cx),
		}
	}
}

impl File {
	async fn new(file: tokio::fs::File, position: u64, length: u64) -> tg::Result<Self> {
		let mut reader = tokio::io::BufReader::new(file);
		reader
			.seek(std::io::SeekFrom::Start(position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek the file"))?;
		Ok(Self {
			current: 0,
			length,
			position,
			reader,
			seeking: false,
		})
	}
}

impl AsyncRead for File {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		if this.current >= this.length {
			return Poll::Ready(Ok(()));
		}
		let poll = Pin::new(&mut this.reader).poll_read(cx, buf);
		if let Poll::Ready(Ok(())) = poll {
			let n = buf.filled().len();
			let n = n.min((this.length - this.current).to_usize().unwrap());
			buf.set_filled(n);
			this.current += n.to_u64().unwrap();
			return Poll::Ready(Ok(()));
		}
		poll
	}
}

impl AsyncBufRead for File {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
		let this = self.get_mut();
		if this.current >= this.length {
			return Poll::Ready(Ok(&[]));
		}
		let poll = Pin::new(&mut this.reader).poll_fill_buf(cx);
		if let Poll::Ready(Ok(mut buf)) = poll {
			let n = buf.len();
			let n = n.min((this.length - this.current).to_usize().unwrap());
			buf = &buf[..n];
			return Poll::Ready(Ok(buf));
		}
		poll
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
		let this = self.get_mut();
		let amt = amt.min((this.length - this.current).to_usize().unwrap());
		this.current += amt.to_u64().unwrap();
		this.reader.consume(amt);
	}
}

impl AsyncSeek for File {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.seeking = true;
		let position = match position {
			std::io::SeekFrom::Start(n) => {
				let n = this.position + n;
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::End(n) => {
				let n = this.position + this.length + n.to_u64().unwrap();
				std::io::SeekFrom::Start(n)
			},
			std::io::SeekFrom::Current(n) => std::io::SeekFrom::Current(n),
		};
		Pin::new(&mut this.reader).start_seek(position)
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		let this = self.get_mut();
		match Pin::new(&mut this.reader).poll_complete(cx) {
			Poll::Ready(Ok(n)) => {
				if !this.seeking {
					return Poll::Ready(Ok(0));
				}
				this.seeking = false;
				this.current = n - this.position;
				Poll::Ready(Ok(this.current))
			},
			Poll::Ready(Err(error)) => {
				this.seeking = false;
				Poll::Ready(Err(error))
			},
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Object {
	async fn new(server: &Server, blob: tg::Blob) -> tg::Result<Self> {
		let cursor = None;
		let position = 0;
		let read = None;
		let size = blob.length(server).await?;
		let server = server.clone();
		Ok(Self {
			blob,
			cursor,
			position,
			read,
			server,
			length: size,
		})
	}
}

impl AsyncRead for Object {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
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
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(error)) => {
					this.read.take();
					return Poll::Ready(Err(std::io::Error::other(error)));
				},
				Poll::Ready(Ok(None)) => {
					this.read.take();
					return Poll::Ready(Ok(()));
				},
				Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			}
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

		Poll::Ready(Ok(()))
	}
}

impl AsyncBufRead for Object {
	fn poll_fill_buf(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<&[u8]>> {
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
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(error)) => {
					this.read.take();
					return Poll::Ready(Err(std::io::Error::other(error)));
				},
				Poll::Ready(Ok(None)) => {
					this.read.take();
					return Poll::Ready(Ok(&[]));
				},
				Poll::Ready(Ok(Some(cursor))) => {
					this.read.take();
					this.cursor.replace(cursor);
				},
			}
		}

		// Read.
		let cursor = this.cursor.as_ref().unwrap();
		let bytes = &cursor.get_ref()[cursor.position().to_usize().unwrap()..];

		Poll::Ready(Ok(bytes))
	}

	fn consume(self: Pin<&mut Self>, amt: usize) {
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
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.read.take();
		let position = match seek {
			std::io::SeekFrom::Start(seek) => seek.to_i64().unwrap(),
			std::io::SeekFrom::End(seek) => this.length.to_i64().unwrap() + seek,
			std::io::SeekFrom::Current(seek) => this.position.to_i64().unwrap() + seek,
		};
		let position = position.to_u64().ok_or(std::io::Error::other(
			"attempted to seek to a negative or overflowing position",
		))?;
		if position > this.length {
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
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		Poll::Ready(Ok(self.position))
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
		let (id, object) = {
			let state = current_blob.state().read().unwrap();
			let id = state.id.clone();
			let object = state.object.clone();
			(id, object)
		};
		let object = if let Some(object) = object {
			object
		} else {
			let bytes = server.get_object(&id.unwrap().into()).await?.bytes;
			let data = tg::blob::Data::deserialize(bytes)?;
			let object = tg::blob::Object::try_from(data)?;
			let object = Arc::new(object);
			if object.is_branch() {
				current_blob.state().write().unwrap().object = Some(object.clone());
			}
			object
		};
		match object.as_ref() {
			tg::blob::Object::Leaf(leaf) => {
				if position < current_blob_position + leaf.bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(leaf.bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			tg::blob::Object::Branch(branch) => {
				for child in &branch.children {
					if position < current_blob_position + child.length {
						current_blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.length;
				}
				return Ok(None);
			},
		}
	}
}
