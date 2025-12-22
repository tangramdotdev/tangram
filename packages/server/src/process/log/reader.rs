use {
	crate::Server,
	bytes::Bytes,
	futures::{FutureExt, future::BoxFuture, io::Cursor},
	num::ToPrimitive,
	std::{io::SeekFrom, pin::Pin, task::Poll},
	sync_wrapper::SyncWrapper,
	tangram_client::prelude::*,
	tangram_store::Store as _,
	tokio::io::{AsyncRead, AsyncSeek},
};

pub enum Reader {
	Blob(crate::read::Reader),
	Store(Store),
}

pub struct Store {
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	process: tg::process::Id,
	seek_future: Option<SyncWrapper<SeekFuture>>,
	read_future: Option<SyncWrapper<ReadFuture>>,
	server: Server,
	stream: Option<tg::process::log::Stream>,
}

type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;
type SeekFuture = BoxFuture<'static, tg::Result<u64>>;

impl Reader {
	pub async fn new(
		server: &Server,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Self> {
		// Attempt to create a blob reader.
		let output = server
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;
		if let Some(log) = output.data.log {
			let blob = tg::Blob::with_id(log);
			let reader = crate::read::Reader::new(server, blob).await?;
			return Ok(Self::Blob(reader));
		}

		// Attempt to create a file reader.
		let store = Store {
			cursor: None,
			position: 0,
			process: id.clone(),
			seek_future: None,
			read_future: None,
			server: server.clone(),
			stream,
		};

		Ok(Self::Store(store))
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match self.get_mut() {
			Reader::Blob(reader) => Pin::new(reader).poll_read(cx, buf),
			Reader::Store(reader) => Pin::new(reader).poll_read(cx, buf),
		}
	}
}

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
		match self.get_mut() {
			Reader::Blob(reader) => Pin::new(reader).start_seek(position),
			Reader::Store(reader) => Pin::new(reader).start_seek(position),
		}
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<std::io::Result<u64>> {
		match self.get_mut() {
			Reader::Blob(reader) => Pin::new(reader).poll_complete(cx),
			Reader::Store(reader) => Pin::new(reader).poll_complete(cx),
		}
	}
}

impl AsyncRead for Store {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read_future.is_none() {
			let server = this.server.clone();
			let position = this.position;
			let length = buf.capacity().to_u64().unwrap();
			let stream = this.stream;
			let process = this.process.clone();
			let read =
				SyncWrapper::new(
					async move {
						poll_read_store_inner(&server, &process, stream, position, length).await
					}
					.boxed(),
				);
			this.read_future.replace(read);
		}

		// Poll the read future if necessary.
		if let Some(read) = this.read_future.as_mut() {
			match read.get_mut().as_mut().poll(cx) {
				Poll::Pending => {
					return Poll::Pending;
				},
				Poll::Ready(Err(error)) => {
					this.read_future.take();
					return Poll::Ready(Err(std::io::Error::other(error)));
				},
				Poll::Ready(Ok(None)) => {
					this.read_future.take();
					return Poll::Ready(Ok(()));
				},
				Poll::Ready(Ok(Some(cursor))) => {
					this.read_future.take();
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

impl AsyncSeek for Store {
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.seek_future.take();
		this.read_future.take();
		this.cursor.take();
		let future = SyncWrapper::new({
			let server = this.server.clone();
			let current_position = this.position;
			let process = this.process.clone();
			let stream = this.stream;
			async move {
				poll_seek_store_inner(&server, &process, stream, current_position, seek).await
			}.boxed()
		});
		this.seek_future.replace(future);
		Ok(())
	}

	fn poll_complete(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		let this = self.get_mut();
		match this
			.seek_future
			.as_mut()
			.unwrap()
			.get_mut()
			.as_mut()
			.poll(cx)
		{
			Poll::Pending => Poll::Pending,
			Poll::Ready(Ok(position)) => {
				this.position = position;
				Poll::Ready(Ok(position))
			},
			Poll::Ready(Err(error)) => Poll::Ready(Err(std::io::Error::other(error))),
		}
	}
}

async fn poll_read_store_inner(
	server: &Server,
	process: &tg::process::Id,
	stream: Option<tg::process::log::Stream>,
	position: u64,
	length: u64,
) -> tg::Result<Option<Cursor<Bytes>>> {
	let arg = tangram_store::ReadLogArg {
		position,
		stream,
		length,
		process: process.clone(),
	};
	server
		.store
		.try_read_log(arg)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the log chunk"))
		.map(|chunk| chunk.map(|chunk| Cursor::new(chunk)))
}

async fn poll_seek_store_inner(
	server: &Server,
	process: &tg::process::Id,
	stream: Option<tg::process::log::Stream>,
	current_position: u64,
	seek: SeekFrom,
) -> tg::Result<u64> {
	let Some(stream_length) = server
		.store
		.try_get_log_length(process, stream)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the stream length"))?
	else {
		return Ok(0);
	};

	// Compute the new posiiton.
	let position = match seek {
		SeekFrom::Start(start) => start,
		SeekFrom::Current(diff) => (current_position.to_i64().unwrap() + diff)
			.max(0)
			.to_u64()
			.unwrap(),
		SeekFrom::End(diff) => {
			if diff >= 0 {
				stream_length
			} else {
				stream_length.saturating_sub(diff.to_u64().unwrap())
			}
		},
	};

	Ok(position)
}
