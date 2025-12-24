use {
	crate::Server,
	bytes::Bytes,
	futures::{FutureExt as _, future::BoxFuture, io::Cursor},
	num::ToPrimitive,
	std::{
		io::SeekFrom,
		pin::Pin,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
		task::Poll,
	},
	sync_wrapper::SyncWrapper,
	tangram_client::prelude::*,
	tangram_store::Store as _,
	tokio::{
		io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _},
		sync::Mutex,
	},
};

pub struct Reader {
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	stream: Option<tg::process::log::Stream>,
	seek_future: Option<SyncWrapper<SeekFuture>>,
	read_future: Option<SyncWrapper<ReadFuture>>,
	inner: Arc<Inner>,
}
type ReadFuture = BoxFuture<'static, tg::Result<Option<Cursor<Bytes>>>>;
type SeekFuture = BoxFuture<'static, tg::Result<Option<u64>>>;

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
		let inner = if let Some(id) = output.data.log {
			let blob = tg::Blob::with_id(id);
			let mut reader = crate::read::Reader::new(server, blob).await?;
			let entry = AtomicUsize::new(0);
			let index = server.read_log_index_from_blob(&mut reader).await?;
			Inner::Blob(BlobInner {
				entry,
				reader: Mutex::new(reader),
				index,
			})
		} else {
			Inner::Store(StoreInner {
				server: server.clone(),
				process: id.clone(),
			})
		};
		Ok(Reader {
			cursor: None,
			position: 0,
			stream,
			seek_future: None,
			read_future: None,
			inner: Arc::new(inner),
		})
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		let this = self.get_mut();

		// Create the read future if necessary.
		if this.cursor.is_none() && this.read_future.is_none() {
			let inner = Arc::clone(&this.inner);
			let position = this.position;
			let length = buf.remaining().to_u64().unwrap();
			let stream = this.stream;
			let read = SyncWrapper::new(
				async move {
					inner
						.try_read_log(position, length, stream)
						.await
						.map(|bytes| bytes.map(Cursor::new))
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
				Poll::Ready(Ok(result)) => {
					this.read_future.take();
					this.cursor = result;
					return Poll::Ready(Ok(()));
				},
			}
		}

		// Read.
		let Some(cursor) = this.cursor.as_mut() else {
			return Poll::Ready(Ok(()));
		};
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

impl AsyncSeek for Reader {
	fn start_seek(self: Pin<&mut Self>, seek: std::io::SeekFrom) -> std::io::Result<()> {
		let this = self.get_mut();
		this.seek_future.take();
		this.read_future.take();
		this.cursor.take();
		let future = SyncWrapper::new({
			let inner = Arc::clone(&this.inner);
			let current_position = this.position;
			let stream = this.stream;
			async move { inner.try_seek_log(seek, current_position, stream).await }.boxed()
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
			Poll::Ready(Ok(result)) => {
				if let Some(position) = result {
					this.position = position;
				}
				Poll::Ready(Ok(this.position))
			},
			Poll::Ready(Err(error)) => Poll::Ready(Err(std::io::Error::other(error))),
		}
	}
}

enum Inner {
	Blob(BlobInner),
	Store(StoreInner),
}

struct BlobInner {
	entry: AtomicUsize,
	reader: Mutex<crate::read::Reader>,
	index: super::serialized::Index,
}

struct StoreInner {
	server: Server,
	process: tg::process::Id,
}

impl Inner {
	async fn try_read_log(
		self: Arc<Self>,
		position: u64,
		length: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<Bytes>> {
		match self.as_ref() {
			Inner::Blob(inner) => {
				let mut output = Vec::with_capacity(length.to_usize().unwrap());
				let mut entry = inner.entry.load(Ordering::SeqCst);
				while output.len().to_u64().unwrap() <= length && length > 0 {
					// Break if we run out of entries.
					if entry == inner.index.entries.len() {
						break;
					}

					// Read the next chunk from the log.
					let range = inner.index.entries[entry];
					inner
						.reader
						.lock()
						.await
						.seek(SeekFrom::Start(range.offset))
						.await
						.map_err(|source| tg::error!(!source, "failed to seek"))?;
					let mut bytes = vec![0u8; range.length.to_usize().unwrap()];
					inner
						.reader
						.lock()
						.await
						.read_exact(&mut bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to read the log entry"))?;
					let chunk = tangram_serialize::from_slice::<tangram_store::log::Chunk>(&bytes)
						.map_err(|source| tg::error!(!source, "log blob is corrupted"))?;

					// Skip any entry not appearing in this stream.
					if stream.is_some_and(|stream| stream != chunk.stream) {
						continue;
					}

					// Compute how many bytes to take from the entry.
					let entry_position = if stream.is_some() {
						chunk.stream_position
					} else {
						chunk.combined_position
					};
					let offset = position.saturating_sub(entry_position).to_usize().unwrap();
					let remaining = (length - output.len().to_u64().unwrap())
						.to_usize()
						.unwrap();
					let available = chunk.bytes.len() - offset;
					let take = remaining.min(available);

					// Copy the bytes out of the log.
					output.extend_from_slice(&chunk.bytes[offset..offset + take]);

					// Increment the entry counter.
					entry += 1;
				}
				inner.entry.store(entry, Ordering::SeqCst);
				Ok(Some(output.into()))
			},
			Inner::Store(inner) => {
				let arg = tangram_store::ReadLogArg {
					length,
					position,
					process: inner.process.clone(),
					stream,
				};
				inner
					.server
					.store
					.try_read_log(arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the log"))
			},
		}
	}

	pub async fn try_seek_log(
		self: Arc<Self>,
		seek: SeekFrom,
		current_position: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		// Compute the new position.
		let position = match seek {
			SeekFrom::Start(position) => position,
			SeekFrom::Current(diff) => (current_position.to_i64().unwrap() + diff)
				.to_u64()
				.unwrap(),
			SeekFrom::End(diff) => {
				let Some(end) = self.try_get_log_length(stream).await? else {
					return Ok(None);
				};
				(end.to_i64().unwrap() + diff).to_u64().unwrap()
			},
		};

		if let Inner::Blob(inner) = self.as_ref() {
			let map = match stream {
				None => &inner.index.combined,
				Some(tg::process::log::Stream::Stderr) => &inner.index.stderr,
				Some(tg::process::log::Stream::Stdout) => &inner.index.stdout,
			};
			let index = map
				.range(0..position)
				.next_back()
				.map(|(_, index)| *index)
				.unwrap_or_default();
			inner
				.entry
				.store(index.to_usize().unwrap(), Ordering::SeqCst);
		}

		Ok(Some(position))
	}

	async fn try_get_log_length(
		&self,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			Inner::Blob(inner) => {
				let range = match stream {
					None => inner.index.entries.last().copied(),
					Some(tg::process::log::Stream::Stderr) => inner
						.index
						.stderr
						.values()
						.next_back()
						.copied()
						.map(|index| inner.index.entries[index.to_usize().unwrap()]),
					Some(tg::process::log::Stream::Stdout) => inner
						.index
						.stdout
						.values()
						.next_back()
						.copied()
						.map(|index| inner.index.entries[index.to_usize().unwrap()]),
				};
				let Some(range) = range else {
					return Ok(Some(0));
				};
				inner
					.reader
					.lock()
					.await
					.seek(SeekFrom::Start(range.offset))
					.await
					.map_err(|source| tg::error!(!source, "failed to seek in the log blob"))?;
				let mut bytes = vec![0u8; range.length.to_usize().unwrap()];
				inner
					.reader
					.lock()
					.await
					.read_exact(&mut bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the log entry"))?;
				let entry = tangram_serialize::from_slice::<tangram_store::log::Chunk>(&bytes)
					.map_err(|source| tg::error!(!source, "log blob is corrupted"))?;
				let position = if stream.is_some() {
					entry.stream_position
				} else {
					entry.combined_position
				};
				Ok(Some(position + entry.bytes.len().to_u64().unwrap()))
			},
			Inner::Store(inner) => inner
				.server
				.store
				.try_get_log_length(&inner.process, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the log")),
		}
	}
}
