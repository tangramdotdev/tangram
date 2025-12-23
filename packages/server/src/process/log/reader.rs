use {
	crate::{Server, process::log::serialized},
	bytes::Bytes,
	futures::{FutureExt, future::BoxFuture, io::Cursor},
	num::ToPrimitive,
	std::{io::SeekFrom, pin::Pin, task::Poll},
	sync_wrapper::SyncWrapper,
	tangram_client::prelude::*,
	tangram_store::Store as _,
	tokio::io::{AsyncRead, AsyncSeek},
};

pub struct Reader {
	cursor: Option<Cursor<Bytes>>,
	position: u64,
	process: tg::process::Id,
	seek_future: Option<SyncWrapper<SeekFuture>>,
	read_future: Option<SyncWrapper<ReadFuture>>,
	server: Server,
	compacted: Option<CompactedLog>,
	stream: Option<tg::process::log::Stream>,
}

struct ReadFutureResult {
	cursor: Option<Cursor<Bytes>>,
	compacted: Option<CompactedLog>,
}

struct SeekFutureResult {
	position: u64,
	compacted: Option<CompactedLog>,
}

type ReadFuture = BoxFuture<'static, tg::Result<ReadFutureResult>>;
type SeekFuture = BoxFuture<'static, tg::Result<SeekFutureResult>>;

struct CompactedLog {
	entry: usize,
	index: serialized::Index,
	reader: crate::read::Reader,
}

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
		let compacted = if let Some(log) = output.data.log {
			let blob = tg::Blob::with_id(log);
			let mut reader = crate::read::Reader::new(server, blob).await?;
			let index = server.read_log_index_from_blob(&mut reader).await?;
			Some(CompactedLog {
				entry: 0,
				index,
				reader,
			})
		} else {
			None
		};
		Ok(Reader {
			cursor: None,
			compacted,
			position: 0,
			process: id.clone(),
			seek_future: None,
			read_future: None,
			server: server.clone(),
			stream,
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
			let server = this.server.clone();
			let position = this.position;
			let length = buf.capacity().to_u64().unwrap();
			let stream = this.stream;
			let process = this.process.clone();
			let compacted = this.compacted.take();
			let read =
				SyncWrapper::new(
					async move {
						read_future(&server, &process, compacted, stream, position, length).await
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
					this.cursor = result.cursor;
					this.compacted = result.compacted;
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
			let server = this.server.clone();
			let current_position = this.position;
			let process = this.process.clone();
			let stream = this.stream;
			let compacted = this.compacted.take();
			async move {
				poll_seek_store_inner(&server, &process, compacted, stream, current_position, seek)
					.await
			}
			.boxed()
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
				this.position = result.position;
				this.compacted = result.compacted;
				Poll::Ready(Ok(this.position))
			},
			Poll::Ready(Err(error)) => Poll::Ready(Err(std::io::Error::other(error))),
		}
	}
}

async fn read_future(
	server: &Server,
	process: &tg::process::Id,
	compacted: Option<CompactedLog>,
	stream: Option<tg::process::log::Stream>,
	position: u64,
	length: u64,
) -> tg::Result<ReadFutureResult> {
	if let Some(mut compacted) = compacted {
		let mut output = Vec::with_capacity(length.to_usize().unwrap());
		while output.len().to_u64().unwrap() <= length && length > 0 {
			// break out of the loop if we hit eof.
			if compacted.entry == compacted.index.entries.len() {
				break;
			}
			let entry: tangram_store::log::Chunk = todo!("deserialize the log entry");
			// skip entries not in this stream.
			if stream.is_some_and(|stream| stream != entry.stream) {
				continue;
			}
			let entry_position = stream
				.is_some()
				.then_some(entry.stream_position)
				.unwrap_or(entry.combined_position);
			let offset = position.saturating_sub(entry_position).to_usize().unwrap();
			let remaining = (length - output.len().to_u64().unwrap())
				.to_usize()
				.unwrap();
			let available = entry.bytes.len() - offset;
			let take = remaining.min(available);
			output.extend_from_slice(&entry.bytes[offset..offset + take]);
		}
		let cursor = Some(Cursor::new(output.into()));
		return Ok(ReadFutureResult {
			cursor,
			compacted: Some(compacted),
		});
	}
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
		.map(|chunk| ReadFutureResult {
			cursor: chunk.map(Cursor::new),
			compacted: None,
		})
}

async fn poll_seek_store_inner(
	server: &Server,
	process: &tg::process::Id,
	compacted: Option<CompactedLog>,
	stream: Option<tg::process::log::Stream>,
	current_position: u64,
	seek: SeekFrom,
) -> tg::Result<SeekFutureResult> {
	if let Some(mut compacted) = compacted {
		todo!(
			"find the last entry in the log if stream is none, else find the last entry corresponding to the same stream. If none can be found, return position = 0 with compacted: Some"
		);
		todo!("given the position at the end of the log, compute the next position");
		todo!(
			"using the compacted.index, find the entry offset corresponding to the earliest chunk containing that position"
		);
		todo!("update the compacted.entry to this index");
		todo!("return the result");
	}
	let Some(stream_length) = server
		.store
		.try_get_log_length(process, stream)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the stream length"))?
	else {
		return Ok(SeekFutureResult {
			position: 0,
			compacted: None,
		});
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

	Ok(SeekFutureResult {
		position,
		compacted: None,
	})
}
