use {
	crate::Server,
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{collections::VecDeque, io::SeekFrom},
	tangram_client::prelude::*,
	tangram_store::Store as _,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

enum Inner {
	Blob(BlobInner),
	Store(StoreInner),
}

struct BlobInner {
	entry: usize,
	reader: crate::read::Reader,
	index: super::Index,
}

struct StoreInner {
	server: Server,
	process: tg::process::Id,
}

impl Server {
	pub async fn process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::log::get::Chunk>>> {
		// Attempt to create a blob reader.
		let output = self
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;

		// Create the inner.
		let mut inner = if let Some(id) = output.data.log {
			let blob = tg::Blob::with_id(id);
			let mut reader = crate::read::Reader::new(self, blob).await?;
			let index = self.read_log_index_from_blob(&mut reader).await?;
			Inner::Blob(BlobInner {
				entry: 0,
				reader,
				index,
			})
		} else {
			Inner::Store(StoreInner {
				server: self.clone(),
				process: id.clone(),
			})
		};

		// Get the starting position of the stream.
		let position = match arg.position.unwrap_or(SeekFrom::Start(0)) {
			SeekFrom::Start(position) => position,
			SeekFrom::Current(_) => {
				return Err(tg::error!("SeekFrom::Current is not supported"));
			},
			SeekFrom::End(offset) => {
				let length = inner
					.try_get_process_log_length(arg.stream)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the log length"))?
					.unwrap_or_default();
				if offset >= 0 {
					length.saturating_add(offset.to_u64().unwrap())
				} else {
					length.saturating_sub(offset.unsigned_abs())
				}
			},
		};

		// Get the log length for forward iteration.
		let log_length = if arg.length.is_some_and(|length| length < 0) {
			None
		} else {
			inner
				.try_get_process_log_length(arg.stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the log length"))?
		};

		// Create the stream.
		struct State {
			entries: VecDeque<tangram_store::ProcessLogEntry<'static>>,
			inner: Inner,
			log_length: Option<u64>,
			read_length: Option<u64>,
			position: u64,
			reverse: bool,
			stream: Option<tg::process::log::Stream>,
		}
		let state = State {
			entries: VecDeque::new(),
			inner,
			log_length,
			read_length: arg.length.map(|i| i.abs().to_u64().unwrap()),
			position,
			reverse: arg.length.is_some_and(|length| length < 0),
			stream: arg.stream,
		};
		let stream = stream::try_unfold(state, async move |mut state| {
			if state.entries.is_empty() {
				// If we are reversing and the position is zero, we are done.
				if state.reverse && state.position == 0 {
					return Ok(None);
				}

				// If we are not reversing and hit EOF, we are done.
				if !state.reverse
					&& state
						.log_length
						.is_some_and(|length| length <= state.position)
				{
					return Ok(None);
				}

				// Get as many entries from the log as possible.
				state.entries = state
					.inner
					.try_read_process_log(
						state.position,
						state.read_length.unwrap_or(4096),
						state.stream,
					)
					.await?
					.into();

				// If the store returned empty but we have not reached EOF, the log may have been compacted and deleted. Try to fall back to reading from the blob.
				if state.entries.is_empty()
					&& state
						.log_length
						.is_some_and(|length| length > state.position)
					&& let Inner::Store(inner) = &state.inner
					&& let Some(output) = inner
						.server
						.try_get_process_local(&inner.process, false)
						.await? && let Some(blob_id) = output.data.log
				{
					// The log was compacted. Switch to reading from the blob.
					let blob = tg::Blob::with_id(blob_id);
					let mut reader = crate::read::Reader::new(&inner.server, blob).await?;
					let index = inner.server.read_log_index_from_blob(&mut reader).await?;
					state.inner = Inner::Blob(BlobInner {
						entry: 0,
						reader,
						index,
					});

					// Retry reading from the blob.
					state.entries = state
						.inner
						.try_read_process_log(
							state.position,
							state.read_length.unwrap_or(4096),
							state.stream,
						)
						.await?
						.into();
				}

				// Update the stream position.
				let boundary = if state.reverse {
					state.entries.front()
				} else {
					state.entries.back()
				};
				state.position = boundary
					.map(|entry| {
						let pos = entry_position(entry, state.stream);
						if state.reverse {
							pos
						} else {
							pos + entry.bytes.len().to_u64().unwrap()
						}
					})
					.unwrap_or_default();
			}

			// Get the next log entry.
			let Some(entry) = (if state.reverse {
				state.entries.pop_back()
			} else {
				state.entries.pop_front()
			}) else {
				return Ok(None);
			};
			let chunk = tg::process::log::get::Chunk {
				position: entry_position(&entry, state.stream),
				bytes: entry.bytes.into_owned().into(),
				stream: entry.stream,
			};
			Ok(Some((chunk, state)))
		})
		.boxed();

		Ok(stream)
	}
}

impl Inner {
	async fn try_read_process_log(
		&mut self,
		position: u64,
		length: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Vec<tangram_store::ProcessLogEntry<'static>>> {
		match self {
			Inner::Blob(inner) => inner.try_read_process_log(position, length, stream).await,
			Inner::Store(inner) => inner.try_read_process_log(position, length, stream).await,
		}
	}

	async fn try_get_process_log_length(
		&mut self,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			Inner::Blob(inner) => inner.try_get_process_log_length(stream).await,
			Inner::Store(inner) => inner.try_get_process_log_length(stream).await,
		}
	}
}

impl BlobInner {
	async fn try_read_process_log(
		&mut self,
		position: u64,
		mut length: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Vec<tangram_store::ProcessLogEntry<'static>>> {
		// Find the starting entry using binary search for O(log n) lookup.
		self.entry = match stream {
			None => {
				let i = self
					.index
					.entries
					.partition_point(|e| e.combined_position <= position);
				i.saturating_sub(1)
			},
			Some(tg::process::log::Stream::Stdout) => {
				let i = self.index.stdout.partition_point(|&idx| {
					self.index.entries[idx as usize].stream_position <= position
				});
				i.checked_sub(1)
					.map_or(0, |i| self.index.stdout[i] as usize)
			},
			Some(tg::process::log::Stream::Stderr) => {
				let i = self.index.stderr.partition_point(|&idx| {
					self.index.entries[idx as usize].stream_position <= position
				});
				i.checked_sub(1)
					.map_or(0, |i| self.index.stderr[i] as usize)
			},
		};

		let mut output = Vec::with_capacity(16);
		while length > 0 && self.entry < self.index.entries.len() {
			// Read the next entry from the log.
			let mut entry = self.read_entry().await?;
			self.entry += 1;

			// Skip entries not in this stream.
			if stream.is_some_and(|s| s != entry.stream) {
				continue;
			}

			// Slice the entry to the requested position and length.
			let offset = position
				.saturating_sub(entry_position(&entry, stream))
				.to_usize()
				.unwrap();
			let take = entry
				.bytes
				.len()
				.saturating_sub(offset)
				.min(length.to_usize().unwrap());
			if take == 0 {
				continue;
			}
			entry.bytes = entry.bytes[offset..offset + take].to_vec().into();
			entry.position += offset.to_u64().unwrap();
			entry.stream_position += offset.to_u64().unwrap();
			length -= take.to_u64().unwrap();
			output.push(entry);
		}
		Ok(output)
	}

	async fn try_get_process_log_length(
		&mut self,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		// Get the last entry index for this stream.
		let last_index = match stream {
			None => self.index.entries.len().checked_sub(1),
			Some(tg::process::log::Stream::Stdout) => self.index.stdout.last().map(|&i| i as usize),
			Some(tg::process::log::Stream::Stderr) => self.index.stderr.last().map(|&i| i as usize),
		};
		let Some(last_index) = last_index else {
			return Ok(Some(0));
		};

		// Save and restore the entry position.
		let saved = self.entry;
		self.entry = last_index;
		let entry = self.read_entry().await?;
		self.entry = saved;

		let pos = entry_position(&entry, stream);
		Ok(Some(pos + entry.bytes.len().to_u64().unwrap()))
	}

	async fn read_entry(&mut self) -> tg::Result<tangram_store::ProcessLogEntry<'static>> {
		let entry = self.index.entries[self.entry];
		self.reader
			.seek(SeekFrom::Start(entry.blob_position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek"))?;
		let mut bytes = vec![0u8; entry.blob_length.to_usize().unwrap()];
		let mut offset = 0;
		loop {
			let amount = self.reader.read(&mut bytes[offset..])
				.await
				.map_err(|source| tg::error!(!source, "failed to read the entry"))?;
			offset += amount;
			if amount == 0 {
				break;
			}
		}
		if offset != bytes.len() {
			return Err(tg::error!("unexpected eof"));
		}
		let entry: tangram_store::ProcessLogEntry<'_> = tangram_serialize::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "log blob is corrupted"))?;
		Ok(entry.into_static())
	}
}

impl StoreInner {
	async fn try_read_process_log(
		&self,
		position: u64,
		length: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Vec<tangram_store::ProcessLogEntry<'static>>> {
		let arg = tangram_store::ReadProcessLogArg {
			length,
			position,
			process: self.process.clone(),
			stream,
		};
		self.server
			.store
			.try_read_process_log(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log"))
	}

	async fn try_get_process_log_length(
		&self,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Option<u64>> {
		self.server
			.store
			.try_get_process_log_length(&self.process, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log"))
	}
}

fn entry_position(
	entry: &tangram_store::ProcessLogEntry<'_>,
	stream: Option<tg::process::log::Stream>,
) -> u64 {
	if stream.is_some() {
		entry.stream_position
	} else {
		entry.position
	}
}
