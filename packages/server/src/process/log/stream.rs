use {
	crate::Server,
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	num::ToPrimitive,
	std::{collections::VecDeque, io::SeekFrom},
	tangram_client::prelude::*,
	tangram_store::Store as _,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

impl Server {
	pub async fn process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::log::get::Chunk>>> {
		// Attempt to create a blob reader.
		let output = self
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;

		// Create the state.
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
			SeekFrom::Start(start) => start,
			SeekFrom::Current(current) => current.abs().to_u64().unwrap(),
			SeekFrom::End(end) => {
				let length = inner
					.try_get_log_length(arg.stream)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the log stream"))?
					.unwrap_or_default();
				if end > 0 {
					length
				} else {
					length - end.to_u64().unwrap()
				}
			},
		};

		// Create the stream.
		struct State {
			entries: VecDeque<tangram_store::log::Entry>,
			inner: Inner,
			length: Option<u64>,
			position: u64,
			reverse: bool,
			stream: Option<tg::process::log::Stream>,
		}
		let state = State {
			entries: VecDeque::new(),
			inner,
			length: arg.length.map(|i| i.abs().to_u64().unwrap()),
			position,
			reverse: arg.length.is_some_and(|length| length < 0),
			stream: arg.stream,
		};
		let stream = stream::try_unfold(state, async move |mut state| {
			if state.entries.is_empty() {
				// If we're reversing and the position is zero, we're done.
				if state.reverse && state.position == 0 {
					return Ok(None);
				}

				// If we're not reversing and hit EOF, we're done.
				if !state.reverse {
					let length = state
						.inner
						.try_get_log_length(state.stream)
						.await?
						.unwrap_or_default();
					if dbg!(state.position) >= dbg!(length) {
						return Ok(None);
					}
				}

				// Get as many entries from the log as possible.
				state.entries = state
					.inner
					.try_read_log(state.position, state.length.unwrap_or(4096), state.stream)
					.await?
					.into();

				// Update the stream position.
				state.position = if state.reverse {
					state
						.entries
						.front()
						.map(|entry| {
							if state.stream.is_some() {
								entry.stream_position
							} else {
								entry.combined_position
							}
						})
						.unwrap_or_default()
				} else {
					state
						.entries
						.back()
						.map(|entry| {
							let length = entry.bytes.len().to_u64().unwrap();
							if state.stream.is_some() {
								entry.stream_position + length
							} else {
								entry.combined_position + length
							}
						})
						.unwrap_or_default()
				};
				dbg!(state.position);
			}

			// Get the next log entry.
			let entry = if state.reverse {
				state.entries.pop_back()
			} else {
				state.entries.pop_front()
			};
			let Some(entry) = entry else {
				return Ok(None);
			};
			let position = if state.stream.is_some() {
				entry.stream_position
			} else {
				entry.combined_position
			};
			let chunk = tg::process::log::get::Chunk {
				bytes: entry.bytes,
				position,
				stream: entry.stream,
			};
			Ok(Some((chunk, state)))
		})
		.boxed();

		Ok(stream)
	}
}

enum Inner {
	Blob(BlobInner),
	Store(StoreInner),
}

struct BlobInner {
	entry: usize,
	reader: crate::read::Reader,
	index: super::serialized::Index,
}

struct StoreInner {
	server: Server,
	process: tg::process::Id,
}

impl Inner {
	async fn try_read_log(
		&mut self,
		position: u64,
		mut length: u64,
		stream: Option<tg::process::log::Stream>,
	) -> tg::Result<Vec<tangram_store::log::Entry>> {
		match self {
			Inner::Blob(inner) => {
				let mut output = Vec::with_capacity(length.to_usize().unwrap());
				while length > 0 {
					// Break if we run out of entries.
					if inner.entry == inner.index.entries.len() {
						break;
					}

					// Read the next chunk from the log.
					let range = inner.index.entries[inner.entry];
					inner
						.reader
						.seek(SeekFrom::Start(range.offset))
						.await
						.map_err(|source| tg::error!(!source, "failed to seek"))?;
					let mut bytes = vec![0u8; range.length.to_usize().unwrap()];
					inner
						.reader
						.read_exact(&mut bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to read the log entry"))?;
					let mut entry =
						tangram_serialize::from_slice::<tangram_store::log::Entry>(&bytes)
							.map_err(|source| tg::error!(!source, "log blob is corrupted"))?;

					// Skip any entry not appearing in this stream.
					if stream.is_some_and(|stream| stream != entry.stream) {
						continue;
					}

					let offset = if stream.is_some() {
						position
							.saturating_sub(entry.stream_position)
							.to_usize()
							.unwrap()
					} else {
						position
							.saturating_sub(entry.combined_position)
							.to_usize()
							.unwrap()
					};

					let take = (entry.bytes.len() - offset).min(length.to_usize().unwrap());
					entry.bytes = entry.bytes[offset..offset + take].to_vec().into();
					if stream.is_some() {
						entry.stream_position += offset.to_u64().unwrap();
					} else {
						entry.stream_position += offset.to_u64().unwrap();
					}
					output.push(entry);

					// Decrement the length counter.
					length -= take.to_u64().unwrap();

					// Increment the entry counter.
					inner.entry += 1;
				}
				Ok(output)
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

	async fn try_get_log_length(
		&mut self,
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
					.seek(SeekFrom::Start(range.offset))
					.await
					.map_err(|source| tg::error!(!source, "failed to seek in the log blob"))?;
				let mut bytes = vec![0u8; range.length.to_usize().unwrap()];
				inner
					.reader
					.read_exact(&mut bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the log entry"))?;
				let entry = tangram_serialize::from_slice::<tangram_store::log::Entry>(&bytes)
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
