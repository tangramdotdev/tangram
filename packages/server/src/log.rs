use {
	crate::Session,
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeSet, VecDeque},
		io::SeekFrom,
	},
	tangram_cache::{Cache as _, log},
	tangram_client as tg,
	tangram_futures::read::Ext as _,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

mod writer;

pub(crate) use writer::Writer;

enum Inner {
	Blob(BlobInner),
	Cache(CacheInner),
}

struct BlobInner {
	entry: usize,
	reader: crate::read::Reader,
	index: Index,
}

struct CacheInner {
	session: Session,
	process: tg::process::Id,
}

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Index {
	#[tangram_serialize(id = 0)]
	pub entries: Vec<Entry>,

	#[tangram_serialize(id = 1)]
	pub stdout: Vec<u32>,

	#[tangram_serialize(id = 2)]
	pub stderr: Vec<u32>,
}

#[derive(Clone, Copy, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Entry {
	#[tangram_serialize(id = 0)]
	pub blob_position: u64,

	#[tangram_serialize(id = 1)]
	pub blob_length: u64,

	#[tangram_serialize(id = 2)]
	pub combined_position: u64,

	#[tangram_serialize(id = 3)]
	pub stream: tg::process::stdio::Stream,

	#[tangram_serialize(id = 4)]
	pub stream_position: u64,
}

impl Session {
	pub(crate) async fn read_log_index_from_blob(
		&self,
		reader: &mut crate::read::Reader,
	) -> tg::Result<Index> {
		let version = reader
			.read_u8()
			.await
			.map_err(|error| tg::error!(!error, "failed to read u8"))?;
		if version != 0 {
			return Err(tg::error!("expected a 0 byte"));
		}

		let index_length = reader
			.read_uvarint()
			.await
			.map_err(|error| tg::error!(!error, "expected a uvarint"))?;
		let mut index = vec![0u8; index_length.to_usize().unwrap()];
		reader
			.read_exact(&mut index)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the log index"))?;

		let mut index = tangram_serialize::from_slice::<Index>(&index)
			.map_err(|error| tg::error!(!error, "failed to deserialize the index"))?;

		let position = reader
			.stream_position()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the stream position"))?;

		for entry in &mut index.entries {
			entry.blob_position += position;
		}

		Ok(index)
	}

	pub(crate) async fn process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: &mut tg::process::stdio::read::Arg,
		mut end: Option<tg::process::stdio::End>,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<(
		Option<tg::process::stdio::End>,
		BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	)> {
		if streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if streams.len() > 2 {
			return Err(tg::error!("invalid log streams"));
		}
		if streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("invalid stdio stream"));
		}
		let output = self
			.try_get_process_local(id, false, false, &[], tg::process::Source::Auto)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;

		let mut inner = if let Some(log) = output.data.log {
			let blob = tg::Blob::with_referent(log);
			let mut reader = crate::read::Reader::new(self, blob).await?;
			let index = self.read_log_index_from_blob(&mut reader).await?;
			Inner::Blob(BlobInner {
				entry: 0,
				reader,
				index,
			})
		} else {
			// Authorize.
			let permission = tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::NodeLogObjects,
			);
			let authorized = self
				.authorize(id.clone(), permission)
				.await?
				.check_exhaustion()?;
			if !authorized.permissions.contains(permission) {
				return Err(tg::error!("unauthorized"));
			}

			Inner::Cache(CacheInner {
				session: self.clone(),
				process: id.clone(),
			})
		};

		// Resolve the requested window against the current log length.
		let mut log_length = inner
			.try_get_length(&streams)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the log length"))?;
		if log_length.is_none() && inner.try_switch_to_blob().await? {
			log_length = inner
				.try_get_length(&streams)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the log length"))?;
		}
		// Recover the final positions from the finished log when no cached marker remains.
		if end.is_none()
			&& let Inner::Blob(inner) = &mut inner
		{
			let stderr = tg::process::stdio::Stream::Stderr;
			let stdout = tg::process::stdio::Stream::Stdout;
			let stderr_position = inner
				.try_get_length(&BTreeSet::from([stderr]))
				.await?
				.unwrap();
			let stdout_position = inner
				.try_get_length(&BTreeSet::from([stdout]))
				.await?
				.unwrap();
			end = Some(tg::process::stdio::End {
				combined_position: stderr_position + stdout_position,
				stream_positions: [(stderr, stderr_position), (stdout, stdout_position)].into(),
			});
		}

		let mut position = match arg.position.unwrap_or(SeekFrom::Start(0)) {
			SeekFrom::Start(position) => position,
			SeekFrom::Current(_) => {
				return Err(tg::error!("SeekFrom::Current is not supported"));
			},
			SeekFrom::End(offset) => {
				let length = log_length.unwrap_or_default();
				if offset >= 0 {
					length.saturating_add(offset.to_u64().unwrap())
				} else {
					length.saturating_sub(offset.unsigned_abs())
				}
			},
		};

		// Clip at EOF only when completion rules out further writes.
		if end.is_some()
			&& let Some(length) = &mut arg.length
			&& *length < 0
		{
			let end = position.min(log_length.unwrap_or_default());
			*length = length.saturating_add_unsigned(position - end).min(0);
			position = end;
		}
		arg.position = Some(SeekFrom::Start(position));

		struct State {
			entries: VecDeque<log::read::Entry<'static>>,
			inner: Inner,
			log_length: Option<u64>,
			position: u64,
			remaining: Option<u64>,
			reverse: bool,
			size: u64,
			streams: BTreeSet<tg::process::stdio::Stream>,
		}

		let state = State {
			entries: VecDeque::new(),
			inner,
			log_length,
			position,
			remaining: arg.length.map(i64::unsigned_abs),
			reverse: arg.length.is_some_and(|length| length < 0),
			size: arg.size.unwrap_or(4096),
			streams,
		};

		let stream = stream::try_unfold(state, async move |mut state| {
			if state.remaining == Some(0) {
				return Ok(None);
			}
			if state.entries.is_empty() {
				if state.reverse && state.position == 0 {
					return Ok(None);
				}

				if !state.reverse
					&& state
						.log_length
						.is_some_and(|length| length <= state.position)
				{
					return Ok(None);
				}

				let mut length = state.size;
				if let Some(remaining) = state.remaining {
					length = length.min(remaining);
				}
				let position = if state.reverse {
					length = length.min(state.position);
					state.position - length
				} else {
					state.position
				};

				state.entries = state
					.inner
					.try_read(position, length, &state.streams)
					.await?
					.into();

				if state.entries.is_empty() && state.inner.try_switch_to_blob().await? {
					state.entries = state
						.inner
						.try_read(position, length, &state.streams)
						.await?
						.into();
				}

				// A reverse read must reach its cursor before yielding any part of the window.
				if state.reverse
					&& state.entries.back().is_some_and(|entry| {
						entry_position(entry, &state.streams) + entry.bytes.len().to_u64().unwrap()
							< state.position
					}) {
					return Ok(None);
				}

				let boundary = if state.reverse {
					state.entries.front()
				} else {
					state.entries.back()
				};
				state.position = boundary
					.map(|entry| {
						let position = entry_position(entry, &state.streams);
						if state.reverse {
							position
						} else {
							position + entry.bytes.len().to_u64().unwrap()
						}
					})
					.unwrap_or_default();
			}

			let Some(entry) = (if state.reverse {
				state.entries.pop_back()
			} else {
				state.entries.pop_front()
			}) else {
				return Ok(None);
			};
			let chunk = tg::process::stdio::Chunk {
				bytes: entry.bytes.into_owned().into(),
				combined_position: entry.position,
				stream: entry.stream,
				stream_position: entry.stream_position,
				timestamp: Some(entry.timestamp),
			};
			if let Some(remaining) = &mut state.remaining {
				*remaining -= chunk.bytes.len().to_u64().unwrap();
			}
			Ok(Some((chunk, state)))
		})
		.boxed();

		Ok((end, stream))
	}
}

impl Inner {
	async fn try_read(
		&mut self,
		position: u64,
		length: u64,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		match self {
			Inner::Blob(inner) => inner.try_read(position, length, streams).await,
			Inner::Cache(inner) => inner.try_read(position, length, streams).await,
		}
	}

	async fn try_get_length(
		&mut self,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			Inner::Blob(inner) => inner.try_get_length(streams).await,
			Inner::Cache(inner) => inner.try_get_length(streams).await,
		}
	}

	async fn try_switch_to_blob(&mut self) -> tg::Result<bool> {
		let Inner::Cache(inner) = self else {
			return Ok(false);
		};
		let Some(output) = inner
			.session
			.try_get_process_local(&inner.process, false, false, &[], tg::process::Source::Auto)
			.await?
		else {
			return Ok(false);
		};
		let Some(log) = output.data.log else {
			return Ok(false);
		};
		let blob = tg::Blob::with_referent(log);
		let mut reader = crate::read::Reader::new(&inner.session, blob).await?;
		let index = inner.session.read_log_index_from_blob(&mut reader).await?;
		*self = Inner::Blob(BlobInner {
			entry: 0,
			reader,
			index,
		});

		Ok(true)
	}
}

impl BlobInner {
	async fn try_read(
		&mut self,
		position: u64,
		mut length: u64,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		self.entry = if streams.len() > 1 {
			let index = self
				.index
				.entries
				.partition_point(|entry| entry.combined_position <= position);
			index.saturating_sub(1)
		} else {
			let stream = streams
				.iter()
				.next()
				.copied()
				.ok_or_else(|| tg::error!("expected at least one log stream"))?;
			match stream {
				tg::process::stdio::Stream::Stdout => {
					let index = self.index.stdout.partition_point(|&index| {
						self.index.entries[index as usize].stream_position <= position
					});
					index
						.checked_sub(1)
						.map_or(0, |index| self.index.stdout[index] as usize)
				},
				tg::process::stdio::Stream::Stderr => {
					let index = self.index.stderr.partition_point(|&index| {
						self.index.entries[index as usize].stream_position <= position
					});
					index
						.checked_sub(1)
						.map_or(0, |index| self.index.stderr[index] as usize)
				},
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
			}
		};

		let mut output = Vec::with_capacity(16);
		while length > 0 && self.entry < self.index.entries.len() {
			let mut entry = self.read_entry().await?;
			self.entry += 1;

			if !streams.contains(&entry.stream) {
				continue;
			}

			let offset = position
				.saturating_sub(entry_position(&entry, streams))
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

	async fn try_get_length(
		&mut self,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		let last_index = if streams.len() > 1 {
			self.index.entries.len().checked_sub(1)
		} else {
			let stream = streams
				.iter()
				.next()
				.copied()
				.ok_or_else(|| tg::error!("expected at least one log stream"))?;
			match stream {
				tg::process::stdio::Stream::Stdout => self.index.stdout.last().map(|&i| i as usize),
				tg::process::stdio::Stream::Stderr => self.index.stderr.last().map(|&i| i as usize),
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
			}
		};
		let Some(last_index) = last_index else {
			return Ok(Some(0));
		};

		let saved = self.entry;
		self.entry = last_index;
		let entry = self.read_entry().await?;
		self.entry = saved;

		let position = entry_position(&entry, streams);
		Ok(Some(position + entry.bytes.len().to_u64().unwrap()))
	}

	async fn read_entry(&mut self) -> tg::Result<log::read::Entry<'static>> {
		let entry = self.index.entries[self.entry];
		self.reader
			.seek(SeekFrom::Start(entry.blob_position))
			.await
			.map_err(|error| tg::error!(!error, "failed to seek"))?;
		let mut bytes = vec![0u8; entry.blob_length.to_usize().unwrap()];
		self.reader
			.read_exact(&mut bytes)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the log entry"))?;
		let entry: log::read::Entry<'_> = tangram_serialize::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "log blob is corrupted"))?;
		Ok(entry.into_static())
	}
}

impl CacheInner {
	async fn try_read(
		&self,
		position: u64,
		length: u64,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		let arg = log::read::Arg {
			length,
			position,
			process: self.process.clone(),
			streams: streams.clone(),
		};
		self.session
			.server
			.cache
			.try_read_log(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the log"))
	}

	async fn try_get_length(
		&self,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		let arg = log::length::Arg {
			process: self.process.clone(),
			streams: streams.clone(),
		};
		self.session
			.server
			.cache
			.try_get_log_length(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the log"))
	}
}

fn entry_position(
	entry: &log::read::Entry<'_>,
	streams: &BTreeSet<tg::process::stdio::Stream>,
) -> u64 {
	if streams.len() > 1 {
		entry.position
	} else {
		entry.stream_position
	}
}
