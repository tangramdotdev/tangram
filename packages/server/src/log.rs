use {
	self::store::{DeleteProcessLogArg, ReadProcessLogArg},
	crate::Server,
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeSet, VecDeque},
		io::{Cursor, SeekFrom},
	},
	tangram_client::{self as tg, Handle as _},
	tangram_database::{self as db, Database as _, Query as _},
	tangram_futures::{read::Ext as _, write::Ext as _},
	tangram_log_store::Store as _,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

pub use self::store::Store;

pub mod store;

enum Inner {
	Blob(BlobInner),
	Store(StoreInner),
}

struct BlobInner {
	entry: usize,
	reader: crate::read::Reader,
	index: Index,
}

struct StoreInner {
	server: Server,
	process: tg::process::Id,
}

#[derive(Clone, Debug, Default, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
pub struct Index {
	#[tangram_serialize(id = 0)]
	pub entries: Vec<Entry>,

	#[tangram_serialize(id = 1)]
	pub stdout: Vec<u32>,

	#[tangram_serialize(id = 2)]
	pub stderr: Vec<u32>,
}

#[derive(Copy, Clone, Debug, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
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

impl Server {
	pub(crate) async fn read_log_index_from_blob(
		&self,
		reader: &mut crate::read::Reader,
	) -> tg::Result<Index> {
		let version = reader
			.read_u8()
			.await
			.map_err(|source| tg::error!(!source, "failed to read u8"))?;
		if version != 0 {
			return Err(tg::error!("expected a 0 byte"));
		}

		let index_length = reader
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "expected a uvarint"))?;
		let mut index = vec![0u8; index_length.to_usize().unwrap()];
		reader
			.read_exact(&mut index)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log index"))?;

		let mut index = tangram_serialize::from_slice::<Index>(&index)
			.map_err(|source| tg::error!(!source, "failed to deserialize the index"))?;

		let position = reader
			.stream_position()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;

		for entry in &mut index.entries {
			entry.blob_position += position;
		}

		Ok(index)
	}

	pub(crate) async fn compact_process_log(&self, process: &tg::process::Id) -> tg::Result<()> {
		let output = self
			.try_get_process_local(process, false)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;
		if output.data.log.is_some()
			|| !(output.data.stdout.is_log() || output.data.stderr.is_log())
		{
			return Ok(());
		}

		let entries = self
			.log_store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: u64::MAX,
				streams: BTreeSet::from([
					tg::process::stdio::Stream::Stdout,
					tg::process::stdio::Stream::Stderr,
				]),
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to get the log entries"))?;

		let mut index = Index::default();
		let mut entries_bytes = Vec::new();
		let mut blob_position = 0u64;

		for (i, entry) in entries.into_iter().enumerate() {
			let serialized = tangram_serialize::to_vec(&entry).unwrap();
			let blob_length = serialized.len().to_u64().unwrap();

			index.entries.push(Entry {
				blob_position,
				blob_length,
				combined_position: entry.position,
				stream: entry.stream,
				stream_position: entry.stream_position,
			});
			match entry.stream {
				tg::process::stdio::Stream::Stderr => {
					index.stderr.push(i.to_u32().unwrap());
				},
				tg::process::stdio::Stream::Stdout => {
					index.stdout.push(i.to_u32().unwrap());
				},
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
			}

			entries_bytes.extend_from_slice(&serialized);

			blob_position += blob_length;
		}

		let index = tangram_serialize::to_vec(&index).unwrap();
		let mut blob_bytes = vec![0u8];
		blob_bytes
			.write_uvarint(index.len().to_u64().unwrap())
			.await
			.unwrap();
		blob_bytes.extend_from_slice(&index);
		blob_bytes.extend_from_slice(&entries_bytes);

		let arg = tg::write::Arg::default();
		let blob = self.write(arg, Cursor::new(blob_bytes)).await?.blob;

		let connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get db connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set log = {p}2
				where id = {p}1 and log is null;
			"
		);
		let params = db::params![process.to_string(), blob.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		self.log_store
			.delete_process_log(DeleteProcessLogArg {
				process: process.clone(),
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the process log from store"))?;

		Ok(())
	}

	pub(crate) async fn process_log_stream(
		&self,
		id: &tg::process::Id,
		position: Option<SeekFrom>,
		length: Option<i64>,
		size: Option<u64>,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>> {
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
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;

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

		let position = match position.unwrap_or(SeekFrom::Start(0)) {
			SeekFrom::Start(position) => position,
			SeekFrom::Current(_) => {
				return Err(tg::error!("SeekFrom::Current is not supported"));
			},
			SeekFrom::End(offset) => {
				let length = inner
					.try_get_process_log_length(&streams)
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

		let log_length = if length.is_some_and(|length| length < 0) {
			None
		} else {
			inner
				.try_get_process_log_length(&streams)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the log length"))?
		};

		struct State {
			entries: VecDeque<tangram_log_store::ProcessLogEntry<'static>>,
			inner: Inner,
			log_length: Option<u64>,
			position: u64,
			reverse: bool,
			size: u64,
			streams: BTreeSet<tg::process::stdio::Stream>,
			total_length: Option<u64>,
		}

		let state = State {
			entries: VecDeque::new(),
			inner,
			log_length,
			position,
			reverse: length.is_some_and(|length| length < 0),
			size: size.unwrap_or(4096),
			streams,
			total_length: length.map(i64::unsigned_abs),
		};

		let stream = stream::try_unfold(state, async move |mut state| {
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
				if let Some(total_length) = state.total_length {
					length = length.min(total_length);
				}
				let position = if state.reverse {
					state.position.saturating_sub(length)
				} else {
					state.position
				};

				state.entries = state
					.inner
					.try_read_process_log(position, length, &state.streams)
					.await?
					.into();

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
					let blob = tg::Blob::with_id(blob_id);
					let mut reader = crate::read::Reader::new(&inner.server, blob).await?;
					let index = inner.server.read_log_index_from_blob(&mut reader).await?;
					state.inner = Inner::Blob(BlobInner {
						entry: 0,
						reader,
						index,
					});

					state.entries = state
						.inner
						.try_read_process_log(position, length, &state.streams)
						.await?
						.into();
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
			let position = entry_position(&entry, &state.streams);
			let chunk = tg::process::stdio::Chunk {
				bytes: entry.bytes.into_owned().into(),
				position: Some(position),
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
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<tangram_log_store::ProcessLogEntry<'static>>> {
		match self {
			Inner::Blob(inner) => inner.try_read_process_log(position, length, streams).await,
			Inner::Store(inner) => inner.try_read_process_log(position, length, streams).await,
		}
	}

	async fn try_get_process_log_length(
		&mut self,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			Inner::Blob(inner) => inner.try_get_process_log_length(streams).await,
			Inner::Store(inner) => inner.try_get_process_log_length(streams).await,
		}
	}
}

impl BlobInner {
	async fn try_read_process_log(
		&mut self,
		position: u64,
		mut length: u64,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<tangram_log_store::ProcessLogEntry<'static>>> {
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

	async fn try_get_process_log_length(
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

	async fn read_entry(&mut self) -> tg::Result<tangram_log_store::ProcessLogEntry<'static>> {
		let entry = self.index.entries[self.entry];
		self.reader
			.seek(SeekFrom::Start(entry.blob_position))
			.await
			.map_err(|source| tg::error!(!source, "failed to seek"))?;
		let mut bytes = vec![0u8; entry.blob_length.to_usize().unwrap()];
		self.reader
			.read_exact(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log entry"))?;
		let entry: tangram_log_store::ProcessLogEntry<'_> =
			tangram_serialize::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "log blob is corrupted"))?;
		Ok(entry.into_static())
	}
}

impl StoreInner {
	async fn try_read_process_log(
		&self,
		position: u64,
		length: u64,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Vec<tangram_log_store::ProcessLogEntry<'static>>> {
		let arg = tangram_log_store::ReadProcessLogArg {
			length,
			position,
			process: self.process.clone(),
			streams: streams.clone(),
		};
		self.server
			.log_store
			.try_read_process_log(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log"))
	}

	async fn try_get_process_log_length(
		&self,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		self.server
			.log_store
			.try_get_process_log_length(&self.process, streams)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log"))
	}
}

fn entry_position(
	entry: &tangram_log_store::ProcessLogEntry<'_>,
	streams: &BTreeSet<tg::process::stdio::Stream>,
) -> u64 {
	if streams.len() > 1 {
		entry.position
	} else {
		entry.stream_position
	}
}
