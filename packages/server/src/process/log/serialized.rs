use {
	crate::Server,
	bytes::Bytes,
	futures::stream::FuturesOrdered,
	indoc::formatdoc,
	num::ToPrimitive,
	std::{
		collections::BTreeMap,
		io::Cursor,
		sync::{Arc, Mutex},
	},
	tangram_client as tg,
	tangram_database::{self as db, Database as _, Query as _},
	tangram_store::Store as _,
	tg::Handle,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
	zerocopy::{FromZeros as _, IntoBytes as _},
};

const MAGIC: [u8; 8] = *b"tgpcslog";

#[derive(
	Copy,
	Clone,
	Debug,
	zerocopy::FromBytes,
	zerocopy::IntoBytes,
	zerocopy::Immutable,
	zerocopy::KnownLayout,
)]
#[repr(C)]
pub struct Header {
	pub magic: [u8; 8],
	pub index_length: u64,
	pub entries_length: u64,
}

#[derive(Clone, Debug, Default, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
pub struct Index {
	#[tangram_serialize(id = 0)]
	pub combined: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 1)]
	pub stdout: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 2)]
	pub stderr: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 3)]
	pub entries: Vec<Range>,
}

#[derive(
	Copy, Clone, Debug, Default, tangram_serialize::Serialize, tangram_serialize::Deserialize,
)]
pub struct Range {
	#[tangram_serialize(id = 0)]
	pub offset: u64,

	#[tangram_serialize(id = 1)]
	pub length: u64,
}

impl Server {
	pub(crate) async fn read_log_index_from_blob(
		&self,
		reader: &mut crate::read::Reader,
	) -> tg::Result<Index> {
		// Read the header.
		let mut header = Header::new_zeroed();
		reader
			.read_exact(header.as_mut_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to read the header"))?;

		// Validate the header.
		if header.magic != MAGIC {
			return Err(tg::error!("expected a serialized process log"));
		}

		// Read the index.
		let mut index = vec![0u8; header.index_length.to_usize().unwrap()];
		reader
			.read_exact(&mut index)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log index"))?;

		// Deserialize the index.
		let mut index = tangram_serialize::from_slice::<Index>(&index)
			.map_err(|source| tg::error!(!source, "failed to deserialize the index"))?;

		// The serialized offsets are relative to the end of the index, so bump all offsets by size_of(Header) + index.len();
		for range in &mut index.entries {
			range.offset += header.index_length + std::mem::size_of::<Header>().to_u64().unwrap();
		}

		Ok(index)
	}

	pub(crate) async fn compact_process_log(&self, process: &tg::process::Id) -> tg::Result<()> {
		let num_entries = self
			.store
			.try_get_num_log_entries(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the number of log entries"))?
			.unwrap_or_default();

		// Create a stream for the entries, which we read into a blob.
		struct State {
			index: Index,
			offset: u64,
		}
		let state = State {
			index: Index::default(),
			offset: 0,
		};
		let state = Arc::new(Mutex::new(state));
		let stream = (0..num_entries)
			.map({
				let state = state.clone();
				let server = self.clone();
				let process = process.clone();
				move |index| {
					let state = state.clone();
					let server = server.clone();
					let process = process.clone();
					async move {
						let chunk = server
							.store
							.try_get_log_entry(&process, index)
							.await
							.map_err(std::io::Error::other)?
							.ok_or_else(|| std::io::Error::other("expected a chunk"))?;
						let mut state = state.lock().unwrap();
						let serialized = tangram_serialize::to_vec(&chunk).unwrap();

						let range = Range {
							offset: state.offset,
							length: serialized.len().to_u64().unwrap(),
						};

						state.index.combined.insert(chunk.combined_position, index);
						state.index.entries.push(range);
						match chunk.stream {
							tg::process::log::Stream::Stderr => {
								state.index.stderr.insert(chunk.stream_position, index)
							},
							tg::process::log::Stream::Stdout => {
								state.index.stdout.insert(chunk.stream_position, index)
							},
						};
						state.offset += range.length;
						Ok::<_, std::io::Error>(Bytes::from(serialized))
					}
				}
			})
			.collect::<FuturesOrdered<_>>();

		// Serialize the entries.
		let reader = StreamReader::new(stream);
		let entries_blob = self.write(reader).await?.blob;

		// Serialize the index.
		let state = Arc::into_inner(state).unwrap().into_inner().unwrap();
		let index = tangram_serialize::to_vec(&state.index).unwrap();
		let index_blob = self.write(Cursor::new(index.clone())).await?.blob;

		// Serialize the header.
		let header = Header {
			magic: MAGIC,
			index_length: index.len().to_u64().unwrap(),
			entries_length: state.offset.to_u64().unwrap(),
		};
		let header_blob = self
			.write(Cursor::new(header.as_bytes().to_vec()))
			.await?
			.blob;

		// Create the main blob.
		let children = vec![
			tg::blob::Child {
				blob: tg::Blob::with_id(header_blob),
				length: std::mem::size_of::<Header>().to_u64().unwrap(),
			},
			tg::blob::Child {
				blob: tg::Blob::with_id(index_blob),
				length: header.index_length,
			},
			tg::blob::Child {
				blob: tg::Blob::with_id(entries_blob),
				length: header.entries_length,
			},
		];
		let blob = tg::Blob::new(children);
		let blob = blob
			.store(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the blob"))?;

		// Update the processes.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get db connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set log = {p}2
				where id = {p}1;
			"
		);
		let params = db::params![process.to_string(), blob.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
