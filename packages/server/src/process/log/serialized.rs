use {
	crate::Server,
	bytes::Bytes,
	futures::stream::FuturesOrdered,
	num::ToPrimitive,
	std::{
		collections::BTreeMap,
		io::Cursor,
		sync::{Arc, Mutex},
	},
	tangram_client as tg,
	tangram_store::Store as _,
	tg::Handle,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

const MAGIC: [u8; 8] = *b"tgpcslog";

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct Header {
	pub magic: [u8; 8],
	pub index_length: u64,
	pub entries_length: u64,
}

#[derive(Debug, Clone, Default, tangram_serialize::Serialize, tangram_serialize::Deserialize)]
pub struct Index {
	#[tangram_serialize(id = 0)]
	pub combined: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 1)]
	pub stdout: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 2)]
	pub stderr: BTreeMap<u64, u64>,

	#[tangram_serialize(id = 3)]
	pub entries: Vec<u64>,
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
		let Index {
			combined,
			stdout,
			stderr,
			entries,
		} = &mut index;

		// The serialized offsets are relative to the end of the index, so bump all offsets by size_of(Header) + index.len();
		let offsets = combined
			.values_mut()
			.chain(stderr.values_mut())
			.chain(stdout.values_mut())
			.chain(entries.iter_mut());
		for offset in offsets {
			*offset += header.index_length + std::mem::size_of::<Header>().to_u64().unwrap();
		}

		Ok(index)
	}

	pub(crate) async fn write_log_to_blob(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<tg::Blob> {
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
							.map_err(|error| std::io::Error::other(error))?
							.ok_or_else(|| std::io::Error::other("expected a chunk"))?;
						let mut state = state.lock().unwrap();
						let offset = state.offset;
						state.index.combined.insert(chunk.combined_position, offset);
						state.index.entries.push(offset);
						match chunk.stream {
							tg::process::log::Stream::Stderr => {
								state.index.stderr.insert(chunk.stream_position, offset)
							},
							tg::process::log::Stream::Stdout => {
								state.index.stdout.insert(chunk.stream_position, offset)
							},
						};
						let serialized = tangram_serialize::to_vec(&chunk).unwrap();
						state.offset += serialized.len().to_u64().unwrap();
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
		blob.store(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the blob"))?;
		Ok(blob)
	}
}

impl Header {
	pub fn new_zeroed() -> Self {
		Self {
			magic: [0u8; 8],
			index_length: 0,
			entries_length: 0,
		}
	}

	pub fn as_bytes(&self) -> &[u8] {
		unsafe {
			let len = std::mem::size_of::<Self>();
			std::slice::from_raw_parts((self as *const Self).cast(), len)
		}
	}

	pub fn as_mut_bytes(&mut self) -> &mut [u8] {
		unsafe {
			let len = std::mem::size_of::<Self>();
			std::slice::from_raw_parts_mut((self as *mut Self).cast(), len)
		}
	}
}
