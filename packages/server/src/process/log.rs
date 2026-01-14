use {
	crate::{Server, store::ReadProcessLogArg},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::io::Cursor,
	tangram_client as tg,
	tangram_database::{self as db, Database as _, Query as _},
	tangram_futures::{read::Ext as _, write::Ext as _},
	tangram_store::Store as _,
	tg::Handle,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _},
};

pub mod get;
pub mod post;
pub mod stream;

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
	pub stream: tg::process::log::Stream,

	#[tangram_serialize(id = 4)]
	pub stream_position: u64,
}

impl Server {
	pub(crate) async fn read_log_index_from_blob(
		&self,
		reader: &mut crate::read::Reader,
	) -> tg::Result<Index> {
		// Read the version.
		let version = reader
			.read_u8()
			.await
			.map_err(|source| tg::error!(!source, "failed to read u8"))?;
		if version != 0 {
			return Err(tg::error!("expected a 0 byte"));
		}

		// Read the index.
		let index_length = reader
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "expected a uvarint"))?;
		let mut index = vec![0u8; index_length.to_usize().unwrap()];
		reader
			.read_exact(&mut index)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the log index"))?;

		// Deserialize the index.
		let mut index = tangram_serialize::from_slice::<Index>(&index)
			.map_err(|source| tg::error!(!source, "failed to deserialize the index"))?;

		// Get the current position.
		let position = reader
			.stream_position()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream position"))?;

		// The serialized blob positions are relative to the end of the index, so bump them.
		for entry in &mut index.entries {
			entry.blob_position += position;
		}

		Ok(index)
	}

	pub(crate) async fn compact_process_log(&self, process: &tg::process::Id) -> tg::Result<()> {
		// Check if the log is already compacted.
		let output = self
			.try_get_process_local(process, false)
			.await?
			.ok_or_else(|| tg::error!("expected the process to exist"))?;
		if output.data.log.is_some() {
			return Ok(());
		}

		let entries = self
			.store
			.try_read_process_log(ReadProcessLogArg {
				process: process.clone(),
				position: 0,
				length: u64::MAX,
				stream: None,
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to get the log entries"))?;

		// Build the index and serialized entries.
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
				tg::process::log::Stream::Stderr => {
					index.stderr.push(i.to_u32().unwrap());
				},
				tg::process::log::Stream::Stdout => {
					index.stdout.push(i.to_u32().unwrap());
				},
			}

			entries_bytes.extend_from_slice(&serialized);

			blob_position += blob_length;
		}

		// Create the blob.
		let index = tangram_serialize::to_vec(&index).unwrap();
		let mut blob_bytes = vec![0u8];
		blob_bytes
			.write_uvarint(index.len().to_u64().unwrap())
			.await
			.unwrap();
		blob_bytes.extend_from_slice(&index);
		blob_bytes.extend_from_slice(&entries_bytes);

		// Write the blob.
		let blob = self.write(Cursor::new(blob_bytes)).await?.blob;

		// Update the process.
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
				where id = {p}1 and log is null;
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
