use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;
pub mod update;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Indexer {
	pub archive_read_sequence: u64,
	pub archive_write_sequence: u64,
	pub available: bool,
	pub id: tg::indexer::Id,
	pub index_read_sequence: u64,
	pub index_write_sequence: u64,
}

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Data {
	#[tangram_serialize(id = 0)]
	archive_read_sequence: u64,

	#[tangram_serialize(id = 1)]
	archive_write_sequence: u64,

	#[tangram_serialize(id = 2)]
	available: bool,

	#[tangram_serialize(id = 3)]
	index_read_sequence: u64,

	#[tangram_serialize(id = 4)]
	index_write_sequence: u64,
}

impl Indexer {
	#[must_use]
	pub fn new(id: tg::indexer::Id) -> Self {
		Self {
			archive_read_sequence: 0,
			archive_write_sequence: 0,
			available: false,
			id,
			index_read_sequence: 0,
			index_write_sequence: 0,
		}
	}

	pub(crate) fn deserialize(id: tg::indexer::Id, bytes: &[u8]) -> tg::Result<Self> {
		let data: Data = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the indexer"))?;
		let indexer = Self {
			archive_read_sequence: data.archive_read_sequence,
			archive_write_sequence: data.archive_write_sequence,
			available: data.available,
			id,
			index_read_sequence: data.index_read_sequence,
			index_write_sequence: data.index_write_sequence,
		};

		Ok(indexer)
	}

	pub(crate) fn serialize(&self) -> tg::Result<Vec<u8>> {
		let data = Data {
			archive_read_sequence: self.archive_read_sequence,
			archive_write_sequence: self.archive_write_sequence,
			available: self.available,
			index_read_sequence: self.index_read_sequence,
			index_write_sequence: self.index_write_sequence,
		};
		let bytes = tangram_serialize::to_vec(&data)
			.map_err(|error| tg::error!(!error, "failed to serialize the indexer"))?;

		Ok(bytes)
	}
}
