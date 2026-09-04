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
}
