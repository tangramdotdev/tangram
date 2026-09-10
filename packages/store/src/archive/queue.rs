use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
	pub indexer: tg::indexer::Id,
	pub object: tg::object::Id,
	pub put: [u8; 16],
	pub sequence: u64,
}
