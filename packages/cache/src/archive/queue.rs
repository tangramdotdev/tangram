use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Item {
	pub indexer: tg::indexer::Id,
	pub payload: bytes::Bytes,
	pub sequence: u64,
}
