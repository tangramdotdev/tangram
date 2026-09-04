use tangram_client::prelude::*;

pub mod batch;
pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fragment {
	pub batch: batch::Id,
	pub fragment: u64,
	pub fragments: u64,
	pub indexer: tg::indexer::Id,
	pub payload: bytes::Bytes,
	pub sequence: u64,
}
