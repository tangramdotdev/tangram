use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub indexer: tg::indexer::Id,
	pub sequence_end: u64,
	pub sequence_start: u64,
}
