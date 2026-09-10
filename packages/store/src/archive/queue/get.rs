use tangram_client::prelude::*;

pub mod batch;

#[derive(Clone, Debug)]
pub struct Arg {
	pub indexer: tg::indexer::Id,
	pub sequence: u64,
}
