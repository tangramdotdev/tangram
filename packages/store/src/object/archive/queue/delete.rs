use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub indexer: tg::indexer::Id,
	pub sequence: u64,
}
