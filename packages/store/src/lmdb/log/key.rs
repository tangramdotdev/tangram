use tangram_client::prelude::*;

#[derive(Debug)]
pub enum Key<'a> {
	Entry {
		position: u64,
		process: &'a tg::process::Id,
	},
	StreamPosition {
		position: u64,
		process: &'a tg::process::Id,
		stream: tg::process::stdio::Stream,
	},
}
