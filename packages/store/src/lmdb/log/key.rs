use tangram_client::prelude::*;

#[derive(Debug)]
pub enum Key<'a> {
	End {
		process: &'a tg::process::Id,
	},
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
