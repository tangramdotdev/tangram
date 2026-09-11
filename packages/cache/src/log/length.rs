use {std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub process: tg::process::Id,
	pub streams: BTreeSet<tg::process::stdio::Stream>,
}
