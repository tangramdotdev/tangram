use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub end: tg::process::log::End,
	pub process: tg::process::Id,
}
