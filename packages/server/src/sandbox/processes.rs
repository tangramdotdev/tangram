use tangram_client::prelude::*;

mod get;

pub(crate) struct Output {
	pub length: u64,
	pub processes: Vec<tg::process::Id>,
	pub status: tg::sandbox::Status,
}
