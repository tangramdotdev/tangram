use tangram_client::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub bytes: u64,
	pub cache_entries: Vec<tg::artifact::Id>,
	pub done: bool,
	pub objects: Vec<tg::object::Id>,
	pub processes: Vec<tg::process::Id>,
}
