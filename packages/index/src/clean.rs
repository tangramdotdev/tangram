use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
	pub max_sandbox_touched_at: i64,
	pub now: i64,
	pub partition_end: u64,
	pub partition_start: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub bytes: u64,
	pub cache_entries: Vec<tg::artifact::Id>,
	pub done: bool,
	pub grants: usize,
	pub objects: Vec<tg::object::Id>,
	pub processes: Vec<tg::process::Id>,
	pub sandboxes: Vec<tg::sandbox::Id>,
}
