use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub created_at: i64,
	pub data: Option<tg::sandbox::get::Output>,
	pub id: tg::sandbox::Id,
	pub runner: Option<tg::runner::Id>,
	pub touched_at: i64,
}
