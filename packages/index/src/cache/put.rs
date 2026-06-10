use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub dependencies: Vec<tg::artifact::Id>,
	pub id: tg::artifact::Id,
	pub touched_at: i64,
}
