use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::group::Id,
	pub name: String,
	pub parent: Option<tg::Id>,
}
