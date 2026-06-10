use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::group::Id,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}
