use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub ids: Vec<tg::object::Id>,
}
