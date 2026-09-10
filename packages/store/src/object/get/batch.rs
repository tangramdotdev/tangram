use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub bytes: bool,
	pub ids: Vec<tg::object::Id>,
}
