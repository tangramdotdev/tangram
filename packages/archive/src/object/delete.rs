use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
	pub put: [u8; 16],
}
