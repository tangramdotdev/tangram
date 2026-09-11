use tangram_client::prelude::*;

pub mod object;

#[derive(Clone, Debug)]
pub struct Arg {
	pub cache: [u8; 16],
	pub id: tg::object::Id,
	pub partition: u64,
	pub put: [u8; 16],
}
