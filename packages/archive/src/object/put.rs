use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub bytes: Bytes,
	pub id: tg::object::Id,
	pub put: [u8; 16],
}
