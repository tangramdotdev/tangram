use tangram_client::prelude::*;

pub(crate) struct Request {
	pub id: tg::object::Id,
	pub put: [u8; 16],
}
