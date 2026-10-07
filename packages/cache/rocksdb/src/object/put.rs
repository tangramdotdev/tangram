use {bytes::Bytes, tangram_cache::object, tangram_client::prelude::*};

pub(crate) struct Request {
	pub bytes: Option<Bytes>,
	pub checkout_pointer: Option<object::checkout::Pointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub put: [u8; 16],
}
