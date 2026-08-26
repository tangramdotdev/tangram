use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub bytes: Option<Bytes>,
	pub checkout_pointer: Option<super::checkout::Pointer>,
	pub id: tg::object::Id,
	pub length: Option<u64>,
	pub stored_at: i64,
}
