use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub object: Option<Object>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub bytes: Bytes,
	pub put: [u8; 16],
}
