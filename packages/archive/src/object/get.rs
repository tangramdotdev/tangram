use {bytes::Bytes, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub bytes: Option<Bytes>,
}
