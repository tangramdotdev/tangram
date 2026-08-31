use {super::Object, tangram_client::prelude::*};

pub mod batch;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
	pub put: Option<[u8; 16]>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub object: Option<Object<'static>>,
}
