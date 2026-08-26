use {super::Object, tangram_client::prelude::*};

pub mod batch;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub object: Option<Object<'static>>,
}
