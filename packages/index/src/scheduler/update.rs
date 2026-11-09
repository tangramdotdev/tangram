use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::scheduler::Id,
	pub value: Value,
}

#[derive(Clone, Debug)]
pub enum Value {
	Available(bool),
}
