use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
	pub now: i64,
	pub ttl: u64,
}
