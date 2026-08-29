use tangram_client::prelude::*;

pub mod object;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::object::Id,
	pub partition: u64,
	pub stored_at: i64,
}
