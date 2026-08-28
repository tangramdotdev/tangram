use tangram_client::prelude::*;

pub mod delete;
pub mod dequeue;
pub mod put;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Entry {
	pub id: tg::object::Id,
	pub partition: u64,
	pub stored_at: i64,
}
