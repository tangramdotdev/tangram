use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Entry {
	pub cache: [u8; 16],
	pub id: tg::object::Id,
	pub partition: u64,
	pub put: [u8; 16],
}
