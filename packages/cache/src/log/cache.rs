use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Entry {
	pub expires_at: i64,
	pub partition: u64,
	pub process: tg::process::Id,
}
