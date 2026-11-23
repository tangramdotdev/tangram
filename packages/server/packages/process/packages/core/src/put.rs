use tangram_client::prelude::*;

pub struct Options {
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,

	pub defer_index: bool,

	pub location: Option<tg::Location>,

	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,

	/// The number of the write among the writes of the process's holder in the process's boot.
	pub sequence: u64,

	pub store_data: bool,
}
