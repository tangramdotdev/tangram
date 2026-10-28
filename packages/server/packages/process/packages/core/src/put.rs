use tangram_client::prelude::*;

pub struct Options {
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,
	pub defer_index: bool,
	pub location: Option<tg::Location>,
	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,
	pub put_children: bool,
	pub store_data: bool,
}
