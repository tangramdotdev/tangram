use tangram_client::prelude::*;

pub struct Options {
	pub command_sync: Option<tg::Referent<tg::sync::Id>>,

	pub defer_index: bool,

	pub location: Option<tg::Location>,

	pub outcome_sync: Option<tg::Referent<tg::sync::Id>>,

	pub registration: Option<tangram_index::runner::Registration>,

	/// The boot that the process's sandbox must still be at for the put to apply, for a finish of a queued process of a boot that ended.
	pub sandbox_boot: Option<tangram_index::process::put::SandboxBoot>,

	pub store_data: bool,
}
