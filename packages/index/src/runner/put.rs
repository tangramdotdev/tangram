use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct Arg {
	pub id: tg::runner::Id,
	pub scheduler: Option<tg::scheduler::Id>,
}
