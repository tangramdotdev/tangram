use tangram_client::prelude::*;

#[derive(Clone)]
pub(super) enum Response {
	AggregateUsageOutput(tangram_index::usage::aggregate::Output),
	Checkouts(Vec<Option<tangram_index::checkout::Checkout>>),
	CleanOutput(tangram_index::clean::Output),
	ExpireUsageOutput(tangram_index::usage::expire::Output),
	Mutation(tg::Result<()>),
	Objects(Vec<Option<tangram_index::object::Object>>),
	Processes(Vec<Option<tangram_index::process::Process>>),
	Unit,
	UpdateOutput(tangram_index::update::Output),
	Usage(tangram_index::usage::Aggregate),
}
