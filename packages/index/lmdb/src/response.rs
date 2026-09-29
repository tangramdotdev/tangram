#[derive(Clone)]
pub(super) enum Response {
	AggregateUsageOutput(tangram_index::usage::aggregate::Output),
	Unit,
	Checkouts(Vec<Option<tangram_index::checkout::Checkout>>),
	Objects(Vec<Option<tangram_index::object::Object>>),
	Processes(Vec<Option<tangram_index::process::Process>>),
	Usage(tangram_index::usage::Aggregate),
	CleanOutput(tangram_index::clean::Output),
	ExpireUsageOutput(tangram_index::usage::expire::Output),
	UpdateOutput(tangram_index::update::Output),
}
