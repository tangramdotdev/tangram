#[derive(Clone)]
pub(super) enum Response {
	Unit,
	CacheEntries(Vec<Option<crate::cache::Entry>>),
	Objects(Vec<Option<crate::object::Object>>),
	Processes(Vec<Option<crate::process::Process>>),
	Usage(crate::usage::Aggregate),
	CleanOutput(crate::clean::Output),
	CleanUsageOutput(crate::usage::clean::Output),
	CompactUsageOutput(crate::usage::compact::Output),
	UpdateOutput(crate::update::Output),
}
