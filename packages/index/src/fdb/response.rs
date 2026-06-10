#[derive(Clone)]
pub(super) enum Response {
	Unit,
	CacheEntries(Vec<Option<crate::cache::Entry>>),
	Objects(Vec<Option<crate::object::Object>>),
	Processes(Vec<Option<crate::process::Process>>),
	CleanOutput(crate::clean::Output),
	UpdateCount(usize),
}
