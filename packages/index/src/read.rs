use tangram_client::prelude::*;

pub type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub enum Request {
	ContainsIds {
		ids: Vec<tg::Id>,
	},
	GetIndexers,
	GetPermissions {
		args: Vec<crate::permission::get::Arg>,
	},
	GetRunnerSandboxes {
		runner: tg::runner::Id,
	},
	GetSchedulerProcesses {
		scheduler: tg::scheduler::Id,
	},
	GetSchedulerRunners {
		scheduler: tg::scheduler::Id,
	},
	GetSchedulerSandboxes {
		scheduler: tg::scheduler::Id,
	},
	GetSchedulers,
	GetSubjectsForPrincipal {
		principal: tg::Principal,
	},
	GetTransactionId,
	ListProcessesForCreator {
		arg: crate::process::list::Arg,
	},
	ListProcessesForOwner {
		arg: crate::process::list::Arg,
	},
	ListSandboxIds {
		after: Option<tg::sandbox::Id>,
		limit: usize,
	},
	ListSandboxesForCreator {
		arg: crate::sandbox::list::Arg,
	},
	ListSandboxesForOwner {
		arg: crate::sandbox::list::Arg,
	},
	PermissionCaptureBatch {
		batch_size: usize,
		partition_end: u64,
		partition_start: u64,
	},
	ProcessHasAncestor {
		ancestor: tg::process::Id,
		process: tg::process::Id,
	},
	TryGetAncestors {
		id: tg::Id,
	},
	TryGetCachedProcesses {
		command: tg::object::Id,
	},
	TryGetCheckouts {
		ids: Vec<tg::Id>,
	},
	TryGetGroups {
		ids: Vec<tg::group::Id>,
	},
	TryGetIdsForSpecifiers {
		specifiers: Vec<tg::Specifier>,
	},
	TryGetIndexer(crate::indexer::get::Arg),
	TryGetObjectChildren {
		id: tg::object::Id,
	},
	TryGetObjects {
		ids: Vec<tg::object::Id>,
	},
	TryGetOldestUpdateTransactionId {
		kind: crate::update::Kind,
	},
	TryGetOrganizations {
		ids: Vec<tg::organization::Id>,
	},
	TryGetProcessChildren {
		id: tg::process::Id,
		length: u64,
		position: std::io::SeekFrom,
	},
	TryGetProcessChildrenAndObjects {
		id: tg::process::Id,
	},
	TryGetProcessChildrenCount {
		id: tg::process::Id,
	},
	TryGetProcesses {
		ids: Vec<tg::process::Id>,
	},
	TryGetProcessesWithData {
		ids: Vec<tg::process::Id>,
	},
	TryGetRunner {
		runner: tg::runner::Id,
	},
	TryGetSandboxProcesses {
		id: tg::sandbox::Id,
		length: u64,
		position: std::io::SeekFrom,
	},
	TryGetSandboxProcessesCount {
		id: tg::sandbox::Id,
	},
	TryGetSandboxScheduler {
		sandbox: tg::sandbox::Id,
	},
	TryGetSandboxes {
		ids: Vec<tg::sandbox::Id>,
	},
	TryGetScheduler(crate::scheduler::get::Arg),
	TryGetSpecifiersForIds {
		ids: Vec<tg::Id>,
	},
	TryGetSyncs {
		ids: Vec<tg::sync::Id>,
	},
	TryGetTags {
		ids: Vec<tg::tag::Id>,
	},
	TryGetUsers {
		ids: Vec<tg::user::Id>,
	},
	VerifyBatch {
		args: Vec<crate::verify::Arg>,
		config: crate::verify::Config,
		principal: tg::Principal,
	},
	Visible {
		ids: Vec<tg::Id>,
		principal: tg::Principal,
	},
}

pub enum Response {
	ContainsIds(Vec<bool>),
	GetIndexers(Vec<crate::indexer::Indexer>),
	GetPermissions(Vec<Vec<tg::authorization::Permission>>),
	GetRunnerSandboxes(Vec<tg::sandbox::Id>),
	GetSchedulerProcesses(Vec<crate::scheduler::process::QueuedProcess>),
	GetSchedulerRunners(Vec<tg::runner::Id>),
	GetSchedulerSandboxes(Vec<crate::runner::SchedulerSandbox>),
	GetSchedulers(Vec<crate::scheduler::Scheduler>),
	GetSubjectsForPrincipal(Vec<tg::authorization::Subject>),
	GetTransactionId(u64),
	ListProcesses(crate::process::list::Output),
	ListSandboxIds(Vec<tg::sandbox::Id>),
	ListSandboxes(crate::sandbox::list::Output),
	PermissionCaptureBatch(Vec<crate::permission::capture::Entry>),
	ProcessHasAncestor(bool),
	TryGetAncestors(Option<Vec<tg::Id>>),
	TryGetCachedProcesses(Vec<(tg::process::Id, crate::process::Process, tg::process::Data)>),
	TryGetCheckouts(Vec<Option<crate::checkout::Checkout>>),
	TryGetGroups(Vec<Option<crate::group::Group>>),
	TryGetIdsForSpecifiers(Vec<Option<tg::Id>>),
	TryGetIndexer(Option<crate::indexer::Indexer>),
	TryGetObjectChildren(Option<Vec<tg::object::Id>>),
	TryGetObjects(Vec<Option<crate::object::Object>>),
	TryGetOldestUpdateTransactionId(Option<u64>),
	TryGetOrganizations(Vec<Option<crate::organization::Organization>>),
	TryGetProcessChildren(Option<Vec<tg::process::data::Child>>),
	TryGetProcessChildrenAndObjects(Option<crate::process::NodeChildren>),
	TryGetProcessChildrenCount(Option<u64>),
	TryGetProcesses(Vec<Option<crate::process::Process>>),
	TryGetProcessesWithData(Vec<Option<(crate::process::Process, Option<tg::process::Data>)>>),
	TryGetRunner(Option<crate::runner::Runner>),
	TryGetSandboxProcesses(Option<Vec<tg::process::Id>>),
	TryGetSandboxProcessesCount(Option<u64>),
	TryGetSandboxScheduler(Option<tg::scheduler::Id>),
	TryGetSandboxes(Vec<Option<crate::sandbox::Sandbox>>),
	TryGetScheduler(Option<crate::scheduler::Scheduler>),
	TryGetSpecifiersForIds(Vec<Option<tg::Specifier>>),
	TryGetSyncs(Vec<Option<crate::sync::Sync>>),
	TryGetTags(Vec<Option<crate::tag::Tag>>),
	TryGetUsers(Vec<Option<crate::user::User>>),
	VerifyBatch(Vec<crate::verify::Output>),
	Visible(Vec<bool>),
}
