use tangram_client::prelude::*;

pub const CHANNEL_CAPACITY: usize = 256;

pub type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub enum Request {
	ContainsIds {
		ids: Vec<tg::Id>,
	},

	GetIndexers,
	GetRequesterSubjects {
		principal: tg::Principal,
	},
	GetRunnerSandboxes {
		runner: tg::runner::Id,
	},

	GetTransactionId,
	ListSandboxes,
	ListSandboxesForCreator {
		creator: tg::Principal,
	},
	ListSandboxesForOwner {
		owner: tg::Principal,
	},
	ProcessHasAncestor {
		ancestor: tg::process::Id,
		process: tg::process::Id,
	},
	PermissionCaptureBatch {
		batch_size: usize,
		partition_end: u64,
		partition_start: u64,
	},
	TryGetAncestors {
		id: tg::Id,
	},
	TryGetCheckouts {
		ids: Vec<tg::Id>,
	},
	TryGetCachedProcesses {
		command: tg::object::Id,
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
	TryGetProcessChildrenCount {
		id: tg::process::Id,
	},
	TryGetProcessChildrenAndObjects {
		id: tg::process::Id,
	},
	TryGetProcesses {
		ids: Vec<tg::process::Id>,
	},
	TryGetSandboxProcesses {
		id: tg::sandbox::Id,
		length: u64,
		position: std::io::SeekFrom,
	},
	TryGetSandboxProcessesCount {
		id: tg::sandbox::Id,
	},
	TryGetSandboxes {
		ids: Vec<tg::sandbox::Id>,
	},
	TryGetSpecifiersForIds {
		ids: Vec<tg::Id>,
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
	GetRequesterSubjects(Vec<tg::authorization::Subject>),
	GetRunnerSandboxes(Vec<tg::sandbox::Id>),
	GetTransactionId(u64),
	ListSandboxes(Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>),
	ProcessHasAncestor(bool),
	PermissionCaptureBatch(Vec<crate::permission::capture::Entry>),
	TryGetAncestors(Option<Vec<tg::Id>>),
	TryGetCheckouts(Vec<Option<crate::checkout::Checkout>>),
	TryGetCachedProcesses(Vec<(tg::process::Id, crate::process::Process)>),
	TryGetGroups(Vec<Option<crate::group::Group>>),
	TryGetIdsForSpecifiers(Vec<Option<tg::Id>>),
	TryGetIndexer(Option<crate::indexer::Indexer>),
	TryGetObjectChildren(Option<Vec<tg::object::Id>>),
	TryGetObjects(Vec<Option<crate::object::Object>>),
	TryGetOldestUpdateTransactionId(Option<u64>),
	TryGetOrganizations(Vec<Option<crate::organization::Organization>>),
	TryGetProcessChildren(Option<Vec<tg::process::data::Child>>),
	TryGetProcessChildrenCount(Option<u64>),
	TryGetProcessChildrenAndObjects(Option<crate::process::NodeChildren>),
	TryGetProcesses(Vec<Option<crate::process::Process>>),
	TryGetSandboxProcesses(Option<Vec<tg::process::Id>>),
	TryGetSandboxProcessesCount(Option<u64>),
	TryGetSandboxes(Vec<Option<crate::sandbox::Sandbox>>),
	TryGetSpecifiersForIds(Vec<Option<tg::Specifier>>),
	TryGetTags(Vec<Option<crate::tag::Tag>>),
	TryGetUsers(Vec<Option<crate::user::User>>),
	VerifyBatch(Vec<crate::verify::Output>),
	Visible(Vec<bool>),
}
