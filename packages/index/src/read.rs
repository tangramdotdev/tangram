use tangram_client::prelude::*;

pub(crate) const CHANNEL_CAPACITY: usize = 256;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(crate) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;

pub(crate) enum Request {
	AuthorizeBatch {
		args: Vec<crate::authorize::Arg>,
		principal: tg::Principal,
	},
	ContainsIds {
		ids: Vec<tg::Id>,
	},
	FdbLogCompactionBatch {
		batch_size: usize,
		partition_end: u64,
		partition_start: u64,
	},
	GetRequesterPrincipals {
		principal: tg::Principal,
	},
	GetRunnerSandboxes {
		runner: tg::runner::Id,
	},
	GetSandboxProcesses {
		sandbox: tg::sandbox::Id,
	},
	GetTransactionId,
	ListSandboxes,
	ListSandboxesForCreator {
		creator: tg::Principal,
	},
	ListSandboxesForOwner {
		owner: tg::Principal,
	},
	LmdbLogCompactionBatch {
		batch_size: usize,
	},
	ProcessHasAncestor {
		ancestor: tg::process::Id,
		process: tg::process::Id,
	},
	TryGetAncestors {
		id: tg::Id,
	},
	TryGetCacheEntries {
		ids: Vec<tg::artifact::Id>,
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
	TryGetObjects {
		ids: Vec<tg::object::Id>,
	},
	TryGetOldestLogCompactionTransactionId,
	TryGetOldestUpdateTransactionId {
		kind: crate::update::Kind,
	},
	TryGetOrganizations {
		ids: Vec<tg::organization::Id>,
	},
	TryGetProcesses {
		ids: Vec<tg::process::Id>,
	},
	TryGetSandboxes {
		ids: Vec<tg::sandbox::Id>,
	},
	TryGetSpecifiersForIds {
		ids: Vec<tg::Id>,
	},
	TryGetUsers {
		ids: Vec<tg::user::Id>,
	},
	Visible {
		ids: Vec<tg::Id>,
		principal: tg::Principal,
	},
}

pub(crate) enum Response {
	AuthorizeBatch(Vec<Option<crate::authorize::Output>>),
	ContainsIds(Vec<bool>),
	LogCompactionBatch(Vec<crate::log::Entry>),
	GetRequesterPrincipals(Vec<tg::grant::Principal>),
	GetRunnerSandboxes(Vec<tg::sandbox::Id>),
	GetSandboxProcesses(Vec<(tg::process::Id, crate::process::Process)>),
	GetTransactionId(u64),
	ListSandboxes(Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>),
	ProcessHasAncestor(bool),
	TryGetAncestors(Option<Vec<tg::Id>>),
	TryGetCacheEntries(Vec<Option<crate::cache::Entry>>),
	TryGetCachedProcesses(Vec<(tg::process::Id, crate::process::Process)>),
	TryGetGroups(Vec<Option<crate::group::Group>>),
	TryGetIdsForSpecifiers(Vec<Option<tg::Id>>),
	TryGetObjects(Vec<Option<crate::object::Object>>),
	TryGetOldestLogCompactionTransactionId(Option<u64>),
	TryGetOldestUpdateTransactionId(Option<u64>),
	TryGetOrganizations(Vec<Option<crate::organization::Organization>>),
	TryGetProcesses(Vec<Option<crate::process::Process>>),
	TryGetSandboxes(Vec<Option<crate::sandbox::Sandbox>>),
	TryGetSpecifiersForIds(Vec<Option<tg::Specifier>>),
	TryGetUsers(Vec<Option<crate::user::User>>),
	Visible(Vec<bool>),
}
