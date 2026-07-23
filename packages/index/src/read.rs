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
	FinalizationBatch {
		batch_size: usize,
		kind: crate::finalization::Kind,
		partition_end: u64,
		partition_start: u64,
	},
	GetProcessDepthDetections {
		limit: usize,
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
	GetSchedulerRunners {
		scheduler: tg::scheduler::Id,
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
	TryGetCacheEntries {
		ids: Vec<tg::artifact::Id>,
	},
	TryGetCachedProcesses {
		command: tg::object::Id,
	},
	TryGetObjects {
		ids: Vec<tg::object::Id>,
	},
	TryGetOldestFinalizationTransactionId {
		kind: crate::finalization::Kind,
	},
	TryGetOldestUpdateTransactionId,
	TryGetProcesses {
		ids: Vec<tg::process::Id>,
	},
	TryGetRunners {
		ids: Vec<tg::runner::Id>,
	},
	TryGetSandboxes {
		ids: Vec<tg::sandbox::Id>,
	},
	Visible {
		ids: Vec<tg::Id>,
		principal: tg::Principal,
	},
}

pub(crate) enum Response {
	AuthorizeBatch(Vec<Option<crate::authorize::Output>>),
	FinalizationBatch(Vec<crate::finalization::Entry>),
	GetProcessDepthDetections(Vec<tg::process::Id>),
	GetRequesterPrincipals(Vec<tg::grant::Principal>),
	GetRunnerSandboxes(Vec<tg::sandbox::Id>),
	GetSandboxProcesses(Vec<(tg::process::Id, crate::process::Process)>),
	GetSchedulerRunners(Vec<tg::runner::Id>),
	GetTransactionId(u64),
	ListSandboxes(Vec<(tg::sandbox::Id, crate::sandbox::Sandbox)>),
	ProcessHasAncestor(bool),
	TryGetCacheEntries(Vec<Option<crate::cache::Entry>>),
	TryGetCachedProcesses(Vec<(tg::process::Id, crate::process::Process)>),
	TryGetObjects(Vec<Option<crate::object::Object>>),
	TryGetOldestFinalizationTransactionId(Option<u64>),
	TryGetOldestUpdateTransactionId(Option<u64>),
	TryGetProcesses(Vec<Option<crate::process::Process>>),
	TryGetRunners(Vec<Option<crate::runner::Runner>>),
	TryGetSandboxes(Vec<Option<crate::sandbox::Sandbox>>),
	Visible(Vec<bool>),
}
