use tangram_client::prelude::*;

#[derive(Clone, Copy)]
pub(super) enum Priority {
	High,
	Low,
	Medium,
}

#[derive(Clone)]
pub(super) enum Request {
	AggregateUsage(tangram_index::usage::aggregate::Arg),
	Batch(tangram_index::batch::Arg),
	Clean(Clean),
	ExpireUsage(tangram_index::usage::expire::Arg),
	CompleteLogCompaction(tangram_index::log::Entry),
	CompletePermissionCapture(tangram_index::permission::capture::Entry),
	DeletePermissions(Vec<tangram_index::permission::delete::Arg>),
	DeleteGroupMembers(Vec<tangram_index::group::member::delete::Arg>),
	DeleteGroups(Vec<tg::group::Id>),
	DeleteIndexer(tangram_index::indexer::delete::Arg),
	DeleteOrganizationMembers(Vec<tangram_index::organization::member::delete::Arg>),
	DeleteOrganizations(Vec<tg::organization::Id>),
	DeleteSandboxes(Vec<tg::sandbox::Id>),
	DeleteTags(Vec<tg::tag::Id>),
	DeleteUsers(Vec<tg::user::Id>),
	EnqueueLogCompaction(tg::process::Id),
	GetUsage {
		account: tangram_index::usage::Account,
		now: jiff::Timestamp,
		period: tangram_index::usage::Period,
	},
	PutCheckouts(Vec<tangram_index::checkout::put::Arg>),
	PutPermissions(Vec<tangram_index::permission::put::Arg>),
	PutGroupMembers(Vec<tangram_index::group::member::put::Arg>),
	PutGroups(Vec<tangram_index::group::put::Arg>),
	PutIndexer(tangram_index::indexer::put::Arg),
	PutObjects(Vec<tangram_index::object::put::Arg>),
	PutOrganizationMembers(Vec<tangram_index::organization::member::put::Arg>),
	PutOrganizations(Vec<tangram_index::organization::put::Arg>),
	PutProcesses(Vec<tangram_index::process::put::Arg>),
	PutSandboxes(Vec<tangram_index::sandbox::put::Arg>),
	PutTags(Vec<tangram_index::tag::put::Arg>),
	PutUsers(Vec<tangram_index::user::put::Arg>),
	TouchCheckouts(TouchCheckouts),
	TouchObjects(TouchObjects),
	TouchProcesses(TouchProcesses),
	Update(Update),
	UpdateIndexer(tangram_index::indexer::update::Arg),
}

#[derive(Clone)]
pub(super) struct Clean {
	pub batch_size: usize,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
	pub max_sandbox_touched_at: i64,
	pub now: i64,
	pub partition_end: u64,
	pub partition_start: u64,
}

#[derive(Clone)]
pub(super) struct TouchCheckouts {
	pub ids: Vec<tg::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchObjects {
	pub account: Option<tangram_index::usage::Account>,
	pub ids: Vec<tg::object::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchProcesses {
	pub account: Option<tangram_index::usage::Account>,
	pub ids: Vec<tg::process::Id>,
	pub put_account: bool,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct Update {
	pub batch_size: usize,
	pub kind: tangram_index::update::Kind,
	pub partition_end: u64,
	pub partition_start: u64,
}

pub(super) enum Item {
	AggregateUsage,
	Clean,
	ExpireUsage,
	CompleteLogCompaction(tangram_index::log::Entry),
	DeletePermission(tangram_index::permission::delete::Arg),
	DeleteGroup(tg::group::Id),
	DeleteGroupMember(tangram_index::group::member::delete::Arg),
	DeleteOrganization(tg::organization::Id),
	DeleteOrganizationMember(tangram_index::organization::member::delete::Arg),
	DeleteSandbox(tg::sandbox::Id),
	DeleteTag(tg::tag::Id),
	DeleteUser(tg::user::Id),
	EnqueueLogCompaction(tg::process::Id),
	GetUsage,
	PutCheckout(tangram_index::checkout::put::Arg),
	PutPermission(tangram_index::permission::put::Arg),
	PutGroup(tangram_index::group::put::Arg),
	PutGroupMember(tangram_index::group::member::put::Arg),
	PutObject(tangram_index::object::put::Arg),
	PutOrganization(tangram_index::organization::put::Arg),
	PutOrganizationMember(tangram_index::organization::member::put::Arg),
	PutProcess(tangram_index::process::put::Arg),
	PutSandbox(tangram_index::sandbox::put::Arg),
	PutTag(tangram_index::tag::put::Arg),
	PutUser(tangram_index::user::put::Arg),
	TouchCheckout(tg::Id),
	TouchObject(tg::object::Id),
	TouchProcess(tg::process::Id),
	Update,
}

pub(super) enum Kind {
	AggregateUsage(tangram_index::usage::aggregate::Arg),
	Clean {
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		max_sandbox_touched_at: i64,
		now: i64,
		partition_end: u64,
		partition_start: u64,
	},
	ExpireUsage(tangram_index::usage::expire::Arg),
	CompleteLogCompaction,
	DeletePermissions,
	DeleteGroupMembers,
	DeleteGroups,
	DeleteOrganizationMembers,
	DeleteOrganizations,
	DeleteSandboxes,
	DeleteTags,
	DeleteUsers,
	EnqueueLogCompaction,
	GetUsage {
		account: tangram_index::usage::Account,
		now: jiff::Timestamp,
		period: tangram_index::usage::Period,
	},
	PutCheckouts,
	PutPermissions,
	PutGroupMembers,
	PutGroups,
	PutObjects,
	PutOrganizationMembers,
	PutOrganizations,
	PutProcesses,
	PutSandboxes,
	PutTags,
	PutUsers,
	TouchCheckouts {
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	TouchObjects {
		account: Option<tangram_index::usage::Account>,
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	TouchProcesses {
		account: Option<tangram_index::usage::Account>,
		put_account: bool,
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	Update {
		kind: tangram_index::update::Kind,
		partition_end: u64,
		partition_start: u64,
	},
}

impl Request {
	#[must_use]
	pub(super) fn priority(&self) -> Priority {
		match self {
			Self::Batch(_)
			| Self::CompleteLogCompaction(_)
			| Self::EnqueueLogCompaction(_)
			| Self::PutCheckouts(_)
			| Self::PutPermissions(_)
			| Self::PutGroupMembers(_)
			| Self::PutGroups(_)
			| Self::PutObjects(_)
			| Self::PutOrganizationMembers(_)
			| Self::PutOrganizations(_)
			| Self::PutProcesses(_)
			| Self::PutSandboxes(_)
			| Self::PutUsers(_) => Priority::Medium,
			Self::AggregateUsage(_)
			| Self::Clean(_)
			| Self::CompletePermissionCapture(_)
			| Self::ExpireUsage(_)
			| Self::Update(_) => Priority::Low,
			Self::DeletePermissions(_)
			| Self::DeleteGroupMembers(_)
			| Self::DeleteGroups(_)
			| Self::DeleteIndexer(_)
			| Self::DeleteOrganizationMembers(_)
			| Self::DeleteOrganizations(_)
			| Self::DeleteSandboxes(_)
			| Self::DeleteTags(_)
			| Self::DeleteUsers(_)
			| Self::GetUsage { .. }
			| Self::PutIndexer(_)
			| Self::PutTags(_)
			| Self::TouchCheckouts(_)
			| Self::TouchObjects(_)
			| Self::TouchProcesses(_)
			| Self::UpdateIndexer(_) => Priority::High,
		}
	}
}
