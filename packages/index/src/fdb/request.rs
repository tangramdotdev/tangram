use tangram_client::prelude::*;

#[derive(Clone, Copy)]
pub(super) enum Priority {
	High,
	Low,
	Medium,
}

#[derive(Clone)]
pub(super) enum Request {
	Batch(crate::batch::Arg),
	Clean(Clean),
	CleanUsage(crate::usage::clean::Arg),
	CompactUsage(crate::usage::compact::Arg),
	CompleteLogCompaction(crate::log::Entry),
	DeleteGrants(Vec<crate::grant::delete::Arg>),
	DeleteGroupMembers(Vec<crate::group::member::delete::Arg>),
	DeleteGroups(Vec<tg::group::Id>),
	DeleteOrganizationMembers(Vec<crate::organization::member::delete::Arg>),
	DeleteOrganizations(Vec<tg::organization::Id>),
	DeleteSandboxes(Vec<tg::sandbox::Id>),
	DeleteTags(Vec<tg::tag::Id>),
	DeleteUsers(Vec<tg::user::Id>),
	EnqueueLogCompaction(tg::process::Id),
	GetUsage {
		account: crate::usage::Account,
		now: jiff::Timestamp,
		period: crate::usage::Period,
	},
	PutCheckouts(Vec<crate::checkout::put::Arg>),
	PutGrants(Vec<crate::grant::put::Arg>),
	PutGroupMembers(Vec<crate::group::member::put::Arg>),
	PutGroups(Vec<crate::group::put::Arg>),
	PutObjects(Vec<crate::object::put::Arg>),
	PutOrganizationMembers(Vec<crate::organization::member::put::Arg>),
	PutOrganizations(Vec<crate::organization::put::Arg>),
	PutProcesses(Vec<crate::process::put::Arg>),
	PutSandboxes(Vec<crate::sandbox::put::Arg>),
	PutTags(Vec<crate::tag::put::Arg>),
	PutUsers(Vec<crate::user::put::Arg>),
	TouchCheckouts(TouchCheckouts),
	TouchObjects(TouchObjects),
	TouchProcesses(TouchProcesses),
	Update(Update),
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
	pub ids: Vec<tg::artifact::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchObjects {
	pub account: Option<crate::usage::Account>,
	pub ids: Vec<tg::object::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchProcesses {
	pub account: Option<crate::usage::Account>,
	pub ids: Vec<tg::process::Id>,
	pub put_account: bool,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct Update {
	pub batch_size: usize,
	pub kind: crate::update::Kind,
	pub partition_end: u64,
	pub partition_start: u64,
}

pub(super) enum Item {
	Clean,
	CleanUsage,
	CompactUsage,
	CompleteLogCompaction(crate::log::Entry),
	DeleteGrant(crate::grant::delete::Arg),
	DeleteGroup(tg::group::Id),
	DeleteGroupMember(crate::group::member::delete::Arg),
	DeleteOrganization(tg::organization::Id),
	DeleteOrganizationMember(crate::organization::member::delete::Arg),
	DeleteSandbox(tg::sandbox::Id),
	DeleteTag(tg::tag::Id),
	DeleteUser(tg::user::Id),
	EnqueueLogCompaction(tg::process::Id),
	GetUsage,
	PutCheckout(crate::checkout::put::Arg),
	PutGrant(crate::grant::put::Arg),
	PutGroup(crate::group::put::Arg),
	PutGroupMember(crate::group::member::put::Arg),
	PutObject(crate::object::put::Arg),
	PutOrganization(crate::organization::put::Arg),
	PutOrganizationMember(crate::organization::member::put::Arg),
	PutProcess(crate::process::put::Arg),
	PutSandbox(crate::sandbox::put::Arg),
	PutTag(crate::tag::put::Arg),
	PutUser(crate::user::put::Arg),
	TouchCheckout(tg::artifact::Id),
	TouchObject(tg::object::Id),
	TouchProcess(tg::process::Id),
	Update,
}

pub(super) enum Kind {
	Clean {
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		max_sandbox_touched_at: i64,
		now: i64,
		partition_end: u64,
		partition_start: u64,
	},
	CleanUsage(crate::usage::clean::Arg),
	CompactUsage(crate::usage::compact::Arg),
	CompleteLogCompaction,
	DeleteGrants,
	DeleteGroupMembers,
	DeleteGroups,
	DeleteOrganizationMembers,
	DeleteOrganizations,
	DeleteSandboxes,
	DeleteTags,
	DeleteUsers,
	EnqueueLogCompaction,
	GetUsage {
		account: crate::usage::Account,
		now: jiff::Timestamp,
		period: crate::usage::Period,
	},
	PutCheckouts,
	PutGrants,
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
		account: Option<crate::usage::Account>,
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	TouchProcesses {
		account: Option<crate::usage::Account>,
		put_account: bool,
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	Update {
		kind: crate::update::Kind,
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
			| Self::PutGrants(_)
			| Self::PutGroupMembers(_)
			| Self::PutGroups(_)
			| Self::PutObjects(_)
			| Self::PutOrganizationMembers(_)
			| Self::PutOrganizations(_)
			| Self::PutProcesses(_)
			| Self::PutSandboxes(_)
			| Self::PutUsers(_) => Priority::Medium,
			Self::Clean(_) | Self::CleanUsage(_) | Self::CompactUsage(_) | Self::Update(_) => {
				Priority::Low
			},
			Self::DeleteGrants(_)
			| Self::DeleteGroupMembers(_)
			| Self::DeleteGroups(_)
			| Self::DeleteOrganizationMembers(_)
			| Self::DeleteOrganizations(_)
			| Self::DeleteSandboxes(_)
			| Self::DeleteTags(_)
			| Self::DeleteUsers(_)
			| Self::GetUsage { .. }
			| Self::PutTags(_)
			| Self::TouchCheckouts(_)
			| Self::TouchObjects(_)
			| Self::TouchProcesses(_) => Priority::High,
		}
	}
}
