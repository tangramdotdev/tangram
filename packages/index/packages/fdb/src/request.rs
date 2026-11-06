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
	Write(tangram_index::write::Arg),
	Clean(Clean),
	ExpireUsage(tangram_index::usage::expire::Arg),
	CompletePermissionCapture(tangram_index::permission::capture::Entry),
	DeleteIndexer(tangram_index::indexer::delete::Arg),
	GetUsage {
		account: tangram_index::usage::Account,
		delta_time_to_live: std::time::Duration,
		now: jiff::Timestamp,
		period: tangram_index::usage::Period,
	},
	NextRunnerRegistrationIndex(tg::runner::Id),
	PutIndexer(tangram_index::indexer::put::Arg),
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
	pub data: bool,
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

pub(super) enum Operation {
	AggregateUsage,
	Write(tangram_index::Item),
	Clean,
	ExpireUsage,
	GetUsage,
	TouchCheckout(tg::Id),
	TouchObject(tg::object::Id),
	TouchProcess(tg::process::Id),
	Update,
}

pub(super) enum Kind {
	AggregateUsage(tangram_index::usage::aggregate::Arg),
	Write,
	Clean {
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		max_sandbox_touched_at: i64,
		now: i64,
		partition_end: u64,
		partition_start: u64,
	},
	ExpireUsage(tangram_index::usage::expire::Arg),
	GetUsage {
		account: tangram_index::usage::Account,
		delta_time_to_live: std::time::Duration,
		now: jiff::Timestamp,
		period: tangram_index::usage::Period,
	},
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
		data: bool,
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
			Self::Write(arg) => {
				// Apply deletes and tag puts ahead of other writes, because readers wait for them.
				let urgent = arg.items.iter().all(|item| {
					matches!(
						item,
						tangram_index::Item::DeleteGroup(_)
							| tangram_index::Item::DeleteGroupMember(_)
							| tangram_index::Item::DeleteOrganization(_)
							| tangram_index::Item::DeleteOrganizationMember(_)
							| tangram_index::Item::DeletePermission(_)
							| tangram_index::Item::DeleteSandbox(_)
							| tangram_index::Item::DeleteTag(_)
							| tangram_index::Item::DeleteUser(_)
							| tangram_index::Item::PutTag(_)
					)
				});
				if urgent {
					Priority::High
				} else {
					Priority::Medium
				}
			},
			Self::AggregateUsage(_)
			| Self::Clean(_)
			| Self::CompletePermissionCapture(_)
			| Self::ExpireUsage(_)
			| Self::Update(_) => Priority::Low,
			Self::DeleteIndexer(_)
			| Self::GetUsage { .. }
			| Self::NextRunnerRegistrationIndex(_)
			| Self::PutIndexer(_)
			| Self::TouchCheckouts(_)
			| Self::TouchObjects(_)
			| Self::TouchProcesses(_)
			| Self::UpdateIndexer(_) => Priority::High,
		}
	}
}
