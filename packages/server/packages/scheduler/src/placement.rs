use {
	super::{
		BorrowableCapacityNotification, Scheduler, SchedulerState,
		queue::List,
		runner::Runner,
		sandbox::{Sandbox, State},
	},
	std::collections::{BTreeMap, HashMap},
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

pub(super) struct Parents(pub(super) HashMap<tg::sandbox::Id, Parent, tg::id::BuildHasher>);

pub(super) struct Parent {
	pub(super) borrowable: Option<Borrowable>,

	/// The runner of each child that the scheduler offered to a runner or that a runner accepted, so that the scheduler prefers the runner with the most of a parent's children for its next child.
	pub(super) children: BTreeMap<tg::sandbox::Id, tg::runner::Id>,

	/// The parent's children that the scheduler holds, in the order that they were enqueued.
	pub(super) queue: List,
}

pub(super) struct Borrowable {
	pub(super) available: bool,
	pub(super) capacity: tg::runner::Capacity,
	pub(super) connection: Option<u64>,
	pub(super) heartbeat_index: u64,
	pub(super) runner: tg::runner::Id,
}

pub(super) struct Block {
	pub(super) connection: u64,
	pub(super) heartbeat_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Placement {
	Borrowed {
		parent: tg::sandbox::Id,
		runner: RunnerRef,
	},
	Regular {
		runner: RunnerRef,
	},
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RunnerRef {
	pub(super) connection: u64,
	pub(super) id: tg::runner::Id,
}

/// A runner that can take a sandbox, with its score and its number of the sandbox's siblings.
pub(super) struct Candidate<'a> {
	pub(super) connection: u64,
	pub(super) id: &'a tg::runner::Id,
	pub(super) score: (u128, u128),
	pub(super) siblings: usize,
}

impl Parents {
	pub(super) fn new() -> Self {
		Self(HashMap::default())
	}
}

impl Parent {
	/// Whether the entry has no queued children, no capacity to lend, and no children on runners, so that it can be dropped.
	pub(super) fn is_empty(&self) -> bool {
		self.queue.len == 0 && self.borrowable.is_none() && self.children.is_empty()
	}
}

impl Borrowable {
	#[must_use]
	pub(super) fn new(
		notification: BorrowableCapacityNotification,
		connection: Option<u64>,
		previous: Option<&Self>,
	) -> Self {
		// A pending borrower must not revoke an existing unconsumed reservation.
		let available = notification.available
			|| previous.is_some_and(|previous| {
				previous.available
					&& previous.capacity == notification.capacity
					&& previous.connection == connection
					&& previous.runner == notification.runner
			});
		let heartbeat_index = previous
			.filter(|previous| {
				previous.runner == notification.runner && previous.connection == connection
			})
			.map_or(notification.heartbeat_index, |previous| {
				previous.heartbeat_index.max(notification.heartbeat_index)
			});
		Self {
			available,
			capacity: notification.capacity,
			connection,
			heartbeat_index,
			runner: notification.runner,
		}
	}
}

impl Placement {
	pub(super) fn runner(&self) -> &RunnerRef {
		match self {
			Self::Borrowed { runner, .. } | Self::Regular { runner } => runner,
		}
	}

	/// Point the placement at a new connection of its runner.
	pub(super) fn set_connection(&mut self, connection: u64) {
		match self {
			Self::Borrowed { runner, .. } | Self::Regular { runner } => {
				runner.connection = connection;
			},
		}
	}
}

impl SchedulerState {
	/// Record the runner of a parent's child.
	pub(super) fn insert_child(
		&mut self,
		parent: &tg::sandbox::Id,
		child: &tg::sandbox::Id,
		runner: &tg::runner::Id,
	) {
		self.parents
			.0
			.entry(parent.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				children: BTreeMap::new(),
				queue: List::default(),
			})
			.children
			.insert(child.clone(), runner.clone());
	}

	/// Forget the runner of a parent's child, and the parent's entry if it is empty.
	pub(super) fn remove_child(&mut self, parent: &tg::sandbox::Id, child: &tg::sandbox::Id) {
		let Some(entry) = self.parents.0.get_mut(parent) else {
			return;
		};
		entry.children.remove(child);
		if entry.is_empty() {
			self.parents.0.remove(parent);
		}
	}

	pub(super) fn handle_borrowable_capacity(
		&mut self,
		scheduler: &Scheduler,
		notification: BorrowableCapacityNotification,
	) {
		let connection = self
			.runners
			.entries
			.get(&notification.runner)
			.map(|runner| runner.connection);
		let runner = notification.runner.clone();
		let parent_id = notification.parent.clone();
		let parent = self
			.parents
			.0
			.entry(parent_id.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				children: BTreeMap::new(),
				queue: List::default(),
			});
		let borrowable = Borrowable::new(notification, connection, parent.borrowable.as_ref());
		let previous = parent.borrowable.take();
		if borrowable.capacity.sandboxes != 0 {
			parent.borrowable = Some(borrowable);
		}
		if let Some(previous) = previous
			&& let Some(runner) = self.runners.entries.get_mut(&previous.runner)
		{
			runner.borrowable.remove(&parent_id);
		}
		if parent.borrowable.is_some()
			&& let Some(runner) = self.runners.entries.get_mut(&runner)
		{
			runner.borrowable.insert(parent_id.clone());
		}
		if parent.is_empty() {
			self.parents.0.remove(&parent_id);
		}
		let sandbox = self.first_queued_child(&parent_id);
		if let Some(sandbox) = sandbox
			&& self.sandboxes.offers < scheduler.config.max_offers
		{
			self.try_offer_boot(scheduler, &sandbox);
		}
		self.queue.wake();
	}

	pub(super) fn find_placement(
		&self,
		scheduler: &Scheduler,
		sandbox: &Sandbox,
	) -> Option<Placement> {
		if let Some(placement) =
			self.borrowed_placement(sandbox, scheduler.config.max_offers_per_runner)
		{
			return Some(placement);
		}

		// Find the runners that can take the sandbox, with their scores and their numbers of the sandbox's siblings.
		let parent = sandbox
			.parent
			.as_ref()
			.and_then(|parent| self.parents.0.get(parent));
		let mut candidates = Vec::new();
		for (id, runner) in &self.runners.entries {
			let blocked = sandbox.blocked.get(id).is_some_and(|block| {
				block.connection == runner.connection
					&& block.heartbeat_index == runner.heartbeat_index
			});
			if !eligible(id, runner, sandbox)
				|| blocked
				|| runner.reservations.len() >= scheduler.config.max_offers_per_runner
				|| !runner.fits(sandbox.capacity)
			{
				continue;
			}
			let score = score(runner, runner.available(), sandbox.capacity);
			let siblings = parent.map_or(0, |parent| {
				parent
					.children
					.values()
					.filter(|runner| *runner == id)
					.count()
			});
			let candidate = Candidate {
				connection: runner.connection,
				id,
				score,
				siblings,
			};
			candidates.push(candidate);
		}

		// Prefer the runner that runs the sandbox's parent, which the scheduler knows from the parent's boot, and then the runner that runs the most of its siblings.
		let parent_runner = sandbox
			.parent
			.as_ref()
			.and_then(|parent| self.sandboxes.entries.get(parent))
			.and_then(|parent| match &parent.state {
				State::Accepted { runner } => Some(runner),
				State::Offered { .. } | State::Queued | State::Terminal { .. } => None,
			})
			.filter(|runner| {
				self.runners
					.entries
					.get(&runner.id)
					.is_some_and(|entry| entry.connection == runner.connection)
			})
			.map(|runner| &runner.id);
		let candidate = choose_runner(&candidates, parent_runner)?;
		let runner = RunnerRef {
			connection: candidate.connection,
			id: candidate.id.clone(),
		};

		Some(Placement::Regular { runner })
	}

	pub(super) fn borrowed_placement(
		&self,
		sandbox: &Sandbox,
		request_limit: usize,
	) -> Option<Placement> {
		sandbox.parents.iter().find_map(|links| {
			let (borrowable, runner) = self.borrowable(sandbox, &links.parent)?;
			if !borrowable.available || runner.reservations.len() >= request_limit {
				return None;
			}
			let runner = RunnerRef {
				connection: runner.connection,
				id: borrowable.runner.clone(),
			};
			let placement = Placement::Borrowed {
				parent: links.parent.clone(),
				runner,
			};
			Some(placement)
		})
	}

	/// Find the capacity that one of a sandbox's parents lends on a runner that can take the sandbox, where the runner also has the capacity that the sandbox must acquire in addition to what it borrows.
	pub(super) fn borrowable(
		&self,
		sandbox: &Sandbox,
		parent: &tg::sandbox::Id,
	) -> Option<(&Borrowable, &Runner)> {
		let borrowable = self.parents.0.get(parent)?.borrowable.as_ref()?;
		let runner = self.runners.entries.get(&borrowable.runner)?;
		let (_, acquired) = borrowable.capacity.try_borrow(sandbox.capacity)?;
		if borrowable
			.connection
			.is_some_and(|connection| connection != runner.connection)
			|| !eligible(&borrowable.runner, runner, sandbox)
			|| !runner.fits(acquired)
		{
			return None;
		}
		Some((borrowable, runner))
	}

	/// Whether a runner could take a sandbox once it has the capacity.
	pub(super) fn placeable(&self, sandbox: &Sandbox) -> bool {
		self.runners
			.entries
			.iter()
			.any(|(id, runner)| eligible(id, runner, sandbox))
	}
}

/// Whether a runner could take a sandbox once it has the capacity: the runner is ready, its owner is the sandbox's owner or one of its ancestors, it has the sandbox's host, it is the runner that the sandbox must start on, and the sandbox is within its total capacity and its CPU limit.
pub(super) fn eligible(id: &tg::runner::Id, runner: &Runner, sandbox: &Sandbox) -> bool {
	let request = sandbox.request();
	runner.ready()
		&& runner
			.owner
			.as_ref()
			.is_none_or(|owner| request.owner_ancestors.contains(owner))
		&& request
			.arg
			.host
			.as_ref()
			.is_none_or(|host| host == &runner.host)
		&& request.runner.as_ref().is_none_or(|runner| runner == id)
		&& runner
			.capacity
			.total
			.contains(sandbox.capacity, runner.capacity.overcommit)
		&& sandbox
			.capacity
			.cpu
			.max
			.is_none_or(|max| max.threads() <= runner.capacity.cpu_limit)
}

pub(super) fn score(
	runner: &Runner,
	available: tg::runner::Capacity,
	requested: tg::runner::Capacity,
) -> (u128, u128) {
	let remaining = available.subtract(requested, runner.capacity.overcommit);
	let total = runner.capacity.total;
	let cpu = u128::from(remaining.cpu.max.map_or(0, tg::sandbox::Amount::millis)) * 1_000_000
		/ u128::from(total.cpu.max.map_or(1, tg::sandbox::Amount::millis).max(1));
	let memory = u128::from(remaining.memory.max.unwrap_or_default()) * 1_000_000
		/ u128::from(total.memory.max.unwrap_or(1).max(1));
	(cpu.max(memory), cpu + memory)
}

/// Choose a runner for a sandbox among the runners that can take it: the runner of the sandbox's parent, then the runner with the most of its siblings, and then the runner with the best score, breaking ties by score and then by runner ID.
pub(super) fn choose_runner<'a>(
	candidates: &'a [Candidate<'a>],
	parent_runner: Option<&tg::runner::Id>,
) -> Option<&'a Candidate<'a>> {
	if let Some(parent_runner) = parent_runner
		&& let Some(candidate) = candidates
			.iter()
			.find(|candidate| candidate.id == parent_runner)
	{
		return Some(candidate);
	}
	let best = |a: &&Candidate, b: &&Candidate| (a.score, a.id).cmp(&(b.score, b.id));
	if let Some(candidate) = candidates
		.iter()
		.filter(|candidate| candidate.siblings > 0)
		.min_by(|a, b| b.siblings.cmp(&a.siblings).then_with(|| best(a, b)))
	{
		return Some(candidate);
	}
	candidates.iter().min_by(best)
}
