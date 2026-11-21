use {
	super::{Borrowable, Candidate, Placement, RunnerRef, choose_runner},
	crate::{
		SchedulerState,
		runner::Runner,
		sandbox::{Sandbox, State},
	},
	std::collections::{HashMap, HashSet},
	tangram_client::prelude::*,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

fn candidate(id: &tg::runner::Id, score: u128, siblings: usize) -> Candidate<'_> {
	Candidate {
		id,
		connection: 0,
		score: (score, score),
		siblings,
	}
}

/// Create runner IDs in ascending order.
fn runners() -> [tg::runner::Id; 3] {
	let mut ids = [
		tg::runner::Id::new(),
		tg::runner::Id::new(),
		tg::runner::Id::new(),
	];
	ids.sort();
	ids
}

#[test]
fn the_parents_runner_comes_first() {
	let [a, b, c] = runners();
	let candidates = [
		candidate(&a, 1, 0),
		candidate(&b, 2, 3),
		candidate(&c, 3, 0),
	];
	let chosen = choose_runner(&candidates, Some(&c)).unwrap();
	assert_eq!(chosen.id, &c);
}

#[test]
fn a_parents_runner_without_room_falls_through_to_the_siblings() {
	let [a, b, c] = runners();
	let candidates = [candidate(&a, 1, 0), candidate(&b, 2, 3)];
	let chosen = choose_runner(&candidates, Some(&c)).unwrap();
	assert_eq!(chosen.id, &b);
}

#[test]
fn the_runner_with_the_most_siblings_comes_next_and_ties_go_to_the_better_score_and_then_the_id() {
	let [a, b, c] = runners();

	// The most siblings wins over a better score.
	let candidates = [
		candidate(&a, 1, 1),
		candidate(&b, 5, 2),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);

	// Equal siblings go to the better score.
	let candidates = [
		candidate(&a, 4, 2),
		candidate(&b, 3, 2),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);

	// Equal siblings and scores go to the lower ID.
	let candidates = [candidate(&b, 3, 2), candidate(&a, 3, 2)];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &a);
}

#[test]
fn a_sandbox_without_a_parent_or_siblings_goes_to_the_best_score() {
	let [a, b, c] = runners();
	let candidates = [
		candidate(&a, 3, 0),
		candidate(&b, 1, 0),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);
	assert!(choose_runner(&[], None).is_none());
}

/// Create a runner that is ready and has room for one sandbox.
fn runner() -> Runner {
	let capacity = tg::runner::Capacity {
		sandboxes: 1,
		..Default::default()
	};
	Runner {
		borrowable: HashSet::default(),
		capacity: tg::runner::control::Capacity {
			available: capacity,
			cpu_limit: 1,
			overcommit: tg::runner::Overcommit::default(),
			total: capacity,
		},
		committed: tg::runner::Capacity::default(),
		connection: 0,
		connection_failed: false,
		heartbeat_at: tangram_server_core::clock::Clock::new(None, None).instant(),
		heartbeat_index: 1,
		host: String::new(),
		owner: None,
		reservations: HashMap::default(),
		reserved: tg::runner::Capacity::default(),
	}
}

/// Create the entry of a queued sandbox that needs one sandbox of capacity.
fn sandbox(id: &tg::sandbox::Id) -> Sandbox {
	let request = EnqueueSandboxRequestArg {
		arg: tg::sandbox::create::Arg::default(),
		boot: 0,
		created_at: 0,
		creator: tg::Principal::default(),
		owner: tg::Principal::default(),
		owner_ancestors: Vec::new(),
		parent: None,
		process: None,
		public: false,
		runner: None,
		sandbox: id.clone(),
		scheduler: None,
		token: None,
	};
	let mut sandbox = Sandbox::new(0, None, Some(request), State::Queued);
	sandbox.capacity = tg::runner::Capacity {
		sandboxes: 1,
		..Default::default()
	};
	sandbox
}

/// Set the capacity that a parent lends on a runner.
fn lend(
	state: &mut SchedulerState,
	parent: &tg::sandbox::Id,
	runner: &tg::runner::Id,
	available: bool,
	connection: u64,
) {
	let borrowable = Borrowable {
		available,
		capacity: tg::runner::Capacity {
			sandboxes: 1,
			..Default::default()
		},
		heartbeat_index: 1,
		connection: Some(connection),
		runner: runner.clone(),
	};
	state.parents.0.get_mut(parent).unwrap().borrowable = Some(borrowable);
}

#[test]
fn a_borrowed_placement_chooses_a_parent_whose_capacity_is_available() {
	let mut state = SchedulerState::new();
	let id = tg::sandbox::Id::new();
	let [lent, unavailable, unlent] = std::array::from_fn(|_| tg::sandbox::Id::new());
	let runner_id = tg::runner::Id::new();
	state.runners.entries.insert(runner_id.clone(), runner());
	state.sandboxes.entries.insert(id.clone(), sandbox(&id));
	state.queue.push(&mut state.sandboxes.entries, &id);
	for parent in [&unavailable, &unlent, &lent] {
		state.add_parent(&id, parent);
	}

	// Skip the parent whose capacity is not available and the parent that lends none, and choose the parent whose capacity is available.
	lend(&mut state, &unavailable, &runner_id, false, 0);
	lend(&mut state, &lent, &runner_id, true, 0);
	let placement = state.borrowed_placement(&state.sandboxes.entries[&id], 1);
	let runner = RunnerRef {
		connection: 0,
		id: runner_id.clone(),
	};
	let expected = Placement::Borrowed {
		parent: lent.clone(),
		runner,
	};
	assert_eq!(placement, Some(expected));

	// Skip a parent whose capacity is on an earlier connection of its runner.
	lend(&mut state, &lent, &runner_id, true, 1);
	assert_eq!(
		state.borrowed_placement(&state.sandboxes.entries[&id], 1),
		None
	);

	// Skip a parent whose runner has no room for another offer.
	lend(&mut state, &lent, &runner_id, true, 0);
	assert_eq!(
		state.borrowed_placement(&state.sandboxes.entries[&id], 0),
		None
	);
}
