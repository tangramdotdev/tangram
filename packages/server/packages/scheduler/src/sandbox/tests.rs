use {
	super::{Sandbox, State, Tracked},
	crate::{
		AddSandboxParentRequestArg, SchedulerState,
		placement::{Placement, RunnerRef},
	},
	tangram_client::prelude::*,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

/// Create the entry of a sandbox that needs one sandbox of capacity.
fn sandbox(id: &tg::sandbox::Id, state: State) -> Sandbox {
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
	let mut sandbox = Sandbox::new(0, None, Some(request), state);
	sandbox.capacity = tg::runner::Capacity {
		sandboxes: 1,
		..Default::default()
	};
	sandbox
}

/// Create a scheduler state that holds a sandbox in a state.
fn state(state: State) -> (SchedulerState, tg::sandbox::Id) {
	let mut scheduler = SchedulerState::new();
	let id = tg::sandbox::Id::new();
	scheduler
		.sandboxes
		.entries
		.insert(id.clone(), sandbox(&id, state));
	if matches!(
		scheduler.sandboxes.entries[&id].state,
		State::Offered { .. } | State::Queued
	) {
		scheduler.queue.push(&mut scheduler.sandboxes.entries, &id);
	}
	(scheduler, id)
}

fn offered() -> State {
	let runner = RunnerRef {
		registration_index: 0,
		id: tg::runner::Id::new(),
	};
	State::Offered {
		connected: false,
		offer: String::new(),
		placement: Placement::Regular { runner },
		retries: 0,
	}
}

#[test]
fn a_parent_is_added_to_a_queued_or_offered_sandbox_once() {
	for held in [State::Queued, offered()] {
		let (mut scheduler, sandbox) = state(held);
		let parent = tg::sandbox::Id::new();
		let request = AddSandboxParentRequestArg {
			parent: parent.clone(),
			sandbox: sandbox.clone(),
		};

		// The scheduler adds the parent, and replies with the sandbox's capacity.
		let output = scheduler.add_sandbox_parent_inner(&request);
		assert_eq!(output.capacity.map(|capacity| capacity.sandboxes), Some(1));
		assert_eq!(scheduler.parents.0[&parent].queue.len, 1);

		// Adding the parent again replies the same, and does not add it twice.
		let output = scheduler.add_sandbox_parent_inner(&request);
		assert_eq!(output.capacity.map(|capacity| capacity.sandboxes), Some(1));
		assert_eq!(scheduler.sandboxes.entries[&sandbox].parents.len(), 1);
		assert_eq!(scheduler.parents.0[&parent].queue.len, 1);
	}
}

#[test]
fn a_sandbox_that_the_scheduler_does_not_hold_has_no_capacity_to_borrow() {
	let parent = tg::sandbox::Id::new();
	let runner = RunnerRef {
		registration_index: 0,
		id: tg::runner::Id::new(),
	};
	let terminal = State::Terminal {
		error: None,
		indexed: false,
		status: tg::sandbox::Status::Destroyed,
	};
	for held in [State::Accepted { runner }, terminal] {
		let (mut scheduler, sandbox) = state(held);
		let request = AddSandboxParentRequestArg {
			parent: parent.clone(),
			sandbox: sandbox.clone(),
		};
		let output = scheduler.add_sandbox_parent_inner(&request);
		assert_eq!(output.capacity, None);
		assert!(scheduler.sandboxes.entries[&sandbox].parents.is_empty());
		assert!(scheduler.parents.0.is_empty());
	}

	// A sandbox that the scheduler does not track, or a sandbox that would be its own parent, has none either.
	let mut scheduler = SchedulerState::new();
	let request = AddSandboxParentRequestArg {
		parent: parent.clone(),
		sandbox: tg::sandbox::Id::new(),
	};
	assert_eq!(scheduler.add_sandbox_parent_inner(&request).capacity, None);
	let (mut scheduler, sandbox) = state(State::Queued);
	let request = AddSandboxParentRequestArg {
		parent: sandbox.clone(),
		sandbox: sandbox.clone(),
	};
	assert_eq!(scheduler.add_sandbox_parent_inner(&request).capacity, None);
	assert!(scheduler.parents.0.is_empty());
}

fn runner() -> RunnerRef {
	RunnerRef {
		registration_index: 1,
		id: tg::runner::Id::new(),
	}
}

fn accepted_runner(scheduler: &SchedulerState, sandbox: &tg::sandbox::Id) -> Option<RunnerRef> {
	match &scheduler.sandboxes.entries.get(sandbox)?.state {
		State::Accepted { runner } => Some(runner.clone()),
		State::Offered { .. } | State::Queued | State::Terminal { .. } => None,
	}
}

#[test]
fn a_reported_sandbox_that_the_scheduler_does_not_track_is_accepted_and_not_queued() {
	let mut scheduler = SchedulerState::new();
	let id = tg::sandbox::Id::new();
	let parent = tg::sandbox::Id::new();
	let runner = runner();
	let mut request = sandbox(&id, State::Queued).request.unwrap();
	request.parent = Some(parent.clone());

	// A restarted scheduler tracks the sandbox on the runner that claims it, so that it is not placed again.
	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, Some(request), runner.clone());
	assert!(matches!(tracked, Tracked::Inserted { ids } if ids.is_empty()));
	assert_eq!(accepted_runner(&scheduler, &id), Some(runner.clone()));
	assert_eq!(scheduler.queue.len(), 0);
	assert_eq!(scheduler.parents.0[&parent].children[&id], runner.id);

	// A report of the same boot without its request keeps the request and the parent.
	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, None, runner.clone());
	assert!(matches!(tracked, Tracked::Inserted { ids } if ids.is_empty()));
	let entry = &scheduler.sandboxes.entries[&id];
	assert!(entry.request.is_some());
	assert_eq!(entry.parent, Some(parent));
}

#[test]
fn a_reported_queued_sandbox_leaves_the_queue_for_its_runner() {
	let (mut scheduler, id) = state(State::Queued);
	let runner = runner();
	assert_eq!(scheduler.queue.len(), 1);

	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, None, runner.clone());
	assert!(matches!(tracked, Tracked::Accepted));
	assert_eq!(accepted_runner(&scheduler, &id), Some(runner));
	assert_eq!(scheduler.queue.len(), 0);
}

#[test]
fn a_report_leaves_an_offered_sandbox_and_a_later_boot_alone() {
	// An offered sandbox waits for the answer to its offer.
	let (mut scheduler, id) = state(offered());
	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, None, runner());
	assert!(matches!(tracked, Tracked::Ignored));
	assert!(matches!(
		scheduler.sandboxes.entries[&id].state,
		State::Offered { .. }
	));
	assert_eq!(scheduler.queue.len(), 1);

	// A report of an older boot does not replace a later boot, whether it is queued or accepted.
	for held in [State::Queued, State::Accepted { runner: runner() }] {
		let (mut scheduler, id) = state(held);
		scheduler.sandboxes.entries.get_mut(&id).unwrap().boot = 2;
		let reporter = runner();
		let tracked = scheduler.track_accepted_sandbox_inner(&id, 1, None, reporter.clone());
		assert!(matches!(tracked, Tracked::Ignored));
		assert_eq!(scheduler.sandboxes.entries[&id].boot, 2);
		assert_ne!(accepted_runner(&scheduler, &id), Some(reporter));
	}
}

#[test]
fn a_report_of_a_later_boot_replaces_an_accepted_boot() {
	let previous = runner();
	let (mut scheduler, id) = state(State::Accepted { runner: previous });
	let runner = runner();

	let tracked = scheduler.track_accepted_sandbox_inner(&id, 1, None, runner.clone());
	assert!(matches!(tracked, Tracked::Inserted { ids } if ids.is_empty()));
	let entry = &scheduler.sandboxes.entries[&id];
	assert_eq!(entry.boot, 1);
	assert!(entry.request.is_none());
	assert_eq!(accepted_runner(&scheduler, &id), Some(runner));
}

#[test]
fn a_report_of_a_sandbox_that_the_scheduler_ended_stops_or_destroys_it_on_its_runner() {
	// A boot that the scheduler ended as stopped is left to its runner.
	let stopped = State::Terminal {
		error: None,
		indexed: false,
		status: tg::sandbox::Status::Stopped,
	};
	let (mut scheduler, id) = state(stopped);
	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, None, runner());
	assert!(matches!(tracked, Tracked::Ignored));
	assert!(matches!(
		scheduler.sandboxes.entries[&id].state,
		State::Terminal { .. }
	));

	// A sandbox that the scheduler ended as destroyed is destroyed on the runner that reports it.
	let destroyed = State::Terminal {
		error: None,
		indexed: false,
		status: tg::sandbox::Status::Destroyed,
	};
	let (mut scheduler, id) = state(destroyed);
	let runner = runner();
	let tracked = scheduler.track_accepted_sandbox_inner(&id, 0, None, runner.clone());
	let Tracked::Destroy(destroy) = tracked else {
		panic!("expected a destroy");
	};
	assert_eq!(destroy.runner, runner.id);
	assert_eq!(destroy.registration_index, runner.registration_index);
	assert_eq!(destroy.sandbox, id);
	assert!(matches!(
		scheduler.sandboxes.entries[&id].state,
		State::Terminal { .. }
	));
}
