use {
	super::{Sandbox, State},
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
		runner: None,
		sandbox: id.clone(),
		scheduler: None,
		stop: false,
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
