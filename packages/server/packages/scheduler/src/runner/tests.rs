use {
	super::Runner,
	crate::{
		SchedulerState,
		placement::RunnerRef,
		report::EndedBoot,
		sandbox::{Sandbox, State, Tracked},
	},
	std::{
		collections::{BTreeSet, HashMap, HashSet},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_server_core::clock::{Clock, Instant},
	tangram_server_scheduler_core::HeartbeatNotification,
};

const TIME_TO_LIVE: Duration = Duration::from_secs(10);

/// Create the capacity of a runner that takes one sandbox.
fn capacity() -> tg::runner::control::Capacity {
	let capacity = tg::runner::Capacity {
		sandboxes: 1,
		..Default::default()
	};
	tg::runner::control::Capacity {
		available: capacity,
		cpu_limit: 1,
		overcommit: tg::runner::Overcommit::default(),
		total: capacity,
	}
}

/// Create a scheduler's state with a runner that connected at an instant and has not sent a heartbeat.
fn state(id: &tg::runner::Id, connection: u64, now: Instant) -> SchedulerState {
	let mut state = SchedulerState::new();
	let runner = Runner {
		borrowable: HashSet::default(),
		capacity: capacity(),
		committed: tg::runner::Capacity::default(),
		connection,
		connection_failed: false,
		heartbeat_at: now,
		heartbeat_index: 0,
		host: String::new(),
		owner: None,
		reservations: HashMap::default(),
		reserved: tg::runner::Capacity::default(),
		restarting: false,
	};
	state.runners.entries.insert(id.clone(), runner);
	state
}

/// Create a heartbeat of a runner's connection.
fn heartbeat(id: &tg::runner::Id, connection: u64, heartbeat_index: u64) -> HeartbeatNotification {
	HeartbeatNotification {
		capacity: capacity(),
		heartbeat_index,
		connection,
		runner: id.clone(),
		sandboxes: BTreeSet::new(),
	}
}

#[test]
fn a_runner_whose_connection_failed_takes_no_sandboxes_and_expires() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 1, now);

	// Fail the connection.
	state.fail_runner_connection(&id, 1);

	// A heartbeat of the failed connection neither readies the runner nor delays its expiry.
	let later = now + TIME_TO_LIVE / 2;
	assert!(!state.handle_heartbeat(&heartbeat(&id, 1, 1), later));
	let runner = &state.runners.entries[&id];
	assert!(!runner.ready());
	assert_eq!(runner.heartbeat_at, now);

	// The runner expires after the ttl.
	let expired = state
		.runners
		.expired(now + TIME_TO_LIVE + Duration::from_secs(1), TIME_TO_LIVE);
	assert_eq!(expired, vec![(id, 1)]);
}

#[test]
fn a_failed_earlier_connection_leaves_a_later_connection_alone() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 2, now);

	// Fail an earlier connection, which a later one replaced.
	state.fail_runner_connection(&id, 1);

	// The later connection becomes ready with its first heartbeat.
	assert!(state.handle_heartbeat(&heartbeat(&id, 2, 1), now));
	assert!(state.runners.entries[&id].ready());
}

/// Create the report of a boot that a runner holds.
fn reported(sandbox: &tg::sandbox::Id, boot: u64) -> tg::runner::control::ArgSandbox {
	tg::runner::control::ArgSandbox {
		boot,
		id: sandbox.clone(),
		processes: Vec::new(),
		proof: None,
	}
}

/// Create the memory of a runner's ended boot.
fn ended(runner: &tg::runner::Id, declared: bool) -> EndedBoot {
	EndedBoot {
		declared,
		processes: BTreeSet::new(),
		runner: runner.clone(),
	}
}

#[test]
fn reconnects_of_a_runner_that_holds_a_boot_tell_it_that_nothing_ended() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 1, now);

	// The runner holds a boot.
	let sandbox = tg::sandbox::Id::new();
	let runner = RunnerRef {
		connection: 1,
		id: id.clone(),
	};
	let entry = Sandbox::new(0, None, None, State::Accepted { runner });
	state.sandboxes.entries.insert(sandbox.clone(), entry);

	// The runner reconnects twice and reports the boot each time, and neither connection is told that the boot ended.
	for connection in [2, 3] {
		let ended = state.get_declared_ended_boots(&id, &[reported(&sandbox, 0)]);
		assert!(
			ended.is_empty(),
			"the connection {connection} was told that the boot ended"
		);
		let runner = RunnerRef {
			connection,
			id: id.clone(),
		};
		let tracked = state.track_accepted_sandbox_inner(&sandbox, 0, None, runner);
		assert!(matches!(tracked, Tracked::Ignored));
	}
	let ended = state.get_declared_ended_boots(&id, &[reported(&sandbox, 0)]);
	assert!(
		ended.is_empty(),
		"the next connection was told that the boot ended"
	);

	// The scheduler still tracks the boot on the runner's latest connection, and does not remember it as ended.
	assert!(matches!(
		&state.sandboxes.entries[&sandbox].state,
		State::Accepted { runner } if runner.connection == 3
	));
	assert!(state.sandboxes.ended.get(&sandbox, 0).is_none());
}

#[test]
fn a_connection_is_told_only_the_boots_that_the_scheduler_declared_ended() {
	let id = tg::runner::Id::new();
	let other = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 1, now);

	// The runner ended one boot itself, the scheduler declared another ended, and the scheduler declared a third ended on another runner.
	let own = tg::sandbox::Id::new();
	let declared = tg::sandbox::Id::new();
	let foreign = tg::sandbox::Id::new();
	state
		.sandboxes
		.ended
		.insert(own.clone(), 0, ended(&id, false));
	state
		.sandboxes
		.ended
		.insert(declared.clone(), 0, ended(&id, true));
	state
		.sandboxes
		.ended
		.insert(foreign.clone(), 0, ended(&other, true));
	let sandboxes = [
		reported(&own, 0),
		reported(&declared, 0),
		reported(&foreign, 0),
	];

	// Only the boot that the scheduler declared ended on the runner is told, and it stays declared when the runner's own end is remembered too.
	let told = |state: &SchedulerState| {
		state
			.get_declared_ended_boots(&id, &sandboxes)
			.into_iter()
			.map(|ended| ended.id)
			.collect::<Vec<_>>()
	};
	assert_eq!(told(&state), vec![declared.clone()]);
	state
		.sandboxes
		.ended
		.insert(declared.clone(), 0, ended(&id, false));
	assert_eq!(told(&state), vec![declared.clone()]);

	// The rotations forget the boot that the runner ended itself, and keep the declared boot.
	state.sandboxes.ended.rotate(now, TIME_TO_LIVE);
	for rotation in 1..=2 {
		let later = now + (TIME_TO_LIVE + Duration::from_secs(1)) * rotation;
		state.sandboxes.ended.rotate(later, TIME_TO_LIVE);
	}
	assert!(state.sandboxes.ended.get(&own, 0).is_none());
	assert_eq!(told(&state), vec![declared.clone()]);

	// Forgetting the runner forgets its declared boot, and leaves the other runner's.
	state.sandboxes.ended.forget_runner(&id);
	assert_eq!(told(&state), Vec::<tg::sandbox::Id>::new());
	assert!(state.sandboxes.ended.is_declared(&foreign, 0, &other));
}
