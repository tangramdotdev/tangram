use {
	super::Runner,
	crate::SchedulerState,
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

/// Create a scheduler's state with a runner that registered at an instant and has not sent a heartbeat.
fn state(id: &tg::runner::Id, registration_index: u64, now: Instant) -> SchedulerState {
	let mut state = SchedulerState::new();
	let runner = Runner {
		borrowable: HashSet::default(),
		capacity: capacity(),
		committed: tg::runner::Capacity::default(),
		heartbeat_at: now,
		heartbeat_index: 0,
		host: String::new(),
		owner: None,
		registration_failed: false,
		registration_index,
		reservations: HashMap::default(),
		reserved: tg::runner::Capacity::default(),
	};
	state.runners.entries.insert(id.clone(), runner);
	state
}

/// Create a heartbeat of a runner's registration.
fn heartbeat(
	id: &tg::runner::Id,
	registration_index: u64,
	heartbeat_index: u64,
) -> HeartbeatNotification {
	HeartbeatNotification {
		capacity: capacity(),
		heartbeat_index,
		registration_index,
		runner: id.clone(),
		sandboxes: BTreeSet::new(),
	}
}

#[test]
fn a_runner_whose_registration_failed_takes_no_sandboxes_and_expires() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 1, now);

	// Fail the registration.
	state.fail_runner_registration(&id, 1);

	// A heartbeat of the failed registration neither readies the runner nor delays its expiry.
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
fn a_failed_earlier_registration_leaves_a_later_registration_alone() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 2, now);

	// Fail an earlier registration, which a later one replaced.
	state.fail_runner_registration(&id, 1);

	// The later registration becomes ready with its first heartbeat.
	assert!(state.handle_heartbeat(&heartbeat(&id, 2, 1), now));
	assert!(state.runners.entries[&id].ready());
}
