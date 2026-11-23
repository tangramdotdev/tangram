use {
	super::{Runner, UnansweredOffer, decide_created_sandbox, decide_unanswered_offers},
	crate::{
		SchedulerState,
		placement::RunnerRef,
		recovery::{SavedBoot, tests::indexed},
		report::{Authorization, Authorized, EndedBoot, Held},
		sandbox::{DestroySandbox, Sandbox, State, Tracked},
	},
	std::{
		collections::{BTreeSet, HashMap, HashSet},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_server_core::clock::{Clock, Instant},
	tangram_server_scheduler_core::{AddRunnerRequestArg, HeartbeatNotification},
};

const TIME_TO_LIVE: Duration = Duration::from_secs(10);

/// Create a connection of a runner that reports boots.
pub(crate) fn request(
	id: &tg::runner::Id,
	first: bool,
	sandboxes: Vec<tg::runner::control::ArgSandbox>,
) -> AddRunnerRequestArg {
	AddRunnerRequestArg {
		capacity: capacity(),
		first,
		host: String::new(),
		runner: id.clone(),
		sandboxes,
	}
}

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
		connection,
		heartbeat_index,
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
	state.sandboxes.ended.forget_runner(&id, &[]);
	assert_eq!(told(&state), Vec::<tg::sandbox::Id>::new());
	assert!(state.sandboxes.ended.is_declared(&foreign, 0, &other));
}

#[test]
fn a_declared_end_is_told_again_after_a_first_connection_fails() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = SchedulerState::new();

	// The scheduler declared a boot of the runner ended, and the runner restarts and reports the boot.
	let sandbox = tg::sandbox::Id::new();
	state
		.sandboxes
		.ended
		.insert(sandbox.clone(), 0, ended(&id, true));
	let mut first = request(&id, true, vec![reported(&sandbox, 0)]);
	let connection = state.connect_runner(&mut first, now);
	assert_eq!(connection.ended.len(), 1);

	// The connection fails, and the runner connects again, which is told that the boot ended.
	state.fail_runner_connection(&id, connection.number);
	let mut again = request(&id, false, vec![reported(&sandbox, 0)]);
	let connection = state.connect_runner(&mut again, now);
	assert!(connection.first);
	assert_eq!(
		connection
			.ended
			.iter()
			.map(|ended| ended.id.clone())
			.collect::<Vec<_>>(),
		vec![sandbox],
		"the second connection was not told that the boot ended"
	);
}

#[test]
fn an_unanswered_offer_whose_boot_an_earlier_connection_ended_is_never_offered_again() {
	let runner = tg::runner::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let offer = UnansweredOffer {
		boot: 1,
		offer: Some(String::new()),
		sandbox: sandbox.clone(),
	};
	let decide = |indexed: tangram_index::sandbox::Sandbox| {
		let mut decided =
			decide_unanswered_offers(std::slice::from_ref(&offer), vec![Some(indexed)]);
		decided.remove(&sandbox).unwrap()
	};

	// A first connection of the runner that did not apply ended the boot that the runner wrote, with the end numbered zero, which leaves the sandbox stopped at the boot and its record numbered zero, and the next first connection decides that the boot ended.
	let decision = decide(indexed(
		&sandbox,
		1,
		tg::sandbox::Status::Stopped,
		0,
		Some(&runner),
	));
	assert!(
		matches!(decision, SavedBoot::Ended),
		"the offer of the ended boot would be returned to the queue"
	);

	// An active boot that the runner wrote is ended by the connection, and a boot that only this scheduler wrote is offered again.
	let decision = decide(indexed(
		&sandbox,
		1,
		tg::sandbox::Status::Started,
		1,
		Some(&runner),
	));
	assert!(decision.is_written());
	let decision = decide(indexed(&sandbox, 1, tg::sandbox::Status::Starting, 0, None));
	assert!(matches!(decision, SavedBoot::Unwritten));
}

#[test]
fn a_runner_whose_connections_never_apply_expires_one_ttl_after_its_first_connection() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = SchedulerState::new();

	// The runner connects, and connects again before each connection is applied, many times within the ttl.
	for attempt in 0..4 {
		let mut connection = request(&id, true, Vec::new());
		state.connect_runner(&mut connection, now + TIME_TO_LIVE / 4 * attempt);
	}

	// The runner expires one ttl after its first connection, because no connection applied.
	let expired = state
		.runners
		.expired(now + TIME_TO_LIVE + Duration::from_secs(1), TIME_TO_LIVE);
	assert_eq!(
		expired
			.into_iter()
			.map(|(runner, _)| runner)
			.collect::<Vec<_>>(),
		vec![id],
		"the connections that never applied kept the runner alive"
	);
}

#[test]
fn a_runner_that_restarted_is_sent_no_destroy_until_its_restart_is_handled() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = state(&id, 1, now);
	let destroy = DestroySandbox {
		connection: 1,
		error: tg::error::Data::default(),
		request: None,
		runner: id.clone(),
		sandbox: tg::sandbox::Id::new(),
	};

	// While the scheduler has not finished handling the runner's restart, the destroy is not sent.
	state.runners.entries.get_mut(&id).unwrap().restarting = true;
	assert_eq!(
		state.destroy_connection(&destroy),
		None,
		"the destroy would be sent to a runner whose restart is not handled"
	);

	// Once the runner's first connection is applied, the destroy is sent on the connection.
	state.runners.entries.get_mut(&id).unwrap().restarting = false;
	assert_eq!(state.destroy_connection(&destroy), Some(1));
}

#[test]
fn a_report_of_a_shortcut_sandbox_is_sent_again_unless_it_is_authorized_or_refused() {
	let authorized = || {
		Authorization::Authorized(Authorized {
			held: Held::Derived,
			processes: BTreeSet::new(),
		})
	};

	// An authorized report on the runner's current connection is acted on.
	assert!(matches!(
		decide_created_sandbox(true, authorized()),
		Ok(std::ops::ControlFlow::Break(_))
	));

	// A report on a connection that a reconnect replaced, and a report that the scheduler cannot authorize yet, are sent again.
	assert!(
		matches!(
			decide_created_sandbox(false, authorized()),
			Ok(std::ops::ControlFlow::Continue(_))
		),
		"a report on a replaced connection would be refused"
	);
	assert!(
		matches!(
			decide_created_sandbox(true, Authorization::NotReady),
			Ok(std::ops::ControlFlow::Continue(_))
		),
		"a report that the scheduler cannot authorize yet would be refused"
	);

	// A refused report is final.
	let error = tg::error!("the runner does not hold the boot");
	assert!(decide_created_sandbox(true, Authorization::Refused(error)).is_err());
}
