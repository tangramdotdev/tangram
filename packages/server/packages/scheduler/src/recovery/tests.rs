use {
	super::{RecoveredBoot, Recovery, SavedBoot, decide_saved_boot},
	crate::{SchedulerState, runner::tests::request, sandbox::is_held_by_runner},
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_server_core::clock::Clock,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

/// Create the index's record of a sandbox at a boot with a status, whose last write has a number and records a runner.
pub(crate) fn indexed(
	sandbox: &tg::sandbox::Id,
	boot: u64,
	status: tg::sandbox::Status,
	sequence: u64,
	runner: Option<&tg::runner::Id>,
) -> tangram_index::sandbox::Sandbox {
	let data = tg::sandbox::Data {
		boot,
		cpu: None,
		creator: tg::Principal::Root,
		error: None,
		expires_at: None,
		hostname: None,
		id: sandbox.clone(),
		memory: None,
		mounts: Vec::new(),
		network: None,
		owner: tg::Principal::Root,
		status,
		stopped_at: None,
		storage: None,
		time_to_live: None,
		time_to_stop: None,
		usage: None,
	};
	let data = tg::sandbox::get::Output {
		data,
		location: None,
		tokens: tg::authorization::Tokens::default(),
	};
	tangram_index::sandbox::Sandbox {
		account: None,
		created_at: 0,
		data: Some(data),
		ends: BTreeMap::new(),
		location: None,
		reference_count: 0,
		runner: runner.cloned(),
		scheduler: None,
		sequence,
		set: tangram_index::sandbox::Set::default(),
		stopped_usages: BTreeMap::new(),
		touched_at: 0,
		usages: BTreeMap::new(),
	}
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

/// Create the saved request of a sandbox's boot.
fn saved(sandbox: &tg::sandbox::Id, boot: u64) -> EnqueueSandboxRequestArg {
	EnqueueSandboxRequestArg {
		arg: tg::sandbox::create::Arg::default(),
		boot,
		created_at: 0,
		creator: tg::Principal::default(),
		owner: tg::Principal::default(),
		owner_ancestors: Vec::new(),
		parent: None,
		process: None,
		public: false,
		runner: None,
		sandbox: sandbox.clone(),
		scheduler: None,
		token: None,
	}
}

#[test]
fn every_connection_of_a_runner_reports_to_a_restarted_scheduler_until_one_is_applied() {
	let id = tg::runner::Id::new();
	let now = Clock::new(None, None).instant();
	let mut state = SchedulerState::new();

	// The restarted scheduler waits for the runner's report, with a boot that it rebuilt from the index on the runner and a saved boot.
	let rebuilt = tg::sandbox::Id::new();
	let queued = tg::sandbox::Id::new();
	let mut recovery = Recovery::default();
	let recovered = RecoveredBoot {
		boot: 0,
		processes: BTreeSet::new(),
		sandbox: rebuilt.clone(),
	};
	recovery.boots.insert(id.clone(), vec![recovered]);
	recovery.requests.insert(queued.clone(), saved(&queued, 0));
	recovery.runners.insert(id.clone());
	state.recovery = Some(recovery);

	// The runner connects twice, reporting the saved boot, before either connection is applied, and each connection reports to the restarted scheduler, whose state neither changes.
	for attempt in 0..2 {
		let mut connection = request(&id, false, vec![reported(&queued, 0)]);
		let connection = state.connect_runner(&mut connection, now);
		assert!(
			connection.recovering,
			"the connection {attempt} did not report to the restarted scheduler"
		);
		let recovery = state.recovery.as_ref().unwrap();
		assert!(recovery.runners.contains(&id));
		assert_eq!(recovery.boots[&id].len(), 1);
		assert!(
			recovery.claimed.is_empty(),
			"the connection {attempt} claimed the saved boot before it was applied"
		);
		assert!(recovery.requests.contains_key(&queued));
	}
}

#[test]
fn a_saved_boot_that_the_index_shows_ended_is_never_offered_again() {
	let sandbox = tg::sandbox::Id::new();
	let runner = tg::runner::Id::new();
	let request = saved(&sandbox, 1);
	let decide = |indexed: Option<tangram_index::sandbox::Sandbox>| {
		decide_saved_boot(request.boot, indexed.as_ref())
	};

	// A boot that its runner stopped or whose sandbox it destroyed, and a boot that a later boot replaced, ended.
	for (boot, status) in [
		(1, tg::sandbox::Status::Stopped),
		(1, tg::sandbox::Status::Destroyed),
		(2, tg::sandbox::Status::Starting),
	] {
		let decision = decide(Some(indexed(&sandbox, boot, status, 3, Some(&runner))));
		assert!(
			matches!(decision, SavedBoot::Ended),
			"the boot {boot} with the status {status} was not decided as ended"
		);
	}

	// A running boot that a runner wrote belongs to the runner.
	let decision = decide(Some(indexed(
		&sandbox,
		1,
		tg::sandbox::Status::Started,
		1,
		Some(&runner),
	)));
	assert!(matches!(decision, SavedBoot::Written { runner: written } if written == runner));

	// A boot that only this scheduler wrote, or that the index does not have, is still held by this scheduler.
	let decision = decide(Some(indexed(
		&sandbox,
		1,
		tg::sandbox::Status::Starting,
		0,
		None,
	)));
	assert!(matches!(decision, SavedBoot::Unwritten));
	assert!(matches!(decide(None), SavedBoot::Unwritten));

	// A sandbox that is being destroyed is destroyed.
	let decision = decide(Some(indexed(
		&sandbox,
		1,
		tg::sandbox::Status::Destroying,
		3,
		Some(&runner),
	)));
	assert!(matches!(decision, SavedBoot::Destroying { .. }));
}

#[test]
fn a_restarted_scheduler_does_not_queue_a_sandbox_that_a_runner_holds() {
	let sandbox = tg::sandbox::Id::new();
	let runner = tg::runner::Id::new();
	let mut scheduler = SchedulerState::new();
	let recovered = RecoveredBoot {
		boot: 1,
		processes: BTreeSet::new(),
		sandbox: sandbox.clone(),
	};
	let recovery = Recovery {
		boots: [(runner, vec![recovered])].into_iter().collect(),
		..Default::default()
	};
	scheduler.recovery = Some(recovery);
	let held = |boot| {
		let entry = scheduler.sandboxes.entries.get(&sandbox);
		let recovered = scheduler.try_get_recovered_boot(&sandbox);
		is_held_by_runner(boot, entry, recovered)
	};

	// An enqueue of the boot that the index records on a runner that has not reported yet, or of an earlier boot, is answered without queueing the sandbox.
	assert!(held(0));
	assert!(held(1));

	// An enqueue of a later boot is not answered from the rebuilt boot.
	assert!(!held(2));
}
