use {
	super::{RecoveredBoot, Recovery},
	crate::{SchedulerState, runner::tests::request},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_server_core::clock::Clock,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

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
	recovery.boots.insert(
		id.clone(),
		vec![RecoveredBoot {
			boot: 0,
			processes: BTreeSet::new(),
			sandbox: rebuilt.clone(),
		}],
	);
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
