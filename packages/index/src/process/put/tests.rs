use {super::*, std::collections::BTreeSet};

/// The records that the index holds for one process, as the merge's effects leave them.
#[derive(Default)]
struct State {
	data: Option<tg::process::Data>,
	permissions: BTreeSet<String>,
	process: Option<crate::process::Process>,
	rejected: BTreeSet<usize>,
}

#[test]
fn the_lifecycle_writes_of_a_process_converge_in_every_order() {
	let id = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let mut queued = arg(&id, data(&sandbox, tg::process::Status::Queued));
	queued.boot = Some(0);
	queued.permissions = vec![permission(&id, 1)];
	let mut started = arg(&id, data(&sandbox, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 1;
	let mut finished = arg(&id, data(&sandbox, tg::process::Status::Finished));
	finished.children = Some(Vec::new());
	finished.sequence = 2;
	let state = assert_every_order_converges(&[queued, started, finished], None);
	assert_eq!(state.process.as_ref().unwrap().boot, Some(0));
	assert_eq!(
		state.data.as_ref().unwrap().status,
		tg::process::Status::Finished
	);
	assert_eq!(state.permissions.len(), 1);
}

#[test]
fn a_holder_write_supersedes_the_writes_with_lower_sequences() {
	let id = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let queued = arg(&id, data(&sandbox, tg::process::Status::Queued));
	let mut started = arg(&id, data(&sandbox, tg::process::Status::Started));
	started.sequence = 1;
	let mut later = arg(&id, data(&sandbox, tg::process::Status::Started));
	later.sequence = 2;
	later.data.as_mut().unwrap().started_at = Some(5);
	let state = assert_every_order_converges(&[queued, started, later], None);
	assert_eq!(state.data.as_ref().unwrap().started_at, Some(5));
}

#[test]
fn the_permissions_of_a_queued_record_apply_after_a_child_link() {
	let id = tg::process::Id::new();
	let parent = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let mut queued = arg(&id, data(&sandbox, tg::process::Status::Queued));
	queued.permissions = vec![permission(&id, 1)];
	let mut link = arg(&id, data(&sandbox, tg::process::Status::Queued));
	link.data = None;
	link.parent = Some(parent.clone());
	link.principal = tg::Principal::Process(parent);
	let state = assert_every_order_converges(&[queued, link], None);
	assert_eq!(state.permissions.len(), 1);
}

#[test]
fn the_first_end_to_arrive_is_final() {
	let id = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let mut started = arg(&id, data(&sandbox, tg::process::Status::Started));
	started.sequence = 1;
	let mut holder = arg(&id, data(&sandbox, tg::process::Status::Finished));
	holder.sequence = 2;
	let mut takeover = arg(&id, data(&sandbox, tg::process::Status::Finished));
	takeover.data.as_mut().unwrap().exit = Some(1);
	let ends = [&holder, &takeover].map(|end| serde_json::to_value(end.data.as_ref()).unwrap());
	let args = [started, holder.clone(), takeover.clone()];
	for order in crate::tests::permutations(args.len()) {
		let mut state = State::default();
		for &index in &order {
			apply(&mut state, index, &args[index], None);
		}
		let first = order.iter().copied().find(|index| *index != 0).unwrap();
		let data = serde_json::to_value(state.data.as_ref()).unwrap();
		assert_eq!(
			data,
			ends[first - 1],
			"the order {order:?} did not keep the first end"
		);
		assert!(state.rejected.is_empty());
	}
}

#[test]
fn a_process_of_a_boot_that_a_takeover_ended_is_finished_by_its_first_write() {
	let id = tg::process::Id::new();
	let mut sandbox = sandbox_record(1, tg::sandbox::Status::Started);
	sandbox.taken_over.insert(0);
	let sandbox_id = sandbox.data.as_ref().unwrap().data.id.clone();
	let mut queued = arg(&id, data(&sandbox_id, tg::process::Status::Queued));
	queued.boot = Some(0);
	queued.permissions = vec![permission(&id, 1)];
	let mut started = arg(&id, data(&sandbox_id, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 1;
	let args = [queued, started];
	for order in crate::tests::permutations(args.len()) {
		let mut state = State::default();
		for &index in &order {
			apply(&mut state, index, &args[index], Some(&sandbox));
		}
		let data = state.data.unwrap();
		assert_eq!(data.status, tg::process::Status::Finished);
		assert!(data.error.is_some());
		assert_eq!(state.permissions.len(), 1);
	}
}

#[test]
fn a_process_of_a_boot_that_its_holder_ended_keeps_its_own_end_in_every_order() {
	let id = tg::process::Id::new();
	let sandbox = sandbox_record(1, tg::sandbox::Status::Started);
	let sandbox_id = sandbox.data.as_ref().unwrap().data.id.clone();
	let mut started = arg(&id, data(&sandbox_id, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 1;
	let mut finished = arg(&id, data(&sandbox_id, tg::process::Status::Finished));
	finished.boot = Some(0);
	finished.sequence = 2;
	let state = assert_every_order_converges(&[started.clone(), finished], Some(&sandbox));
	let data = state.data.unwrap();
	assert_eq!(data.exit, Some(0));
	assert!(data.error.is_none());

	// A late start of a process of a boot that its holder ended stays unfinished until the process's end arrives.
	let state = assert_every_order_converges(&[started], Some(&sandbox));
	assert_eq!(state.data.unwrap().status, tg::process::Status::Started);
}

#[test]
fn a_process_of_a_running_boot_is_not_finished() {
	let id = tg::process::Id::new();
	let sandbox = sandbox_record(0, tg::sandbox::Status::Started);
	let sandbox_id = sandbox.data.as_ref().unwrap().data.id.clone();
	let mut queued = arg(&id, data(&sandbox_id, tg::process::Status::Queued));
	queued.boot = Some(0);
	let mut started = arg(&id, data(&sandbox_id, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 1;
	let state = assert_every_order_converges(&[queued, started], Some(&sandbox));
	assert_eq!(state.data.unwrap().status, tg::process::Status::Started);
}

#[test]
fn a_write_that_changes_the_boot_is_rejected() {
	let id = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let mut queued = arg(&id, data(&sandbox, tg::process::Status::Queued));
	queued.boot = Some(0);
	let mut started = arg(&id, data(&sandbox, tg::process::Status::Started));
	started.boot = Some(1);
	started.sequence = 1;
	let mut state = State::default();
	apply(&mut state, 0, &queued, None);
	apply(&mut state, 1, &started, None);
	assert!(state.rejected.contains(&1));
}

/// Apply the writes in every order with the sandbox's record, and assert that every order leaves the same records and rejects the same writes. Return the records of the last order.
fn assert_every_order_converges(args: &[Arg], sandbox: Option<&crate::sandbox::Sandbox>) -> State {
	let mut expected = None;
	let mut last = None;
	for order in crate::tests::permutations(args.len()) {
		// Apply the writes in the order.
		let mut state = State::default();
		for &index in &order {
			apply(&mut state, index, &args[index], sandbox);
		}

		// Compare the records with the first order's records.
		let outcome = outcome(&state);
		match &expected {
			None => {
				expected = Some((order, outcome));
			},
			Some((first, expected)) => {
				assert_eq!(
					&outcome, expected,
					"the order {order:?} produced different records than the order {first:?}"
				);
			},
		}
		last = Some(state);
	}

	last.unwrap()
}

fn apply(state: &mut State, index: usize, arg: &Arg, sandbox: Option<&crate::sandbox::Sandbox>) {
	let sandbox = arg.context_sandbox(state.process.as_ref()).and(sandbox);
	let data = if arg.reads_data(state.process.as_ref(), sandbox) {
		state.data.as_ref()
	} else {
		None
	};
	let context = Context {
		accepts_child: None,
		children: None,
		data,
		sandbox,
	};
	let Ok(merge) = arg.merge(state.process.as_ref(), context).unwrap() else {
		state.rejected.insert(index);
		return;
	};
	if merge.permissions {
		for permission in &arg.permissions {
			state.permissions.insert(format!("{permission:?}"));
		}
	}
	if let Some(update) = merge.update {
		if let Some(data) = update.data {
			state.data = Some(data);
		}
		state.process = Some(update.process);
	}
}

fn outcome(
	state: &State,
) -> (
	Option<Vec<u8>>,
	Option<serde_json::Value>,
	Vec<String>,
	Vec<usize>,
) {
	let process = state
		.process
		.as_ref()
		.map(|process| process.serialize().unwrap());
	let data = state
		.data
		.as_ref()
		.map(|data| serde_json::to_value(data).unwrap());
	let permissions = state.permissions.iter().cloned().collect();
	let rejected = state.rejected.iter().copied().collect();
	(process, data, permissions, rejected)
}

fn arg(id: &tg::process::Id, data: tg::process::Data) -> Arg {
	Arg {
		account: None,
		boot: None,
		cached: false,
		captures: crate::permission::capture::Captures::default(),
		children: None,
		command: None,
		command_id: tg::command::Id::new(b"command").into(),
		data: Some(data),
		error: None,
		id: id.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		permissions: Vec::new(),
		principal: tg::Principal::Process(id.clone()),
		registration: None,
		sandbox: None,
		sandbox_boot: None,
		sequence: 0,
		storage: Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

fn data(sandbox: &tg::sandbox::Id, status: tg::process::Status) -> tg::process::Data {
	let finished = status.is_finished();
	tg::process::Data {
		cacheable: false,
		checksum: tg::process::Checksum::default(),
		children: None,
		command: tg::Referent::with_node(tg::Either::Right(tg::command::Id::new(b"command"))),
		created_at: 0,
		creator: tg::Principal::Root,
		debug: None,
		error: None,
		exit: finished.then_some(0),
		finished_at: finished.then_some(2),
		host: "host".to_owned(),
		log: None,
		output: None,
		owner: tg::Principal::Root,
		retry: false,
		sandbox: Some(sandbox.clone()),
		started_at: (!status.is_queued()).then_some(1),
		status,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	}
}

fn permission(id: &tg::process::Id, created_at: i64) -> crate::permission::put::Arg {
	crate::permission::put::Arg {
		created_at,
		permissions: tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::default(),
		),
		resource: id.clone().into(),
		source: crate::permission::Source::Direct {
			expires_at: Some(created_at),
		},
		subject: tg::authorization::Subject::Public,
		time_to_touch: None,
		version: None,
	}
}

fn sandbox_record(boot: u64, status: tg::sandbox::Status) -> crate::sandbox::Sandbox {
	let data = tg::sandbox::Data {
		boot,
		cpu: None,
		creator: tg::Principal::Root,
		error: None,
		expires_at: None,
		hostname: None,
		id: tg::sandbox::Id::new(),
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
	crate::sandbox::Sandbox {
		account: None,
		created_at: 0,
		data: Some(data),
		location: None,
		reference_count: 0,
		runner: None,
		scheduler: None,
		sequence: 0,
		set: crate::sandbox::Set::default(),
		stopped_usages: std::collections::BTreeMap::new(),
		taken_over: std::collections::BTreeSet::new(),
		touched_at: 0,
		usages: std::collections::BTreeMap::new(),
	}
}
