use {super::*, std::collections::BTreeSet};

/// A write of the process or of its sandbox.
enum Write {
	Process(Arg),
	Sandbox(crate::sandbox::put::Arg),
}

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
	let mut other = arg(&id, data(&sandbox, tg::process::Status::Finished));
	other.data.as_mut().unwrap().exit = Some(1);
	let ends = [&holder, &other].map(|end| serde_json::to_value(end.data.as_ref()).unwrap());
	let args = [started, holder.clone(), other.clone()];
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
fn a_process_write_at_or_above_its_boot_end_is_failed() {
	let id = tg::process::Id::new();
	let mut sandbox = sandbox_record(1, tg::sandbox::Status::Started);
	sandbox.ends.insert(0, end(3));
	let sandbox_id = sandbox.data.as_ref().unwrap().data.id.clone();
	for status in [
		tg::process::Status::Queued,
		tg::process::Status::Started,
		tg::process::Status::Finished,
	] {
		let mut write = arg(&id, data(&sandbox_id, status));
		write.boot = Some(0);
		write.permissions = vec![permission(&id, 1)];
		write.sequence = 3;
		let mut state = State::default();
		apply(&mut state, 0, &write, Some(&sandbox));
		let data = state.data.unwrap();
		assert_eq!(data.status, tg::process::Status::Finished);
		assert_eq!(data.exit, Some(1));
		assert!(data.error.is_some());
		assert_eq!(state.permissions.len(), 1);
	}
}

#[test]
fn a_process_write_below_its_boot_end_applies() {
	let id = tg::process::Id::new();
	let mut sandbox = sandbox_record(1, tg::sandbox::Status::Started);
	sandbox.ends.insert(0, end(3));
	let sandbox_id = sandbox.data.as_ref().unwrap().data.id.clone();
	let mut started = arg(&id, data(&sandbox_id, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 2;
	let mut state = State::default();
	apply(&mut state, 0, &started, Some(&sandbox));
	assert_eq!(state.data.unwrap().status, tg::process::Status::Started);
}

#[test]
#[expect(
	clippy::large_stack_arrays,
	reason = "the test lists the writes that it applies in every order"
)]
fn the_end_of_a_boot_and_the_writes_of_its_processes_keep_the_first_end_in_every_order() {
	let first = tg::process::Id::new();
	let second = tg::process::Id::new();
	let sandbox_id = tg::sandbox::Id::new();

	// The first process is queued by the scheduler, then started and finished by the runner. Every write has its own time, and every write that records the start carries the same start, as the runner sets it once.
	let mut queued = arg(&first, data(&sandbox_id, tg::process::Status::Queued));
	queued.boot = Some(0);
	queued.permissions = vec![permission(&first, 1)];
	queued.touched_at = 10;
	let mut started = arg(&first, data(&sandbox_id, tg::process::Status::Started));
	started.boot = Some(0);
	started.sequence = 1;
	started.touched_at = 20;
	let mut finished = arg(&first, data(&sandbox_id, tg::process::Status::Finished));
	finished.boot = Some(0);
	finished.sequence = 2;
	finished.touched_at = 30;
	let finished_data = finished.data.as_mut().unwrap();
	finished_data.log = Some(tg::Referent::with_node(tg::blob::Id::new(b"log")));
	finished_data.output = Some(tg::Referent::with_node(tg::value::Data::Null));

	// The second process's first write carries no boot, such as a write of a parent by its child's spawn, and the runner's start sets its boot.
	let mut unbooted = arg(&second, data(&sandbox_id, tg::process::Status::Started));
	unbooted.principal = tg::Principal::Process(second.clone());
	unbooted.touched_at = 40;
	let mut booted = arg(&second, data(&sandbox_id, tg::process::Status::Started));
	booted.boot = Some(0);
	booted.sequence = 1;
	booted.touched_at = 50;

	// The sandbox's scheduler ends the boot with the number zero, and its holder ends it with the number three.
	let mut zero = sandbox_arg(&sandbox_id, 0);
	zero.touched_at = 60;
	let mut holder = sandbox_arg(&sandbox_id, 3);
	holder.touched_at = 70;

	let writes = [
		(0, Write::Process(queued)),
		(0, Write::Process(started)),
		(0, Write::Process(finished)),
		(1, Write::Process(unbooted)),
		(1, Write::Process(booted)),
		(2, Write::Sandbox(zero)),
		(2, Write::Sandbox(holder)),
	];
	let mut expected = std::collections::BTreeMap::new();
	for order in crate::tests::permutations(writes.len()) {
		// Apply the writes in the order, failing with a zero end every unfinished process that the index lists under the boot, as the index backends do.
		let mut states = [State::default(), State::default()];
		let mut sandbox = None::<crate::sandbox::Sandbox>;
		for &index in &order {
			match &writes[index] {
				(process, Write::Process(arg)) => {
					apply(&mut states[*process], index, arg, sandbox.as_ref());
				},
				(_, Write::Sandbox(arg)) => {
					let merge = arg.merge(sandbox.as_ref(), None).unwrap().unwrap();
					let sandbox = sandbox.insert(merge.sandbox);
					let Some(boot) = merge.zero_end else {
						continue;
					};
					for (state, id) in states.iter_mut().zip([&first, &second]) {
						let listed = state
							.process
							.as_ref()
							.is_some_and(|process| process.boot == Some(boot));
						if !listed {
							continue;
						}
						let failed =
							Arg::failed(id, state.process.as_ref(), state.data.clone(), sandbox);
						if let Some(failed) = failed {
							apply(state, index, &failed, Some(sandbox));
						}
					}
				},
			}
		}

		// Each process keeps its own end if its finish or the holder's end of the boot arrived before the zero end, and is failed with the zero end's time otherwise.
		let position = |index: usize| order.iter().position(|i| *i == index).unwrap();
		let zero_first = position(5) < position(6);
		let normal = !zero_first || position(2) < position(5);
		let first_data = states[0].data.as_ref().unwrap();
		assert_eq!(first_data.status, tg::process::Status::Finished);
		assert_eq!(
			(first_data.exit, first_data.finished_at),
			if normal {
				(Some(0), Some(2))
			} else {
				(Some(1), Some(60))
			},
			"the order {order:?} did not keep the first end of the first process"
		);
		assert_eq!(first_data.started_at, Some(1));
		let second_data = states[1].data.as_ref().unwrap();
		let expected_status = if zero_first {
			tg::process::Status::Finished
		} else {
			tg::process::Status::Started
		};
		assert_eq!(
			second_data.status, expected_status,
			"the order {order:?} did not keep the first end of the second process"
		);
		assert_eq!(states[0].permissions.len(), 1);
		assert!(states.iter().all(|state| state.rejected.is_empty()));

		// Compare the whole records with those of the first order that keeps the same ends.
		let outcomes = states.each_ref().map(outcome);
		match expected.get(&(normal, zero_first)) {
			None => {
				expected.insert((normal, zero_first), (order, outcomes));
			},
			Some((first, expected)) => {
				assert_eq!(
					&outcomes, expected,
					"the order {order:?} produced different records than the order {first:?}"
				);
			},
		}
	}
}

#[test]
fn a_process_of_a_boot_that_its_holder_ended_keeps_its_own_end_in_every_order() {
	let id = tg::process::Id::new();
	let mut sandbox = sandbox_record(1, tg::sandbox::Status::Started);
	sandbox.ends.insert(0, end(3));
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
	let data = if arg.reads_data(state.process.as_ref()) {
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
		sandbox: None,
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

fn end(sequence: u64) -> crate::sandbox::End {
	crate::sandbox::End {
		at: 0,
		error: None,
		sequence,
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

fn sandbox_arg(id: &tg::sandbox::Id, sequence: u64) -> crate::sandbox::put::Arg {
	let mut data = sandbox_data(0, tg::sandbox::Status::Stopped);
	data.data.id = id.clone();
	crate::sandbox::put::Arg {
		account: None,
		created_at: 0,
		data: Some(data),
		id: id.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Root,
		process: None,
		processes: None,
		runner: None,
		scheduler: None,
		sequence,
		stopped_boot: None,
		stopped_usage: None,
		touched_at: 0,
	}
}

fn sandbox_record(boot: u64, status: tg::sandbox::Status) -> crate::sandbox::Sandbox {
	let data = sandbox_data(boot, status);
	crate::sandbox::Sandbox {
		account: None,
		created_at: 0,
		data: Some(data),
		ends: std::collections::BTreeMap::new(),
		location: None,
		reference_count: 0,
		runner: None,
		scheduler: None,
		sequence: 0,
		set: crate::sandbox::Set::default(),
		stopped_usages: std::collections::BTreeMap::new(),
		touched_at: 0,
		usages: std::collections::BTreeMap::new(),
	}
}

fn sandbox_data(boot: u64, status: tg::sandbox::Status) -> tg::sandbox::get::Output {
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
	tg::sandbox::get::Output {
		data,
		location: None,
		tokens: tg::authorization::Tokens::default(),
	}
}
