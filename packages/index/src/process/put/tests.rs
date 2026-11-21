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
	queued.permissions = vec![permission(&id, 1)];
	let started = arg(&id, data(&sandbox, tg::process::Status::Started));
	let mut finished = arg(&id, data(&sandbox, tg::process::Status::Finished));
	finished.children = Some(Vec::new());
	assert_every_order_converges(&[queued, started, finished]);
}

/// Apply the writes in every order, and assert that every order leaves the same records and rejects the same writes.
fn assert_every_order_converges(args: &[Arg]) {
	let mut expected = None;
	for order in crate::tests::permutations(args.len()) {
		// Apply the writes in the order.
		let mut state = State::default();
		for &index in &order {
			apply(&mut state, index, &args[index]);
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
	}
}

fn apply(state: &mut State, index: usize, arg: &Arg) {
	let data = if arg.compares_data(state.process.as_ref()) {
		state.data.as_ref()
	} else {
		None
	};
	let context = Context {
		accepts_child: None,
		children: None,
		data,
		sandbox: None,
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
