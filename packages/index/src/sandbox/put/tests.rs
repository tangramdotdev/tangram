use super::*;

#[test]
fn a_late_write_does_not_overwrite_a_later_status() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let merge = arg(data(0, tg::sandbox::Status::Started, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Stopped);
}

#[test]
fn a_start_overwrites_the_previous_stop() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let merge = arg(data(1, tg::sandbox::Status::Starting, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Starting);
}

#[test]
fn a_destroy_is_not_overwritten_by_a_later_boot() {
	let existing = sandbox(data(0, tg::sandbox::Status::Destroyed, None));
	let merge = arg(data(1, tg::sandbox::Status::Starting, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Destroyed);
}

#[test]
fn a_destroying_status_is_after_every_boot_and_before_destroyed() {
	let existing = sandbox(data(0, tg::sandbox::Status::Destroying, None));
	let merge = arg(data(1, tg::sandbox::Status::Started, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Destroying);
	let merge = arg(data(0, tg::sandbox::Status::Destroyed, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Destroyed);
	let existing = sandbox(data(0, tg::sandbox::Status::Destroyed, None));
	let merge = arg(data(0, tg::sandbox::Status::Destroying, None)).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Destroyed);
}

#[test]
fn the_usage_of_every_boot_is_summed() {
	let mut existing = sandbox(data(1, tg::sandbox::Status::Started, Some(usage(5))));
	existing.usages = BTreeMap::from([(1, usage(5))]);

	// A late report for an earlier boot keeps the later status but records its usage.
	let merge =
		arg(data(0, tg::sandbox::Status::Stopped, Some(usage(3)))).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Started);
	assert_eq!(total(&merge), 8);

	// A later report within a boot replaces the boot's usage.
	existing.data = merge.data;
	existing.usages = merge.usages;
	let merge =
		arg(data(1, tg::sandbox::Status::Stopped, Some(usage(7)))).merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Stopped);
	assert_eq!(total(&merge), 10);
}

#[test]
fn the_stopped_usage_of_every_boot_is_summed_and_only_grows() {
	let mut existing = sandbox(data(0, tg::sandbox::Status::Stopped, Some(usage(5))));
	existing.usages = BTreeMap::from([(0, usage(5))]);

	// A stopped usage report adds to the running usage and keeps the data.
	let mut report = arg(data(0, tg::sandbox::Status::Stopped, None));
	report.data = None;
	report.stopped_usage = Some(StoppedUsage {
		boot: 0,
		usage: usage(3),
	});
	let merge = report.merge_data(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Stopped);
	assert_eq!(total(&merge), 8);

	// A later report replaces the boot's stopped usage, and a stale report does not reduce it.
	existing.data = merge.data;
	existing.stopped_usages = merge.stopped_usages;
	report.stopped_usage = Some(StoppedUsage {
		boot: 0,
		usage: usage(4),
	});
	let merge = report.merge_data(Some(&existing));
	assert_eq!(total(&merge), 9);
	existing.data = merge.data;
	existing.stopped_usages = merge.stopped_usages;
	report.stopped_usage = Some(StoppedUsage {
		boot: 0,
		usage: usage(2),
	});
	let merge = report.merge_data(Some(&existing));
	assert_eq!(total(&merge), 9);
}

#[test]
fn a_reservation_of_the_next_boot_requires_the_sandbox_to_be_stopped_at_its_boot() {
	let mut reservation = arg(data(3, tg::sandbox::Status::Starting, None));
	reservation.stopped_boot = Some(2);

	// The sandbox is stopped at the boot.
	let existing = sandbox(data(2, tg::sandbox::Status::Stopped, None));
	assert!(reservation.validate_stopped_boot(Some(&existing)).is_ok());

	// Another start reserved the boot.
	let existing = sandbox(data(3, tg::sandbox::Status::Starting, None));
	assert!(reservation.validate_stopped_boot(Some(&existing)).is_err());

	// The boot is not stopped yet.
	let existing = sandbox(data(2, tg::sandbox::Status::Stopping, None));
	assert!(reservation.validate_stopped_boot(Some(&existing)).is_err());

	// The sandbox is stopped at an earlier boot or does not exist.
	let existing = sandbox(data(1, tg::sandbox::Status::Stopped, None));
	assert!(reservation.validate_stopped_boot(Some(&existing)).is_err());
	assert!(reservation.validate_stopped_boot(None).is_err());

	// A put that reserves no boot is not checked.
	reservation.stopped_boot = None;
	assert!(reservation.validate_stopped_boot(None).is_ok());
}

#[test]
fn the_lifecycle_writes_of_a_sandbox_converge_in_every_order() {
	let runner = tg::runner::Id::new();
	let starting = data(0, tg::sandbox::Status::Starting, None);
	let id = starting.data.id.clone();
	let writes = [
		tg::sandbox::Status::Starting,
		tg::sandbox::Status::Started,
		tg::sandbox::Status::Stopping,
		tg::sandbox::Status::Stopped,
	]
	.into_iter()
	.enumerate()
	.map(|(sequence, status)| {
		let mut data = starting.clone();
		data.data.status = status;
		let mut arg = arg(data);
		arg.principal = tg::Principal::Sandbox(id.clone());
		arg.runner = (sequence > 0).then(|| runner.clone());
		arg.sequence = sequence as u64;
		arg
	})
	.collect::<Vec<_>>();
	let sandbox = assert_every_order_converges(&writes).unwrap();
	assert_eq!(
		sandbox.data.unwrap().data.status,
		tg::sandbox::Status::Stopped
	);
	assert_eq!(sandbox.runner, Some(runner));
}

#[test]
fn a_late_write_of_an_earlier_boot_leaves_a_later_boot_and_its_runner() {
	let first = tg::runner::Id::new();
	let second = tg::runner::Id::new();
	let mut existing = sandbox(data(1, tg::sandbox::Status::Starting, None));
	existing.runner = Some(second.clone());
	for status in [tg::sandbox::Status::Started, tg::sandbox::Status::Destroyed] {
		let mut late = arg(data(0, status, None));
		late.runner = Some(first.clone());
		let merge = late.merge(Some(&existing), None).unwrap().unwrap();
		assert_eq!(
			merge.sandbox.data.unwrap().data.status,
			tg::sandbox::Status::Starting
		);
		assert_eq!(merge.sandbox.runner, Some(second.clone()));
	}
}

#[test]
fn the_first_end_of_a_boot_is_final() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let mut late = arg(data(0, tg::sandbox::Status::Destroyed, None));
	late.sequence = 5;
	let merge = late.merge(Some(&existing), None).unwrap().unwrap();
	assert_eq!(
		merge.sandbox.data.unwrap().data.status,
		tg::sandbox::Status::Stopped
	);
}

#[test]
fn the_permissions_of_a_write_whose_data_loses_apply() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let mut late = arg(data(0, tg::sandbox::Status::Starting, None));
	late.permissions = vec![crate::permission::put::Arg {
		created_at: 0,
		permissions: tg::authorization::permission::Set::Sandbox(
			tg::authorization::permission::sandbox::Set::default(),
		),
		resource: late.id.clone().into(),
		source: crate::permission::Source::Direct {
			expires_at: Some(1),
		},
		subject: tg::authorization::Subject::Public,
		time_to_touch: None,
		version: None,
	}];
	let merge = late.merge(Some(&existing), None).unwrap().unwrap();
	assert!(merge.permissions);
	assert_eq!(
		merge.sandbox.data.unwrap().data.status,
		tg::sandbox::Status::Stopped
	);
}

/// Apply the writes in every order, and assert that every order leaves the same record and rejects the same writes. Return the record of the last order.
fn assert_every_order_converges(args: &[Arg]) -> Option<super::super::Sandbox> {
	let mut expected = None;
	let mut last = None;
	for order in crate::tests::permutations(args.len()) {
		// Apply the writes in the order.
		let mut sandbox = None::<super::super::Sandbox>;
		let mut permissions = BTreeSet::new();
		let mut rejected = BTreeSet::new();
		for &index in &order {
			let arg = &args[index];
			let Ok(merge) = arg.merge(sandbox.as_ref(), None).unwrap() else {
				rejected.insert(index);
				continue;
			};
			if merge.permissions {
				for permission in &arg.permissions {
					permissions.insert(format!("{permission:?}"));
				}
			}
			sandbox = Some(merge.sandbox);
		}

		// Compare the record with the first order's record.
		let record = sandbox.as_ref().map(|sandbox| sandbox.serialize().unwrap());
		let outcome = (record, permissions, rejected);
		match &expected {
			None => {
				expected = Some((order, outcome));
			},
			Some((first, expected)) => {
				assert_eq!(
					&outcome, expected,
					"the order {order:?} produced a different record than the order {first:?}"
				);
			},
		}
		last = sandbox;
	}

	last
}

fn arg(data: tg::sandbox::get::Output) -> Arg {
	let id = data.data.id.clone();
	Arg {
		registration: None,
		account: None,
		created_at: 0,
		data: Some(data),
		id,
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Root,
		process: None,
		processes: None,
		runner: None,
		sequence: 0,
		stopped_boot: None,
		stopped_usage: None,
		touched_at: 0,
	}
}

fn data(
	boot: u64,
	status: tg::sandbox::Status,
	usage: Option<tg::sandbox::Usage>,
) -> tg::sandbox::get::Output {
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
		usage,
	};
	tg::sandbox::get::Output {
		data,
		location: None,
		tokens: tg::authorization::Tokens::default(),
	}
}

fn sandbox(data: tg::sandbox::get::Output) -> super::super::Sandbox {
	super::super::Sandbox {
		account: None,
		created_at: 0,
		data: Some(data),
		ends: BTreeMap::new(),
		location: None,
		reference_count: 0,
		runner: None,
		sequence: 0,
		set: super::super::Set::default(),
		stopped_usages: BTreeMap::new(),
		touched_at: 0,
		usages: BTreeMap::new(),
	}
}

fn status(merge: &DataMerge) -> tg::sandbox::Status {
	merge.data.as_ref().unwrap().data.status
}

fn total(merge: &DataMerge) -> u64 {
	merge
		.data
		.as_ref()
		.unwrap()
		.data
		.usage
		.as_ref()
		.unwrap()
		.cpu
		.consumption
}

fn usage(cpu: u64) -> tg::sandbox::Usage {
	tg::sandbox::Usage {
		cpu: tg::sandbox::ResourceUsage {
			consumption: cpu,
			reservation: 0,
		},
		memory: tg::sandbox::ResourceUsage::default(),
		storage: tg::sandbox::ResourceUsage::default(),
	}
}
