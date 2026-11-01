use super::*;

#[test]
fn a_late_write_does_not_overwrite_a_later_status() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let merge = arg(data(0, tg::sandbox::Status::Started, None)).merge(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Stopped);
}

#[test]
fn a_start_overwrites_the_previous_stop() {
	let existing = sandbox(data(0, tg::sandbox::Status::Stopped, None));
	let merge = arg(data(1, tg::sandbox::Status::Starting, None)).merge(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Starting);
}

#[test]
fn a_destroy_is_not_overwritten_by_a_later_boot() {
	let existing = sandbox(data(0, tg::sandbox::Status::Destroyed, None));
	let merge = arg(data(1, tg::sandbox::Status::Starting, None)).merge(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Destroyed);
}

#[test]
fn the_usage_of_every_boot_is_summed() {
	let mut existing = sandbox(data(1, tg::sandbox::Status::Started, Some(usage(5))));
	existing.usages = BTreeMap::from([(1, usage(5))]);

	// A late report for an earlier boot keeps the later status but records its usage.
	let merge = arg(data(0, tg::sandbox::Status::Stopped, Some(usage(3)))).merge(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Started);
	assert_eq!(total(&merge), 8);

	// A later report within a boot replaces the boot's usage.
	existing.data = merge.data;
	existing.usages = merge.usages;
	let merge = arg(data(1, tg::sandbox::Status::Stopped, Some(usage(7)))).merge(Some(&existing));
	assert_eq!(status(&merge), tg::sandbox::Status::Stopped);
	assert_eq!(total(&merge), 10);
}

fn arg(data: tg::sandbox::get::Output) -> Arg {
	let id = data.data.id.clone();
	Arg {
		account: None,
		created_at: 0,
		data: Some(data),
		id,
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Root,
		processes: None,
		runner: None,
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
		hostname: None,
		id: tg::sandbox::Id::new(),
		memory: None,
		mounts: Vec::new(),
		network: None,
		owner: tg::Principal::Root,
		status,
		storage: None,
		ttl: None,
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
		location: None,
		reference_count: 0,
		runner: None,
		set: super::super::Set::default(),
		touched_at: 0,
		usages: BTreeMap::new(),
	}
}

fn status(merge: &Merge) -> tg::sandbox::Status {
	merge.data.as_ref().unwrap().data.status
}

fn total(merge: &Merge) -> u64 {
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
