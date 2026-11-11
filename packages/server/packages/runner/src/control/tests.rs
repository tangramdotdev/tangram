use {super::stopped_sandbox_action, tangram_client::prelude::*};

fn indexed(
	runner: Option<&tg::runner::Id>,
	status: Option<tg::sandbox::Status>,
) -> tangram_index::sandbox::Sandbox {
	let id = tg::sandbox::Id::new();
	let data = status.map(|status| tg::sandbox::get::Output {
		data: tg::sandbox::Data {
			boot: 0,
			cpu: None,
			creator: tg::Principal::Root,
			error: None,
			expires_at: None,
			hostname: None,
			id: id.clone(),
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
		},
		location: None,
		tokens: tg::authorization::Tokens::default(),
	});
	tangram_index::sandbox::Sandbox {
		account: None,
		created_at: 0,
		data,
		location: None,
		reference_count: 0,
		runner: runner.cloned(),
		set: tangram_index::sandbox::Set::default(),
		stopped_usages: std::collections::BTreeMap::new(),
		touched_at: 0,
		usages: std::collections::BTreeMap::new(),
	}
}

#[test]
fn a_runner_removes_the_directory_of_a_destroyed_sandbox_or_another_runners_sandbox() {
	let runner = tg::runner::Id::new();
	let other = tg::runner::Id::new();
	let remove = |removal| tg::runner::control::StoppedSandboxAction::Remove(removal);

	// A sandbox that the index does not have is kept, because the index may lag.
	assert_eq!(
		stopped_sandbox_action(None, &runner),
		tg::runner::control::StoppedSandboxAction::Keep
	);

	// A sandbox that the index records as held by another runner is removed.
	let sandbox = indexed(Some(&other), Some(tg::sandbox::Status::Stopped));
	assert_eq!(
		stopped_sandbox_action(Some(&sandbox), &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::OtherRunner)
	);

	// A destroyed or destroying sandbox is removed, whichever runner the index records.
	let sandbox = indexed(Some(&other), Some(tg::sandbox::Status::Destroyed));
	assert_eq!(
		stopped_sandbox_action(Some(&sandbox), &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::Destroyed)
	);
	let sandbox = indexed(Some(&runner), Some(tg::sandbox::Status::Destroying));
	assert_eq!(
		stopped_sandbox_action(Some(&sandbox), &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::Destroying)
	);

	// This runner's stopped sandbox, and a sandbox that the index records without a runner, are kept.
	for sandbox in [
		indexed(Some(&runner), Some(tg::sandbox::Status::Stopped)),
		indexed(None, Some(tg::sandbox::Status::Stopped)),
		indexed(Some(&runner), None),
	] {
		assert_eq!(
			stopped_sandbox_action(Some(&sandbox), &runner),
			tg::runner::control::StoppedSandboxAction::Keep
		);
	}
}
