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
		ends: std::collections::BTreeMap::new(),
		location: None,
		reference_count: 0,
		runner: runner.cloned(),
		scheduler: None,
		sequence: 0,
		set: tangram_index::sandbox::Set::default(),
		stopped_usages: std::collections::BTreeMap::new(),
		touched_at: 0,
		usages: std::collections::BTreeMap::new(),
	}
}

#[test]
fn a_runner_keeps_only_the_directories_of_its_sandboxes_that_can_start_again() {
	let runner = tg::runner::Id::new();
	let other = tg::runner::Id::new();
	let remove = |removal| tg::runner::control::StoppedSandboxAction::Remove(removal);

	// A sandbox that the index does not record after indexing was awaited is removed without a report.
	assert_eq!(
		stopped_sandbox_action(None, &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::Unrecorded)
	);

	// A sandbox that the index records on another runner is removed without a report, whatever its status.
	for status in [tg::sandbox::Status::Stopped, tg::sandbox::Status::Destroyed] {
		let sandbox = indexed(Some(&other), Some(status));
		assert_eq!(
			stopped_sandbox_action(Some(&sandbox), &runner),
			remove(tg::runner::control::StoppedSandboxRemoval::Unrecorded)
		);
	}

	// A destroyed or destroying sandbox of this runner is removed with a report.
	let sandbox = indexed(Some(&runner), Some(tg::sandbox::Status::Destroyed));
	assert_eq!(
		stopped_sandbox_action(Some(&sandbox), &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::Destroyed)
	);
	let sandbox = indexed(Some(&runner), Some(tg::sandbox::Status::Destroying));
	assert_eq!(
		stopped_sandbox_action(Some(&sandbox), &runner),
		remove(tg::runner::control::StoppedSandboxRemoval::Destroying)
	);

	// This runner's stopped, starting, or running sandbox, and a sandbox that the index records without a runner, are kept.
	for sandbox in [
		indexed(Some(&runner), Some(tg::sandbox::Status::Stopped)),
		indexed(Some(&runner), Some(tg::sandbox::Status::Started)),
		indexed(None, Some(tg::sandbox::Status::Stopped)),
		indexed(Some(&runner), None),
	] {
		assert_eq!(
			stopped_sandbox_action(Some(&sandbox), &runner),
			tg::runner::control::StoppedSandboxAction::Keep
		);
	}
}
