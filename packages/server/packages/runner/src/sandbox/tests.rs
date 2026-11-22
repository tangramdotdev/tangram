use super::*;

#[test]
fn stopped_storage_usage_charges_the_size_over_time_without_a_reservation() {
	// A mebibyte for one second is a thousand mebibyte-milliseconds.
	let usage = stopped_storage_usage(1_048_576, 1);
	assert_eq!(usage.storage.consumption, 1000);
	assert_eq!(usage.storage.reservation, 0);
	assert_eq!(usage.cpu, tg::sandbox::ResourceUsage::default());
	assert_eq!(usage.memory, tg::sandbox::ResourceUsage::default());

	// A partial mebibyte rounds up, and no time charges nothing.
	assert_eq!(stopped_storage_usage(1, 1).storage.consumption, 1);
	assert_eq!(stopped_storage_usage(1_048_576, 0).storage.consumption, 0);

	// A clock that moved backward charges nothing.
	assert_eq!(stopped_storage_usage(1_048_576, -5).storage.consumption, 0);
}

#[test]
fn a_request_that_says_that_the_index_records_the_end_gives_the_boot_up() {
	// A stop or a destroy that says that the index records the boot's end gives the boot up whatever the boot's phase, after which the boot numbers nothing more and takes no end.
	let stop =
		tg::sandbox::control::ServerRequestArg::Stop(tg::sandbox::control::StopServerRequestArg {
			ended: true,
		});
	let destroy = tg::sandbox::control::ServerRequestArg::Destroy(
		tg::sandbox::control::DestroyServerRequestArg {
			ended: true,
			error: None,
		},
	);
	for (arg, expected) in [(stop, Teardown::Stop), (destroy, Teardown::Destroy)] {
		let location = tg::Location::Local(tg::location::Local::default());
		let boot = Boot::new(0, tg::sandbox::Id::new(), location, None, true, 1);
		let kind = ended_teardown(&arg).unwrap();
		assert_eq!(kind, expected);
		assert!(boot.give_up(kind));
		assert_eq!(boot.next(), None);
		assert_eq!(boot.end(), None);
	}

	// A stop or a destroy that does not say so gives nothing up.
	let stop =
		tg::sandbox::control::ServerRequestArg::Stop(tg::sandbox::control::StopServerRequestArg {
			ended: false,
		});
	assert_eq!(ended_teardown(&stop), None);
}

#[test]
fn a_boot_refuses_a_spawn_for_another_boot_and_accepts_a_spawn_for_itself() {
	let destroy = std::sync::Mutex::new(None);
	let phases = [
		Phase::Ended,
		Phase::Running { reported: true },
		Phase::Stopping { destroy: &destroy },
	];
	for phase in phases {
		// A spawn for an earlier or a later boot of the sandbox is refused, so that the sandbox's scheduler ends the process.
		assert!(matches!(
			answer_spawn(phase, 1, Some(0), true),
			SpawnAnswer::Refuse
		));
		assert!(matches!(
			answer_spawn(phase, 1, Some(2), true),
			SpawnAnswer::Refuse
		));

		// A spawn for the boot, or a spawn without a boot from this server, is accepted.
		assert!(matches!(
			answer_spawn(phase, 1, Some(1), true),
			SpawnAnswer::Accept
		));
		assert!(matches!(
			answer_spawn(phase, 1, None, false),
			SpawnAnswer::Accept
		));

		// A server's spawn without a boot is sent again.
		assert!(matches!(
			answer_spawn(phase, 1, None, true),
			SpawnAnswer::Retry(tg::control::Retry::NotHolder)
		));
	}

	// A server's spawn without a boot waits for the scheduler to acknowledge a boot on the shortcut path.
	assert!(matches!(
		answer_spawn(Phase::Running { reported: false }, 1, None, true),
		SpawnAnswer::Retry(tg::control::Retry::NotReady)
	));
}

#[test]
fn a_failed_boot_that_the_runner_gave_up_ends_as_the_scheduler_decided() {
	// A failed boot that the runner gave up with a destroy is destroyed, even if its directory remains, because the scheduler destroyed the sandbox.
	assert_eq!(
		failed_teardown(Some(Teardown::Destroy), true, 1, true),
		Teardown::Destroy
	);

	// A failed boot that the runner gave up with a stop is stopped, because the scheduler keeps the sandbox's directory.
	assert_eq!(
		failed_teardown(Some(Teardown::Stop), false, 0, false),
		Teardown::Stop
	);

	// A failed boot that the runner holds is stopped if its directory remains for a later start, and destroyed otherwise.
	assert_eq!(failed_teardown(None, true, 0, true), Teardown::Stop);
	assert_eq!(failed_teardown(None, false, 1, true), Teardown::Stop);
	assert_eq!(failed_teardown(None, false, 0, true), Teardown::Destroy);
	assert_eq!(failed_teardown(None, false, 1, false), Teardown::Destroy);
}
