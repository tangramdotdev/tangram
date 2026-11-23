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
fn a_request_that_says_that_the_index_records_the_end_abandons_the_boot() {
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
		assert!(boot.abandon(kind));
		assert_eq!(boot.next(), None);
	}

	// A stop or a destroy that does not say so gives nothing up.
	let stop =
		tg::sandbox::control::ServerRequestArg::Stop(tg::sandbox::control::StopServerRequestArg {
			ended: false,
		});
	assert_eq!(ended_teardown(&stop), None);
}

#[test]
fn a_failed_boot_that_the_runner_abandoned_ends_as_the_scheduler_decided() {
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

#[test]
fn a_spawn_for_a_later_boot_is_sent_again_and_one_for_an_earlier_boot_is_refused() {
	// A spawn for a later boot is sent again, so that it reaches the later boot, whatever the phase of this boot.
	for phase in [Phase::Ended, Phase::Started { reported: true }] {
		assert_eq!(
			spawn_answer(phase, true, Some(2), 1),
			SpawnAnswer::Retry(tg::control::Retry::NotHolder)
		);
	}

	// A spawn for an earlier boot is refused, because the earlier boot is over.
	assert_eq!(
		spawn_answer(Phase::Started { reported: true }, true, Some(0), 1),
		SpawnAnswer::Earlier
	);

	// A spawn for this boot is accepted while the boot runs, and refused once it is over.
	assert_eq!(
		spawn_answer(Phase::Started { reported: true }, true, Some(1), 1),
		SpawnAnswer::Accept
	);
	assert_eq!(
		spawn_answer(Phase::Ended, true, Some(1), 1),
		SpawnAnswer::Over
	);

	// A server's spawn without a boot is sent again.
	assert_eq!(
		spawn_answer(Phase::Started { reported: false }, true, None, 0),
		SpawnAnswer::Retry(tg::control::Retry::NotReady)
	);
	assert_eq!(
		spawn_answer(Phase::Started { reported: false }, false, None, 0),
		SpawnAnswer::Accept
	);
}
