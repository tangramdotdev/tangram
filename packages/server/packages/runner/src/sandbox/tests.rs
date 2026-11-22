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
