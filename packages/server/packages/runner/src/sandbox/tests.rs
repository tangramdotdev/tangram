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
