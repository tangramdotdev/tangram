use std::time::Duration;

#[test]
fn duration() {
	let duration = Duration::new(60, 123);
	let bytes = tangram_serialize::to_vec(&duration).unwrap();
	let expected = tangram_serialize::to_vec(&(60_u64, 123_u32)).unwrap();
	assert_eq!(bytes, expected);
	let actual = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(duration, actual);
}

#[test]
fn duration_with_invalid_nanoseconds() {
	let bytes = tangram_serialize::to_vec(&(60_u64, 1_000_000_000_u32)).unwrap();
	let result = tangram_serialize::from_slice::<Duration>(&bytes);
	assert!(result.is_err());
}
