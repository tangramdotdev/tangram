#[derive(
	Debug, Default, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
#[allow(clippy::option_option)]
struct Value {
	#[tangram_serialize(
		default,
		id = 0,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	option: Option<Option<u64>>,
}

#[test]
fn unwrap_or_skip() {
	for option in [None, Some(None), Some(Some(42))] {
		let expected = Value { option };
		let bytes = tangram_serialize::to_vec(&expected).unwrap();
		let actual = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(expected, actual);
	}
}
