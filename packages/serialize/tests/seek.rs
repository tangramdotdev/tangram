use {std::io::SeekFrom, tangram_serialize::Value};

#[test]
fn seek_from_preserves_variants_and_offsets() {
	let cases = [
		(SeekFrom::Current(i64::MIN), 0, Value::IVarint(i64::MIN)),
		(SeekFrom::Current(0), 0, Value::IVarint(0)),
		(SeekFrom::Current(i64::MAX), 0, Value::IVarint(i64::MAX)),
		(SeekFrom::End(i64::MIN), 1, Value::IVarint(i64::MIN)),
		(SeekFrom::End(0), 1, Value::IVarint(0)),
		(SeekFrom::End(i64::MAX), 1, Value::IVarint(i64::MAX)),
		(SeekFrom::Start(0), 2, Value::UVarint(0)),
		(SeekFrom::Start(u64::MAX), 2, Value::UVarint(u64::MAX)),
	];
	for (position, id, value) in cases {
		let value = tangram_serialize::value::Enum {
			id,
			value: Box::new(value),
		};
		let expected = tangram_serialize::to_vec(&Value::Enum(value)).unwrap();
		let bytes = tangram_serialize::to_vec(&position).unwrap();
		assert_eq!(bytes, expected);
		let decoded = tangram_serialize::from_slice::<SeekFrom>(&bytes).unwrap();
		assert_eq!(position, decoded);
	}
}

#[test]
fn seek_from_rejects_invalid_variants_and_payloads() {
	let cases = [
		(3, Value::IVarint(0)),
		(0, Value::Null),
		(1, Value::UVarint(0)),
		(2, Value::IVarint(-1)),
	];
	for (id, value) in cases {
		let value = tangram_serialize::value::Enum {
			id,
			value: Box::new(value),
		};
		let bytes = tangram_serialize::to_vec(&Value::Enum(value)).unwrap();
		assert!(tangram_serialize::from_slice::<SeekFrom>(&bytes).is_err());
	}
	let bytes = tangram_serialize::to_vec(&0_u64).unwrap();
	assert!(tangram_serialize::from_slice::<SeekFrom>(&bytes).is_err());
}
