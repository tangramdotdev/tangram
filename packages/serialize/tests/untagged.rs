#[derive(Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
#[tangram_serialize(untagged)]
enum Value {
	Boolean(bool),
	String(String),
}

// An untagged enum delegates to its selected variant's wire representation and backtracks on input.
#[test]
fn roundtrip() {
	let value = Value::Boolean(true);
	let bytes = tangram_serialize::to_vec(&value).unwrap();
	assert_eq!(bytes, tangram_serialize::to_vec(&true).unwrap());
	let actual = tangram_serialize::from_slice::<Value>(&bytes).unwrap();
	assert_eq!(actual, value);

	let string = "value".to_owned();
	let value = Value::String(string.clone());
	let bytes = tangram_serialize::to_vec(&value).unwrap();
	assert_eq!(bytes, tangram_serialize::to_vec(&string).unwrap());
	let actual = tangram_serialize::from_slice::<Value>(&bytes).unwrap();
	assert_eq!(actual, value);
}
