use super::*;

#[test]
fn optional_sandbox_roundtrips() {
	for sandbox in [None, Some(tg::sandbox::Id::new())] {
		let value = serde_json::json!({
			"command": tg::command::Id::new(b"command"),
			"created_at": 0,
			"host": "test",
			"sandbox": sandbox,
			"status": "finished",
		});
		let data: Data = serde_json::from_value(value).unwrap();
		let json = serde_json::to_value(&data).unwrap();
		assert_eq!(json.get("sandbox").is_some(), sandbox.is_some());
		let parsed: Data = serde_json::from_value(json.clone()).unwrap();
		assert_eq!(parsed.sandbox, sandbox);
		let bytes = tangram_serialize::to_vec(&data).unwrap();
		let parsed: Data = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(parsed.sandbox, sandbox);
		assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
		let state = tg::process::State::try_from(data).unwrap();
		assert_eq!(state.sandbox, sandbox);
	}
}
