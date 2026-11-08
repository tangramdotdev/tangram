use super::Options;

#[test]
fn watch_default() {
	for (destructive, watch, expected) in [
		(false, None, true),
		(false, Some(false), false),
		(false, Some(true), true),
		(true, None, false),
		(true, Some(false), false),
		(true, Some(true), true),
	] {
		let options = Options {
			destructive,
			watch,
			..Default::default()
		};
		assert_eq!(options.watch(), expected);
	}
}

#[test]
fn watch_serialization() {
	for watch in [None, Some(false), Some(true)] {
		let options = Options {
			watch,
			..Default::default()
		};
		let value = serde_json::to_value(&options).unwrap();
		assert_eq!(
			value.get("watch"),
			watch.map(serde_json::Value::Bool).as_ref()
		);
		let options: Options = serde_json::from_value(value).unwrap();
		assert_eq!(options.watch, watch);
	}
	for (value, watch) in [("false", false), ("true", true)] {
		let value = serde_json::json!({"watch": value});
		let options: Options = serde_json::from_value(value).unwrap();
		assert_eq!(options.watch, Some(watch));
	}
}
