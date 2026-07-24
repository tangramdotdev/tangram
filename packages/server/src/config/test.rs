use {super::*, std::collections::BTreeSet};

#[test]
fn default_roles() {
	let config = serde_json::from_str::<Config>("{}").unwrap();

	assert_eq!(config.roles, super::default_roles());
}

#[test]
fn process_spawn_connection_timeout_defaults() {
	let config = serde_json::from_str::<Config>(r#"{"process":{}}"#).unwrap();

	assert_eq!(
		config.process.spawn_connection_timeout,
		default_process_spawn_connection_timeout()
	);
}

#[test]
fn roles_are_an_exact_allowlist() {
	let config = serde_json::from_str::<Config>(r#"{"roles":["http"]}"#).unwrap();
	let expected = BTreeSet::from([Role::Http]);

	assert_eq!(config.roles, expected);
}

#[test]
fn sandbox_create_connection_timeout_defaults() {
	let config = serde_json::from_str::<Config>(r#"{"sandbox":{}}"#).unwrap();

	assert_eq!(
		config.sandbox.create_connection_timeout,
		Duration::from_secs(10)
	);
}
