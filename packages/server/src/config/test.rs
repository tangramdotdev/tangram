use {super::*, std::collections::BTreeSet};

#[test]
fn default_roles() {
	let config = serde_json::from_str::<Config>("{}").unwrap();

	assert_eq!(config.roles, super::default_roles());
}

#[test]
fn roles_are_an_exact_allowlist() {
	let config = serde_json::from_str::<Config>(r#"{"roles":["http"]}"#).unwrap();
	let expected = BTreeSet::from([Role::Http]);

	assert_eq!(config.roles, expected);
}
