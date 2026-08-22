use super::*;

#[test]
fn validate_id_and_specifier_rejects_an_existing_id_at_another_specifier() {
	let id = tg::Id::from(tg::group::Id::new());
	let incoming_specifier: tg::Specifier = "incoming".parse().unwrap();
	let existing_specifier: tg::Specifier = "existing".parse().unwrap();
	let mut stored_specifiers = Specifiers::default();
	stored_specifiers.insert(id.clone(), existing_specifier.clone());

	let by_id = stored_specifiers.specifiers_by_id.get(&id);
	let by_specifier = stored_specifiers.ids_by_specifier.get(&incoming_specifier);
	let error = Session::sync_get_database_validate_id_and_specifier(
		&id,
		&incoming_specifier,
		by_id,
		by_specifier,
	)
	.unwrap_err();

	assert!(error.to_string().contains("the id is already in use"));
	assert_eq!(
		stored_specifiers.specifiers_by_id.get(&id),
		Some(&existing_specifier)
	);
	assert_eq!(
		stored_specifiers.ids_by_specifier.get(&existing_specifier),
		Some(&id)
	);
	assert!(
		!stored_specifiers
			.ids_by_specifier
			.contains_key(&incoming_specifier)
	);
}
