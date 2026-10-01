use {super::new_index, crate::Index, tangram_client::prelude::*};

#[tokio::test]
async fn target_versions_clear_subject_permissions() {
	let (_directory, index) = new_index();
	let id = tg::tag::Id::new();
	let first: tg::object::Id = tg::file::Id::new(b"first").into();
	let second: tg::object::Id = tg::file::Id::new(b"second").into();
	let subject = tg::authorization::Subject::Tag(id.clone());
	let mut tag = tangram_index::tag::put::Arg {
		account: None,
		id: id.clone(),
		name: "test".into(),
		parent: None,
		permissions: Vec::new(),
		specifier: "test".parse().unwrap(),
		target: tg::Either::Left(first.clone()),
		version: "z".into(),
	};
	index.put_tags(std::slice::from_ref(&tag)).await.unwrap();
	let permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: None,
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::NODE,
		),
		resource: tg::Id::from(first.clone()),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: subject.clone(),
		time_to_touch: None,
		version: None,
	};
	index.put_permissions(&[permission]).await.unwrap();
	let has_permissions = || {
		let transaction = index.env.read_txn().unwrap();
		!Index::get_resource_permission_entries_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&first.clone().into(),
			&subject,
		)
		.unwrap()
		.is_empty()
	};

	// Repeating the current target preserves the captured permissions.
	index.put_tags(std::slice::from_ref(&tag)).await.unwrap();
	assert!(has_permissions());

	// A target change clears permissions regardless of version ordering.
	tag.target = tg::Either::Left(second);
	tag.version = "a".into();
	index.put_tags(std::slice::from_ref(&tag)).await.unwrap();
	assert!(!has_permissions());

	// A conflicting target must have a new version.
	tag.target = tg::Either::Left(first);
	assert!(index.put_tags(&[tag]).await.is_err());
}
