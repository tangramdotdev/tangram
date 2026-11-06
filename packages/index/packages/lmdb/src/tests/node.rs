use {super::new_index, tangram_client::prelude::*};

#[tokio::test]
async fn deleting_replaced_nodes_preserves_the_replacement() {
	let group = tg::group::Id::new();
	let organization = tg::organization::Id::new();
	let tag = tg::tag::Id::new();
	let user = tg::user::Id::new();
	let specifier: tg::Specifier = "test".parse().unwrap();
	let nodes = vec![
		(
			tangram_index::Item::PutGroup(Box::new(tangram_index::group::put::Arg {
				id: group.clone(),
				parent: None,
				specifier: specifier.clone(),
			})),
			tangram_index::Item::DeleteGroup(Box::new(tangram_index::group::delete::Arg {
				id: group,
			})),
		),
		(
			tangram_index::Item::PutOrganization(Box::new(tangram_index::organization::put::Arg {
				billing_ready: None,
				id: organization.clone(),
				specifier: specifier.clone(),
			})),
			tangram_index::Item::DeleteOrganization(Box::new(
				tangram_index::organization::delete::Arg { id: organization },
			)),
		),
		(
			tangram_index::Item::PutTag(Box::new(tangram_index::tag::put::Arg {
				account: None,
				captures: tangram_index::permission::capture::Captures::default(),
				id: tag.clone(),
				name: "test".to_owned(),
				parent: None,
				specifier: specifier.clone(),
				target: tg::Either::Left(tg::file::Id::new(b"target").into()),
				touched_at: 0,
				version: "test".to_owned(),
			})),
			tangram_index::Item::DeleteTag(Box::new(tangram_index::tag::delete::Arg { id: tag })),
		),
		(
			tangram_index::Item::PutUser(Box::new(tangram_index::user::put::Arg {
				billing_ready: None,
				id: user.clone(),
				specifier: specifier.clone(),
			})),
			tangram_index::Item::DeleteUser(Box::new(tangram_index::user::delete::Arg {
				id: user,
			})),
		),
	];
	for (put, delete) in nodes {
		let (_directory, index) = new_index();
		let replacement = tg::group::Id::new();
		let batch = tangram_index::write::Arg {
			items: vec![
				put,
				tangram_index::Item::PutGroup(Box::new(tangram_index::group::put::Arg {
					id: replacement.clone(),
					parent: None,
					specifier: specifier.clone(),
				})),
				delete,
			],
		};
		index.write(batch).await.unwrap().unwrap();
		let ids = index
			.try_get_ids_for_specifiers(std::slice::from_ref(&specifier))
			.await
			.unwrap();
		assert_eq!(ids, vec![Some(replacement.into())]);
	}
}
