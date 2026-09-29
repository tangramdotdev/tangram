use {super::new_index, std::str::FromStr as _, tangram_client::prelude::*};

#[tokio::test]
async fn try_get_ancestors() {
	let (_dir, index) = new_index();
	let missing = tg::user::Id::new();
	assert!(
		index
			.try_get_ancestors(&missing.into())
			.await
			.unwrap()
			.is_none()
	);

	let organization = tg::organization::Id::new();
	let parent = tg::group::Id::new();
	let child = tg::group::Id::new();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
				billing_ready: None,
				id: organization.clone(),
				specifier: tg::Specifier::from_str("organization").unwrap(),
			}),
			tangram_index::batch::Item::PutGroup(tangram_index::group::put::Arg {
				id: parent.clone(),
				parent: Some(organization.clone().into()),
				specifier: tg::Specifier::from_str("organization/parent").unwrap(),
			}),
			tangram_index::batch::Item::PutGroup(tangram_index::group::put::Arg {
				id: child.clone(),
				parent: Some(parent.clone().into()),
				specifier: tg::Specifier::from_str("organization/parent/child").unwrap(),
			}),
		],
	};
	index.batch(arg).await.unwrap();

	let ancestors = index
		.try_get_ancestors(&child.clone().into())
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		ancestors,
		vec![child.into(), parent.into(), organization.into()]
	);
}
