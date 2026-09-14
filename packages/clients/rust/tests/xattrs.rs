use {std::path::PathBuf, tangram_client::prelude::*};

#[derive(serde::Deserialize)]
struct Fixture {
	copy: PathBuf,
	dependency: tg::artifact::Id,
	id: tg::artifact::Id,
	path: PathBuf,
}

#[tokio::test]
#[ignore = "requires the checkout/client_xattrs.nu fixture"]
async fn checkout_metadata() {
	let fixture = std::env::var("TANGRAM_TEST_XATTRS").unwrap();
	let fixture: Fixture = serde_json::from_str(&fixture).unwrap();
	let metadata = tg::file::xattrs::read(&fixture.path).unwrap();
	let token = metadata.token.as_ref().unwrap();
	assert_eq!(token.body.resource, fixture.id.clone().into());
	let dependencies = metadata.dependencies.as_deref().unwrap();
	assert_eq!(dependencies.len(), 1);
	assert_eq!(
		dependencies[0].node(),
		&tg::reference::Node::Id(fixture.dependency.clone().into())
	);

	// The recovered token authorizes an otherwise private object.
	let client = tg::Client::with_env(tg::Arg::default()).unwrap();
	let artifact = tg::Artifact::with_id(fixture.id.clone());
	assert!(artifact.load_with_handle(&client).await.is_err());
	let referent =
		tg::Referent::with_node_and_local_tokens(fixture.id.clone(), Some(token.clone()));
	tg::Artifact::with_referent(referent)
		.load_with_handle(&client)
		.await
		.unwrap();

	// Write the public schema onto a fresh file.
	let contents = std::fs::read(&fixture.path).unwrap();
	std::fs::write(&fixture.copy, contents).unwrap();
	let arg = tg::file::xattrs::Arg {
		dependencies: Some(dependencies),
		required: &[],
		token: Some(token),
	};
	tg::file::xattrs::write(&fixture.copy, arg, tg::file::xattrs::Options::default()).unwrap();
	assert_eq!(tg::file::xattrs::read(&fixture.copy).unwrap(), metadata);

	// A wrapper token remains sufficient when dependency tokens are omitted for space.
	let dependencies = dependencies
		.iter()
		.cloned()
		.map(tg::Reference::without_tokens)
		.collect::<Vec<_>>();
	let arg = tg::file::xattrs::Arg {
		dependencies: Some(&dependencies),
		required: &[],
		token: Some(token),
	};
	tg::file::xattrs::write(&fixture.copy, arg, tg::file::xattrs::Options::default()).unwrap();
	let metadata = tg::file::xattrs::read(&fixture.copy).unwrap();
	assert!(
		metadata
			.dependencies
			.unwrap()
			.iter()
			.all(|reference| reference.options().tokens.is_empty())
	);
	let client = tg::Client::with_env(tg::Arg::default()).unwrap();
	assert!(
		tg::Artifact::with_id(fixture.dependency)
			.load_with_handle(&client)
			.await
			.is_err()
	);
	let arg = tg::checkin::Arg {
		options: tg::checkin::Options {
			root: true,
			..Default::default()
		},
		path: fixture.copy,
		updates: Vec::new(),
	};
	let output = tg::checkin::checkin_with_handle(&client, arg)
		.await
		.unwrap();
	assert_eq!(output.artifact.node, fixture.id);
}
