use {std::path::PathBuf, tangram_client::prelude::*};

#[derive(serde::Deserialize)]
struct Fixture {
	directory: PathBuf,
	file: tg::file::Id,
	file_path: PathBuf,
	wrapper: PathBuf,
	wrapper_without_dependency_tokens: PathBuf,
}

#[tokio::test]
#[ignore = "requires the checkout/client_references.nu fixture"]
async fn unrender() {
	let fixture = std::env::var("TANGRAM_TEST_CHECKOUT").unwrap();
	let fixture: Fixture = serde_json::from_str(&fixture).unwrap();
	let prefix = fixture.directory.parent().unwrap().to_str().unwrap();
	let path = fixture.directory.join("lib/libexample.so");
	let path = path.to_str().unwrap();

	// A leaf token cannot authorize the directory retained by unrender.
	let template = tg::Template::unrender(prefix, path).unwrap();
	let directory = template.artifacts().next().unwrap();
	assert!(directory.is_directory());
	let client = tg::Client::with_env(tg::Arg::default()).unwrap();
	assert!(directory.load_with_handle(&client).await.is_err());
	let metadata = tg::file::checkout::read(path).unwrap();
	assert_eq!(
		metadata.token.as_ref().unwrap().body.resource,
		fixture.file.clone().into()
	);
	assert!(load(metadata, prefix, path).await.is_err());

	// Exercise dependency tokens independently when the provider retains them.
	let mut metadata = tg::file::checkout::read(&fixture.wrapper).unwrap();
	if metadata
		.dependencies
		.iter()
		.flatten()
		.any(|reference| reference.options().tokens.local().is_some())
	{
		metadata.token = None;
	}
	let file = load(metadata, prefix, path).await.unwrap();
	assert_eq!(file.id(), fixture.file);

	// Fall back to the wrapper's token when dependency tokens are absent.
	let metadata = tg::file::checkout::read(&fixture.wrapper_without_dependency_tokens).unwrap();
	assert!(
		metadata
			.dependencies
			.iter()
			.flatten()
			.all(|reference| reference.options().tokens.is_empty())
	);
	let file = load(metadata, prefix, path).await.unwrap();
	assert_eq!(file.id(), fixture.file);

	// A file-rooted path uses the file's own token.
	let path = fixture.file_path.to_str().unwrap();
	let metadata = tg::file::checkout::read(&fixture.file_path).unwrap();
	let file = load(metadata, prefix, path).await.unwrap();
	assert_eq!(file.id(), fixture.file);
}

async fn load(
	metadata: tg::file::checkout::Output,
	prefix: &str,
	path: &str,
) -> tg::Result<tg::File> {
	// Attach the recovered tokens to the unrendered artifacts.
	let template = tg::Template::unrender(prefix, path)?;
	let file_tokens = tg::authorization::Tokens::with_local(metadata.token);
	for artifact in template.artifacts() {
		let mut tokens = metadata
			.dependencies
			.iter()
			.flatten()
			.find(|reference| reference.node() == &tg::reference::Node::Id(artifact.id().into()))
			.map(|reference| reference.options().tokens.clone())
			.unwrap_or_default();
		tokens.inherit(&file_tokens);
		artifact.state().set_tokens(tokens);
	}

	// Resolve the path and load the file in a fresh client.
	let client = tg::Client::with_env(tg::Arg::default())?;
	let file = match template.components() {
		[tg::template::Component::Artifact(artifact)] => artifact.clone(),
		[
			tg::template::Component::Artifact(artifact),
			tg::template::Component::String(path),
		] => {
			let directory = artifact
				.clone()
				.try_unwrap_directory()
				.map_err(|error| tg::error!(!error, "expected a directory"))?;
			directory
				.get_with_handle(&client, path.trim_start_matches('/'))
				.await?
		},
		_ => return Err(tg::error!("expected an artifact path")),
	};
	let file = file
		.try_unwrap_file()
		.map_err(|error| tg::error!(!error, "expected a file"))?;
	file.load_with_handle(&client).await?;

	Ok(file)
}
