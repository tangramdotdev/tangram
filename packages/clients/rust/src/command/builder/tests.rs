use {super::Builder, crate::prelude::*};

#[test]
fn spawn_arg_preserves_executable_and_stdin_referents() {
	let location = tg::Location::Remote(tg::location::Remote {
		name: "remote".to_owned(),
		region: None,
	});
	let file_id = tg::file::Id::new(b"executable");
	let artifact = tg::Artifact::with_id(file_id.clone().into());
	let artifact_tokens = tokens(file_id.into(), &location);
	artifact.state().set_location(Some(location.clone()));
	artifact.state().set_tokens(artifact_tokens.clone());
	let blob_id = tg::blob::Id::new(b"stdin");
	let blob = tg::Blob::with_id(blob_id.clone());
	let blob_tokens = tokens(blob_id.into(), &location);
	blob.state().set_location(Some(location.clone()));
	blob.state().set_tokens(blob_tokens.clone());
	let executable = tg::command::Executable {
		artifact: Some(artifact),
		path: Some("bin/example".into()),
	};
	let arg = Builder::new()
		.executable(executable)
		.stdin(blob)
		.build_spawn_arg()
		.unwrap();

	assert_eq!(arg.executable.options.location, Some(location.clone()));
	assert_eq!(arg.executable.options.tokens, artifact_tokens);
	let stdin = arg.stdin.as_ref().unwrap();
	assert_eq!(stdin.options.location, Some(location));
	assert_eq!(stdin.options.tokens, blob_tokens);

	let builder = Builder::try_with_spawn_arg(arg).unwrap();
	let artifact = builder.executable.unwrap().artifact.unwrap();
	assert_eq!(artifact.state().tokens(), artifact_tokens);
	let stdin = builder.stdin.unwrap();
	assert_eq!(stdin.state().tokens(), blob_tokens);
}

fn tokens(resource: tg::Id, location: &tg::Location) -> tg::authorization::Tokens {
	let token = tg::authorization::Token {
		body: tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource,
		},
		metadata: tg::authorization::Metadata {
			algorithm: tg::authorization::Algorithm::Ed25519,
			key: "test".to_owned(),
		},
		signature: Vec::new(),
	};
	let mut tokens = tg::authorization::Tokens::default();
	tokens.set(location.clone(), token);

	tokens
}
