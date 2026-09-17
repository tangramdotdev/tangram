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

#[test]
fn input_tokens_are_not_pooled() {
	let location = tg::Location::Local(tg::location::Local::default());
	let first = tg::Blob::with_id(tg::blob::Id::new(b"first"));
	let second = tg::Blob::with_id(tg::blob::Id::new(b"second"));
	let first_tokens = tokens(first.id().into(), &location);
	let second_tokens = tokens(second.id().into(), &location);
	first.state().set_tokens(first_tokens.clone());
	second.state().set_tokens(second_tokens.clone());
	let value = tg::Value::Array(vec![
		first.clone().into(),
		second.clone().into(),
		first.clone().into(),
	]);
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let command = Builder::new()
		.executable(executable)
		.arg(tg::command::Value::Value(value))
		.build()
		.unwrap();
	assert!(command.state().tokens().is_empty());
	let object = command.state().object().unwrap();
	let _data = object.to_data().without_location_and_tokens();
	assert_eq!(first.state().tokens(), first_tokens);
	assert_eq!(second.state().tokens(), second_tokens);
}

fn tokens(resource: tg::Id, location: &tg::Location) -> tg::Tokens {
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
	let mut tokens = tg::Tokens::default();
	tokens.insert_authorization(location.clone(), token);

	tokens
}
