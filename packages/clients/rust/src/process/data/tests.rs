use super::*;

#[test]
fn optional_sandbox_roundtrips() {
	for sandbox in [None, Some(tg::sandbox::Id::new())] {
		let value = serde_json::json!({
			"command": tg::command::Id::new(b"command"),
			"created_at": 0,
			"host": "test",
			"sandbox": sandbox,
			"status": "finished",
		});
		let data: Data = serde_json::from_value(value).unwrap();
		let json = serde_json::to_value(&data).unwrap();
		assert_eq!(json.get("sandbox").is_some(), sandbox.is_some());
		let parsed: Data = serde_json::from_value(json.clone()).unwrap();
		assert_eq!(parsed.sandbox, sandbox);
		let bytes = tangram_serialize::to_vec(&data).unwrap();
		let parsed: Data = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(parsed.sandbox, sandbox);
		assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
		let state = tg::process::State::try_from(data).unwrap();
		assert_eq!(state.sandbox, sandbox);
	}
}

#[test]
fn outcome_referents_roundtrip() {
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let sync = sync_token(&key);
	let tokens = tg::authorization::Tokens::with_authorization([sync]);
	let file = tg::object::Id::from(tg::file::Id::new(b"file"));
	let value = tg::value::Data::Array(vec![
		tg::value::Data::Object(tg::Referent::with_node(file.clone())),
		tg::value::Data::Object(tg::Referent::with_node(file.clone())),
	]);
	for error in [
		tg::Either::Left(Box::default()),
		tg::Either::Right(tg::error::Id::new(b"error")),
	] {
		let mut data = process_data();
		data.error = Some(tg::Referent::with_node_and_tokens(error, tokens.clone()));
		data.output = Some(tg::Referent::with_node_and_tokens(
			value.clone(),
			tokens.clone(),
		));
		let json = serde_json::to_value(&data).unwrap();
		let parsed: Data = serde_json::from_value(json.clone()).unwrap();
		assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
		let bytes = tangram_serialize::to_vec(&data).unwrap();
		let parsed: Data = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
		assert_eq!(parsed.output.unwrap().options.tokens, tokens);
		assert_eq!(parsed.error.unwrap().options.tokens, tokens);
	}
}

#[test]
fn output_with_location_and_tokens_inherits_the_outer_tokens() {
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let sync = sync_token(&key);
	let tokens = tg::authorization::Tokens::with_authorization([sync.clone()]);
	let files = [b"first".as_slice(), b"second".as_slice()]
		.map(|bytes| tg::object::Id::from(tg::file::Id::new(bytes)));
	let value = tg::value::Data::Array(
		files
			.iter()
			.cloned()
			.map(|file| tg::value::Data::Object(tg::Referent::with_node(file)))
			.collect(),
	);
	let mut data = process_data();
	data.output = Some(tg::Referent::with_node_and_tokens(value, tokens));
	data.error = Some(tg::Referent::with_node_and_tokens(
		tg::Either::Right(tg::error::Id::new(b"error")),
		tg::authorization::Tokens::with_authorization([sync.clone()]),
	));

	// The stored output carries the sync token once, on the outer referent.
	let mut stored = Vec::new();
	data.output
		.as_ref()
		.unwrap()
		.node
		.children_with_tokens(&mut stored);
	assert!(stored.iter().all(|object| object.options.tokens.is_empty()));

	// The expanded output carries the sync token on every object referent.
	let mut objects = Vec::new();
	data.output_with_location_and_tokens()
		.unwrap()
		.children_with_tokens(&mut objects);
	assert_eq!(objects.len(), files.len());
	for object in &objects {
		assert_eq!(
			object.options.tokens.local_authorization(),
			std::slice::from_ref(&sync)
		);
	}
	let tg::Either::Right(error) = data.error_with_location_and_tokens().unwrap() else {
		panic!("expected an error referent");
	};
	assert_eq!(error.options.tokens.local_authorization(), [sync]);
}

#[test]
fn without_location_and_tokens_except_sync_keeps_only_the_outer_sync_tokens() {
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let sync = sync_token(&key);
	let file = tg::object::Id::from(tg::file::Id::new(b"file"));
	let read = object_token(&key, file.clone().into());
	let outer = tg::authorization::Tokens::with_authorization([sync.clone(), read.clone()]);
	let inner = tg::authorization::Tokens::with_authorization([sync.clone(), read]);
	let mut data = process_data();
	data.command.options.tokens = outer.clone();
	data.log = Some(tg::Referent::with_node_and_tokens(
		tg::blob::Id::new(b"log"),
		outer.clone(),
	));
	data.output = Some(tg::Referent::with_node_and_tokens(
		tg::value::Data::Object(tg::Referent::with_node_and_tokens(file, inner)),
		outer.clone(),
	));
	let mut remote = outer.clone();
	let remote_location = tg::Location::Remote(tg::location::Remote {
		name: "default".to_owned(),
		region: None,
	});
	remote.set(remote_location.clone(), remote.local_entry());
	data.error = Some(tg::Referent::with_node_and_tokens(
		tg::Either::Right(tg::error::Id::new(b"error")),
		remote,
	));

	let data = data.without_location_and_tokens_except_sync();
	assert_eq!(
		data.command.options.tokens.local_authorization(),
		std::slice::from_ref(&sync)
	);
	assert_eq!(
		data.log.unwrap().options.tokens.local_authorization(),
		std::slice::from_ref(&sync)
	);
	let error = data.error.unwrap();
	assert_eq!(
		error.options.tokens.local_authorization(),
		std::slice::from_ref(&sync)
	);
	assert!(error.options.tokens.get(&remote_location).is_none());
	let output = data.output.unwrap();
	assert_eq!(output.options.tokens.local_authorization(), [sync]);
	let mut objects = Vec::new();
	output.node.children_with_tokens(&mut objects);
	assert!(
		objects
			.iter()
			.all(|object| object.options.tokens.is_empty())
	);
}

fn process_data() -> Data {
	let value = serde_json::json!({
		"command": tg::command::Id::new(b"command"),
		"created_at": 0,
		"host": "test",
		"status": "finished",
	});
	serde_json::from_value(value).unwrap()
}

fn sync_token(key: &tg::authorization::PrivateKey) -> tg::authorization::Token {
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)],
		resource: tg::sync::Id::new().into(),
	};
	tg::authorization::Token::sign(body, key).unwrap()
}

fn object_token(key: &tg::authorization::PrivateKey, resource: tg::Id) -> tg::authorization::Token {
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)],
		resource,
	};
	tg::authorization::Token::sign(body, key).unwrap()
}
