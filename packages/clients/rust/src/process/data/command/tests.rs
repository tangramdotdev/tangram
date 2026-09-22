use {super::Command, crate::prelude::*, std::collections::BTreeMap};

#[test]
fn id_matches_command_object() {
	let command = command();
	let object = tg::command::Builder::try_with_spawn_arg(command.to_spawn_arg())
		.unwrap()
		.build()
		.unwrap();
	assert_eq!(command.id().unwrap(), object.id());
}

#[test]
fn id_ignores_tokens_and_locations() {
	let mut command = command();
	let id = command.id().unwrap();
	let tokens = tokens();
	let location = tg::Location::Remote(tg::location::Remote {
		name: "source".to_owned(),
		region: None,
	});
	command.executable.options.tokens = tokens.clone();
	command.executable.options.location = Some(location.clone());
	let stdin = command.stdin.as_mut().unwrap();
	stdin.options.tokens = tokens.clone();
	stdin.options.location = Some(location.clone());
	for value in command.args.iter_mut().chain(command.env.values_mut()) {
		let tg::command::data::Value::Value(tg::value::Data::Object(object)) = value else {
			panic!("expected an object argument");
		};
		object.options.tokens = tokens.clone();
		object.options.location = Some(location.clone());
	}
	assert_eq!(command.id().unwrap(), id);
	for object in command.objects() {
		assert_eq!(object.options.tokens, tokens);
		assert_eq!(object.options.location, Some(location.clone()));
	}
}

#[test]
fn canonical_data_preserves_argument_names() {
	let mut command = command();
	let id = command.id().unwrap();
	let tg::command::data::Value::Value(tg::value::Data::Object(object)) = &mut command.args[0]
	else {
		panic!("expected an object argument");
	};
	object.options.path = Some("input.txt".into());
	assert_ne!(command.id().unwrap(), id);
	let data = command.to_command_data();
	let tg::command::data::Value::Value(tg::value::Data::Object(object)) = &data.args[0] else {
		panic!("expected an object argument");
	};
	assert_eq!(
		object.options.path.as_deref(),
		Some(std::path::Path::new("input.txt"))
	);
}

#[test]
fn serialization_preserves_referents() {
	let mut command = command();
	command.executable.options.tokens = tokens();
	command.stdin.as_mut().unwrap().options.tokens = tokens();
	let json = serde_json::to_value(&command).unwrap();
	let from_json: Command = serde_json::from_value(json.clone()).unwrap();
	assert_eq!(serde_json::to_value(&from_json).unwrap(), json);
	let bytes = tangram_serialize::to_vec(&command).unwrap();
	let from_bytes: Command = tangram_serialize::from_slice(&bytes).unwrap();
	assert_eq!(serde_json::to_value(&from_bytes).unwrap(), json);
}

#[test]
fn resolving_command_object_inherits_tokens_without_inheriting_names() {
	let data = command().to_command_data();
	let options = tg::referent::Options {
		location: Some(tg::Location::Remote(tg::location::Remote {
			name: "source".to_owned(),
			region: None,
		})),
		path: Some("command".into()),
		tokens: tokens(),
		..tg::referent::Options::default()
	};
	let command = Command::with_command_data(data.clone(), &options);
	for object in command.objects() {
		assert_eq!(object.options.tokens, options.tokens);
		assert_eq!(object.options.location, options.location);
		assert!(object.options.path.is_none());
	}
	assert_eq!(
		command.id().unwrap(),
		tg::command::Id::new(&data.serialize().unwrap())
	);
}

#[test]
fn inheritance_preserves_input_location_and_tokens() {
	let mut command = command();
	let input_tokens = tokens();
	let input_location = tg::Location::Remote(tg::location::Remote {
		name: "input".to_owned(),
		region: None,
	});
	command.executable.options.tokens = input_tokens.clone();
	command.executable.options.location = Some(input_location.clone());
	let options = tg::referent::Options {
		location: Some(tg::Location::Local(tg::location::Local::default())),
		tokens: tokens(),
		..tg::referent::Options::default()
	};
	command.inherit_location_and_tokens(&options);
	assert_eq!(command.executable.options.location, Some(input_location));
	let mut expected = input_tokens;
	expected.inherit(&options.tokens);
	assert_eq!(command.executable.options.tokens, expected);
}

#[test]
fn module_serialization_preserves_graph_tokens() {
	let graph = tg::Graph::with_id(tg::graph::Id::new(b"graph"));
	let tokens = tokens();
	graph.state().set_tokens(tokens.clone());
	let pointer = tg::graph::Pointer {
		graph: Some(graph),
		index: 0,
		kind: tg::artifact::Kind::File,
	};
	let module = tg::Module {
		kind: tg::module::Kind::Ts,
		referent: tg::Referent::with_node(tg::module::Source::Edge(tg::graph::Edge::Pointer(
			pointer,
		))),
	};
	assert_eq!(module.to_data().referent.options.tokens, tokens);
}

#[test]
fn command_can_have_no_objects() {
	let mut command = command();
	command.args.clear();
	command.env.clear();
	command.executable.node.artifact = None;
	command.stdin = None;
	assert!(command.objects().is_empty());
}

#[test]
fn referent_serialization_preserves_both_variants() {
	let command = command();
	let id = command.id().unwrap();
	for node in [tg::Either::Left(Box::new(command)), tg::Either::Right(id)] {
		let options = tg::referent::Options {
			name: Some("compile".to_owned()),
			..Default::default()
		};
		let referent = tg::Referent::new(node, options);
		let json = serde_json::to_value(&referent).unwrap();
		let from_json: tg::Referent<tg::Either<Box<Command>, tg::command::Id>> =
			serde_json::from_value(json.clone()).unwrap();
		let bytes = tangram_serialize::to_vec(&referent).unwrap();
		let from_bytes: tg::Referent<tg::Either<Box<Command>, tg::command::Id>> =
			tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(serde_json::to_value(from_json).unwrap(), json);
		assert_eq!(serde_json::to_value(from_bytes).unwrap(), json);
	}
}

#[test]
fn inline_tokens_do_not_spread_to_other_inputs() {
	let mut command = command();
	let tokens = tokens();
	command.executable.options.tokens = tokens.clone();
	let referent = tg::Referent::with_node(tg::Either::Left(Box::new(command)));
	let objects = referent.objects();
	assert_eq!(objects[0].options.tokens, tokens);
	assert!(
		objects[1..]
			.iter()
			.all(|object| object.options.tokens.is_empty())
	);
	assert!(referent.options.tokens.is_empty());
}

#[test]
fn command_field_serialization_preserves_variants_and_options() {
	let command = command();
	let id = command.id().unwrap();
	for options in [
		tg::referent::Options::default(),
		tg::referent::Options {
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: Some("compile".into()),
			path: Some("build.ts".into()),
			tokens: tokens(),
			..Default::default()
		},
	] {
		let string = tg::Referent::new(id.clone(), options.clone()).to_string();
		for node in [
			tg::Either::Left(Box::new(command.clone())),
			tg::Either::Right(id.clone()),
		] {
			let referent = tg::Referent::new(node, options.clone());
			let mut data: tg::process::Data = serde_json::from_value(serde_json::json!({
				"command": string,
				"created_at": 0,
				"host": "test",
				"sandbox": "sbx_00041061050r3gg28a1c60t3gf20",
				"status": "started"
			}))
			.unwrap();
			data.command = referent;
			let json = serde_json::to_value(&data).unwrap();
			assert_eq!(json["command"].is_string(), data.command.node.is_right());
			if data.command.node.is_right() {
				assert_eq!(json["command"], string);
			}
			let parsed: tg::process::Data = serde_json::from_value(json.clone()).unwrap();
			assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
			let bytes = tangram_serialize::to_vec(&data).unwrap();
			let parsed: tg::process::Data = tangram_serialize::from_slice(&bytes).unwrap();
			assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
			let arg: tg::process::spawn::Arg =
				serde_json::from_value(serde_json::json!({"command": json["command"]})).unwrap();
			assert_eq!(
				serde_json::to_value(&arg).unwrap()["command"],
				json["command"]
			);
			let bytes = tangram_serialize::to_vec(&arg).unwrap();
			let parsed: tg::process::spawn::Arg = tangram_serialize::from_slice(&bytes).unwrap();
			assert_eq!(
				serde_json::to_value(&parsed).unwrap(),
				serde_json::to_value(&arg).unwrap()
			);
		}
	}
}

#[test]
fn command_id_referent_has_only_the_command_as_a_root() {
	let command = command();
	let id = command.id().unwrap();
	let tokens = tokens();
	let referent =
		tg::Referent::with_node_and_tokens(tg::Either::Right(id.clone()), tokens.clone());
	let objects = referent.objects();
	assert_eq!(objects.len(), 1);
	assert_eq!(objects[0].node, id.clone().into());
	assert_eq!(objects[0].options.tokens, tokens);
	assert_eq!(referent.command_id().unwrap(), id);
}

#[test]
fn inheritance_normalizes_object_referents() {
	let mut command = command();
	let resource = command.executable.node.artifact.clone().unwrap().into();
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let body = tg::authorization::Body {
		expires_at: 30,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource,
	};
	let direct = tg::authorization::Token::sign(body, &key).unwrap();
	command.executable.options.tokens = tg::Tokens::with_authorization([direct.clone()]);
	let mut inherited = direct.clone();
	inherited.body.resource = tg::directory::Id::new(b"parent").into();
	let options = tg::referent::Options {
		tokens: tg::Tokens::with_authorization([inherited.clone()]),
		..Default::default()
	};
	command.inherit_location_and_tokens(&options);
	assert_eq!(
		command.executable.options.tokens.local_authorization(),
		&[direct]
	);
	assert_eq!(
		command
			.stdin
			.as_ref()
			.unwrap()
			.options
			.tokens
			.local_authorization(),
		&[inherited]
	);
}

fn command() -> Command {
	let file = tg::file::Id::new(b"file");
	let executable = tg::command::data::Executable {
		artifact: Some(file.clone().into()),
		path: None,
	};
	let value = tg::command::data::Value::Value(tg::value::Data::Object(tg::Referent::with_node(
		file.into(),
	)));
	Command {
		args: vec![value.clone()],
		cwd: None,
		env: BTreeMap::from([("INPUT".to_owned(), value)]),
		executable: tg::Referent::with_node(executable),
		host: "x86_64-linux".to_owned(),
		stdin: Some(tg::Referent::with_node(tg::blob::Id::new(b"stdin"))),
		user: None,
	}
}

fn tokens() -> tg::Tokens {
	let key =
		tg::authorization::PrivateKey::generate("default", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let token = tg::sync::Token::sign(tg::sync::token::Body::new(i64::MAX), &key).unwrap();
	let mut tokens = tg::Tokens::default();
	tokens.insert_sync(tg::Location::Local(tg::location::Local::default()), token);
	tokens
}
