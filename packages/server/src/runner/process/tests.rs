use {super::*, std::collections::BTreeMap};

#[test]
fn rendering_preserves_module_tokens() {
	let tg::command::data::Value::Value(tg::value::Data::Object(input)) = input(b"module") else {
		panic!("expected an object argument");
	};
	let pointer = tg::graph::data::Pointer {
		graph: Some(tg::graph::Id::new(b"graph")),
		index: 0,
		kind: tg::artifact::Kind::File,
	};
	let module = tg::module::Data {
		kind: tg::module::Kind::Ts,
		referent: tg::Referent::with_node_and_tokens(
			tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer)),
			input.options.tokens.clone(),
		),
	};
	let arg = tg::command::data::Value::Value(tg::value::Data::Module(module));
	let args = render_args(&[arg], Path::new("/store"), Path::new("/output")).unwrap();
	let tg::Value::Module(module) = args[0].parse::<tg::Value>().unwrap() else {
		panic!("expected a module argument");
	};
	assert_eq!(
		module.to_data().referent.options.tokens,
		input.options.tokens
	);
}

#[test]
fn rendering_preserves_each_inputs_tokens() {
	let first = input(b"first");
	let second = input(b"second");
	let args = vec![first.clone(), second.clone()];
	let env = BTreeMap::from([("FIRST".to_owned(), first), ("SECOND".to_owned(), second)]);
	let store = Path::new("/store");
	let output = Path::new("/output");
	let rendered_args = render_args(&args, store, output).unwrap();
	let rendered_env = render_env(&env, store, output).unwrap();
	for (index, key) in ["FIRST", "SECOND"].into_iter().enumerate() {
		let tg::command::data::Value::Value(tg::value::Data::Object(expected)) = &args[index]
		else {
			panic!("expected an object argument");
		};
		for rendered in [
			&rendered_args[index],
			&rendered_env[key],
			&rendered_env[&format!("{}{key}", tg::process::env::PREFIX)],
		] {
			let value: tg::Value = rendered.parse().unwrap();
			let tg::Value::Object(object) = value else {
				panic!("expected an object argument");
			};
			assert_eq!(object.id(), expected.node);
			assert_eq!(object.state().tokens(), expected.options.tokens);
		}
	}
}

fn input(bytes: &[u8]) -> tg::command::data::Value {
	let key =
		tg::authorization::PrivateKey::generate("default", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let token = tg::authorization::Token::sign(
		tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			)],
			resource: tg::sync::Id::new().into(),
		},
		&key,
	)
	.unwrap();
	let mut tokens = tg::authorization::Tokens::default();
	tokens.insert_authorization(tg::Location::Local(tg::location::Local::default()), token);
	let id = tg::file::Id::new(bytes).into();
	let referent = tg::Referent::with_node_and_tokens(id, tokens);
	tg::command::data::Value::Value(tg::value::Data::Object(referent))
}
