use crate::prelude::*;

#[test]
fn empty_referent_options_are_omitted() {
	let mut options = tg::referent::Options::default();
	let expected = print_values(options.clone());
	for output in &expected {
		assert!(!output.contains("\"options\""));
	}
	options.tokens.insert_local_authorization(token());
	assert_eq!(print_values(options), expected);
}

#[test]
fn nonempty_referent_options_are_preserved() {
	let id = tg::file::Id::new(b"source");
	let token = token();
	for (field, value) in [
		("artifact", id.to_string()),
		("id", id.to_string()),
		("location", "local".into()),
		("name", String::new()),
		("path", String::new()),
		("tag", "example".into()),
	] {
		let mut options: tg::referent::Options =
			serde_json::from_value(serde_json::json!({ (field): value })).unwrap();
		let expected = print_values(options.clone());
		let options_field = format!("\"options\":{{\"{field}\":");
		for output in &expected {
			assert!(output.contains(&options_field), "{output}");
		}
		options.tokens.insert_local_authorization(token.clone());
		assert_eq!(print_values(options), expected);
	}
}

#[test]
fn object_query_is_gray() {
	let id = tg::file::Id::new(b"source");
	let referent = tg::Referent::with_node_and_local_tokens(id.clone(), vec![token()]);
	let string = referent.to_string();
	let (_, query) = string.split_once('?').unwrap();
	let value = tg::Value::from(tg::File::with_referent(referent));
	let options = tg::value::print::Options {
		color: true,
		tokens: true,
		..Default::default()
	};
	assert_eq!(
		value.print(options),
		format!("\x1b[94m{id}\x1b[0m\x1b[38;5;244m?{query}\x1b[0m")
	);
}

#[test]
fn referent_query_preserves_options() {
	let id = tg::file::Id::new(b"source");
	let mut options = tg::referent::Options::with_path("source.ts");
	options.tokens.insert_local_authorization(token());
	let referent = tg::Referent::new(id.clone(), options);
	let string = referent.to_string();
	let (_, query) = string.split_once('?').unwrap();
	let options = tg::value::print::Options {
		color: true,
		tokens: true,
		..Default::default()
	};
	let mut output = String::new();
	let mut printer = tg::value::print::Printer::new(&mut output, options);
	printer.referent(&referent).unwrap();
	assert_eq!(
		output,
		format!("\x1b[94m{id}\x1b[0m\x1b[38;5;244m?{query}\x1b[0m")
	);
}

#[test]
fn referent_without_color_is_a_uri() {
	let id = tg::file::Id::new(b"source");
	let referent = tg::Referent::with_node_and_local_tokens(id, vec![token()]);
	let options = tg::value::print::Options {
		tokens: true,
		..Default::default()
	};
	let mut output = String::new();
	let mut printer = tg::value::print::Printer::new(&mut output, options);
	printer.referent(&referent).unwrap();
	assert_eq!(output, referent.to_string());
}

#[test]
fn referent_without_tokens_preserves_other_options() {
	let id = tg::file::Id::new(b"source");
	let mut options = tg::referent::Options::with_path("source.ts");
	options.tokens.insert_local_authorization(token());
	let referent = tg::Referent::new(id, options);
	let mut output = String::new();
	let mut printer =
		tg::value::print::Printer::new(&mut output, tg::value::print::Options::default());
	printer.referent(&referent).unwrap();
	assert_eq!(output, referent.without_token().to_string());
	assert!(output.contains("path=source.ts"));
	assert!(!output.contains("tokens["));
	assert!(!referent.options.tokens.is_empty());
}

fn print_values(options: tg::referent::Options) -> [String; 3] {
	let module = tg::Module {
		kind: tg::module::Kind::Ts,
		referent: tg::Referent::new(
			tg::module::Source::Path("source.ts".into()),
			options.clone(),
		),
	};
	let source = tg::File::with_id(tg::file::Id::new(b"source"));
	let node = Some(tg::graph::Edge::Object(source.into()));
	let dependency = tg::graph::Dependency(tg::Referent::new(node, options));
	let reference = tg::Reference::with_path("dependency".into());
	let file = tg::graph::File {
		contents: tg::Blob::with_id(tg::blob::Id::new(b"contents")),
		dependencies: [(reference, Some(dependency))].into(),
		executable: false,
		module: None,
	};
	let graph = tg::Graph::with_nodes(vec![tg::graph::Node::File(file.clone())]);
	let file = tg::File::with_object(tg::file::Object::Node(file));
	let values = [
		tg::Value::from(module),
		tg::Value::from(file),
		tg::Value::from(graph),
	];
	values.map(|value| value.print(tg::value::print::Options::default()))
}

fn token() -> tg::authorization::Token {
	let private_key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: tg::file::Id::new(b"source").into(),
	};
	tg::authorization::Token::sign(body, &private_key).unwrap()
}
