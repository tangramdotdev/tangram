use {crate::Session, tangram_client::prelude::*};

#[test]
fn spawn_command_sync_tokens_survive_forwarding() {
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let sync = tg::sync::Id::new();
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)],
		resource: sync.clone().into(),
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();
	let sync = tg::Referent::with_node_and_local_tokens(sync, vec![token.clone()]);
	let file = tg::file::Id::new(b"file");
	let blob = tg::blob::Id::new(b"input");
	let executable = tg::command::data::Executable {
		artifact: Some(file.into()),
		path: None,
	};
	let arg = tg::value::Data::Object(tg::Referent::with_node(
		tg::directory::Id::new(b"arg").into(),
	));
	let env = tg::value::Data::Object(tg::Referent::with_node(tg::file::Id::new(b"env").into()));
	let inline = tg::process::spawn::CommandArg {
		args: vec![tg::command::data::Value::Value(tg::value::Data::Array(
			vec![arg],
		))],
		cwd: None,
		env: [("INPUT".into(), tg::command::data::Value::Value(env))].into(),
		executable: tg::Referent::with_node(executable),
		host: Some("aarch64-darwin".into()),
		stdin: Some(tg::Referent::with_node(blob)),
		user: None,
	};
	let locations = [
		tg::Location::Local(tg::location::Local {
			region: Some("east".into()),
		}),
		tg::Location::Remote(tg::location::Remote {
			name: "remote".into(),
			region: None,
		}),
	];
	for location in locations {
		for node in [
			tg::Either::Left(inline.clone()),
			tg::Either::Right(tg::command::Id::new(b"command")),
		] {
			let count = if node.is_left() { 4 } else { 1 };
			let mut command = tg::Referent::with_node(node);
			Session::set_spawn_process_command_sync(&mut command, &location, &sync).unwrap();
			Session::update_spawn_process_command_for_location(&mut command, &location).unwrap();
			assert_eq!(
				command.options.tokens.local_authorization(),
				std::slice::from_ref(&token)
			);
			let json = serde_json::to_value(&command).unwrap();
			let command = serde_json::from_value(json).unwrap();
			let objects = Session::spawn_process_command_nodes(&command).unwrap();
			assert_eq!(objects.len(), count);
			for object in objects {
				assert_eq!(
					object.options.tokens.local_authorization(),
					std::slice::from_ref(&token)
				);
			}
		}
	}
}
