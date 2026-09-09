use {
	super::*,
	tangram_http::response::builder::Ext as _,
	tg::authorization::{Permission, Tokens, permission::object::Permission::Node},
};

#[test]
fn inheritance_preserves_existing_process_authorization() {
	let state = State::with_id(tg::File::with_contents("object").id());
	let process = tg::process::Id::new();
	for (permission, expiration) in [
		(
			tg::authorization::permission::process::Permission::NodeOutput,
			0,
		),
		(
			tg::authorization::permission::process::Permission::SubtreeOutput,
			i64::MAX,
		),
	] {
		let existing = tokens(
			process.clone().into(),
			Permission::Process(permission),
			i64::MAX,
		);
		state.set_tokens(existing.clone());
		let incoming = tokens(state.id().into(), Permission::Object(Node), expiration);
		state.inherit_tokens(&incoming);
		assert_eq!(state.tokens(), existing);
	}
}

#[test]
fn inheritance_does_not_compute_the_object_id() {
	let graph = tg::Graph::with_nodes(Vec::new());
	let state = graph.state();
	assert!(state.try_get_id().is_none());
	state.inherit_tokens(&Tokens::default());
	let parent = tg::File::with_contents("parent");
	state.inherit_tokens(&inherited(&parent.id().into()));
	assert!(state.try_get_id().is_none());
}

#[test]
fn store_refreshes_returned_tokens_and_preserves_other_locations() {
	let state = State::with_id(tg::File::with_contents("object").id());
	let inherited = inherited(&state.id());
	for returned in [
		tokens(state.id().into(), Permission::Object(Node), i64::MAX),
		Tokens::default(),
	] {
		state.set_tokens(inherited.clone());
		let local = returned.local().or(inherited.local()).cloned();
		let options = tg::referent::Options {
			tokens: returned,
			..Default::default()
		};
		state
			.finish_store(tg::Referent::new(state.id(), options))
			.unwrap();
		let mut expected = inherited.clone();
		expected.set_local(local.unwrap());
		assert_eq!(state.tokens(), expected);
		assert!(state.stored());
	}
}

#[tokio::test]
async fn load_refreshes_returned_tokens_and_preserves_concurrent_inheritance() {
	let file = tg::File::with_contents("object");
	let inherited = inherited(&file.id().into());
	let bytes = file
		.state()
		.object()
		.unwrap()
		.to_data()
		.serialize()
		.unwrap();
	for returned in [
		tokens(file.id().into(), Permission::Object(Node), i64::MAX),
		Tokens::default(),
	] {
		let state = State::with_id(file.id());
		state.set_tokens(inherited.clone());
		let mut late = Tokens::default();
		late.set(remote("late"), inherited.local().unwrap().clone());
		let mut expected = inherited.clone();
		if let Some(token) = returned.local() {
			expected.set_local(token.clone());
		}
		expected.set(remote("late"), inherited.local().unwrap().clone());
		let mut client = tg::Client::new(tg::Arg::default()).unwrap();
		let service = tower::service_fn({
			let bytes = bytes.clone();
			let state = state.clone();
			move |_| {
				state.inherit_tokens(&late);
				let response = http::Response::builder()
					.header_json(tg::object::get::TOKENS_HEADER, &returned)
					.unwrap()
					.bytes(bytes.clone())
					.unwrap()
					.map(tangram_http::body::Ext::boxed);
				async move { Ok(response) }
			}
		});
		Arc::get_mut(&mut client.0).unwrap().service = crate::http::Service::new(service);
		state.load_with_handle(&client).await.unwrap();
		assert_eq!(state.tokens(), expected);
	}
}

fn inherited(id: &tg::object::Id) -> Tokens {
	let mut tokens = tokens(id.clone().into(), Permission::Object(Node), i64::MAX - 1);
	tokens.set(remote("test"), tokens.local().unwrap().clone());
	tokens
}

fn remote(name: &str) -> tg::Location {
	tg::Location::Remote(tg::location::Remote {
		name: name.into(),
		region: None,
	})
}

fn tokens(resource: tg::Id, permission: Permission, expires_at: i64) -> Tokens {
	let body = tg::authorization::Body {
		expires_at,
		permissions: vec![permission],
		resource,
	};
	let key = tg::authorization::PrivateKey::new(
		"test",
		tg::authorization::Algorithm::Ed25519,
		vec![0; 32],
	);
	Tokens::with_local(Some(tg::authorization::Token::sign(body, &key).unwrap()))
}
