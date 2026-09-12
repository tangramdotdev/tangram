use {super::*, tangram_http::request::builder::Ext as _};

#[tokio::test]
async fn request_arg_preserves_list_and_node_options() {
	let key =
		tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
			.unwrap();
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: tg::file::Id::new(b"contents").into(),
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();
	let tokens = tg::Tokens::with_authorization([token]);
	let location = tg::Location::Remote(tg::location::Remote {
		name: "remote".into(),
		region: Some("region".into()),
	});
	for length in [1, tangram_http::body::arg::THRESHOLD] {
		let name = "x".repeat(length);
		let value = serde_json::json!({
			"cached": true,
			"groups": false,
			"length": 7,
			"location": location,
			"name": name,
			"organizations": false,
			"path": "child",
			"position": 3,
			"recursive": true,
			"reverse": true,
			"tags": false,
			"tokens": tokens,
			"ttl": "infinite",
			"users": false,
		});
		let request = http::Request::builder()
			.uri("/list/node")
			.arg(&value, BoxBody::empty())
			.unwrap()
			.unwrap();
		let (arg, _) = request.arg::<Arg>().await.unwrap();
		let Arg { arg, options } = arg.unwrap();
		assert!(arg.cached);
		assert!(!arg.groups);
		assert_eq!(arg.length, Some(7));
		assert_eq!(arg.location, Some(location.clone().into()));
		assert_eq!(arg.position, Some(3));
		assert!(!arg.organizations);
		assert!(arg.recursive);
		assert!(arg.reverse);
		assert!(!arg.tags);
		assert_eq!(arg.ttl, tg::remote::cache::Ttl::Infinite);
		assert!(!arg.users);
		assert_eq!(options.name.as_deref(), Some(name.as_str()));
		assert_eq!(options.path, Some("child".into()));
		assert_eq!(options.tokens, tokens);
	}
}

#[test]
fn sort_and_truncate_applies_position() {
	let entries = vec![entry("c"), entry("a"), entry("b")];
	let entries = sort_and_truncate(entries, false, Some(1), Some(1));
	let specifiers = entries
		.into_iter()
		.map(|entry| entry.specifier().to_string())
		.collect::<Vec<_>>();

	assert_eq!(specifiers, ["b"]);
}

#[test]
fn sort_and_truncate_defaults_position_to_zero() {
	let entries = vec![entry("b"), entry("a")];
	let entries = sort_and_truncate(entries, false, None, None);
	let specifiers = entries
		.into_iter()
		.map(|entry| entry.specifier().to_string())
		.collect::<Vec<_>>();

	assert_eq!(specifiers, ["a", "b"]);
}

fn entry(specifier: &str) -> tg::list::Entry {
	let id = tg::tag::Id::new();
	let name = specifier.to_owned();
	let specifier = specifier.parse().unwrap();
	let target = tg::Either::Left(tg::file::Id::new(name.as_bytes()).into());
	let target = Some(tg::Referent::with_node(target));
	tg::list::Entry {
		node: tg::Referent::with_node(id.into()),
		parent: None,
		specifier,
		target,
	}
}
