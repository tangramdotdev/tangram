use {super::*, tangram_http::request::builder::Ext as _};

#[tokio::test]
async fn request_arg_preserves_list_and_node_options() {
	let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: tg::file::Id::new(b"contents").into(),
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();
	let tokens = tg::authorization::Tokens::with_authorization([token]);
	let location = tg::Location::Remote(tg::location::Remote {
		name: "remote".into(),
		region: Some("region".into()),
	});
	for length in [1, tangram_http::body::arg::THRESHOLD] {
		let name = "x".repeat(length);
		let value = serde_json::json!({
			"cached": true,
			"groups": false,
			"limit": 7,
			"location": location,
			"name": name,
			"organizations": false,
			"path": "child",
			"cursor": "opaque",
			"recursive": true,
			"reverse": true,
			"tags": false,
			"tokens": tokens,
			"ttl": "infinite",
			"users": false,
		});
		let mut request = http::Request::builder()
			.uri("/list/node")
			.arg(&value, BoxBody::empty())
			.unwrap()
			.unwrap();
		request
			.extensions_mut()
			.insert(tangram_http::body::arg::MaxLength::default());
		let (arg, _) = request.arg::<Arg>().await.unwrap();
		let Arg { arg, options } = arg.unwrap();
		assert!(arg.cached);
		assert!(!arg.groups);
		assert_eq!(arg.limit, Some(7));
		assert_eq!(arg.location, Some(location.clone().into()));
		assert_eq!(arg.cursor.as_deref(), Some("opaque"));
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
fn sort_items_breaks_version_ties() {
	for reverse in [false, true] {
		let items = vec![item("10"), item("1"), item("2"), item("01")];
		let items = sort_items(items, reverse);
		let specifiers = items
			.into_iter()
			.map(|item| item.specifier().to_string())
			.collect::<Vec<_>>();
		let mut expected = vec!["01", "1", "2", "10"];
		if reverse {
			expected.reverse();
		}
		assert_eq!(specifiers, expected);
	}
}

fn item(specifier: &str) -> tg::list::Item {
	let id = tg::tag::Id::new();
	let name = specifier.to_owned();
	let specifier = specifier.parse().unwrap();
	let target = tg::Either::Left(tg::file::Id::new(name.as_bytes()).into());
	let target = Some(tg::Referent::with_node(target));
	tg::list::Item {
		node: tg::Referent::with_node(id.into()),
		parent: None,
		specifier,
		target,
	}
}
