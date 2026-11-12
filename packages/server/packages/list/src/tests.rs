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
			"time_to_live": "infinite",
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
		assert_eq!(arg.time_to_live, tg::remote::cache::TimeToLive::Infinite);
		assert!(!arg.users);
		assert_eq!(options.name.as_deref(), Some(name.as_str()));
		assert_eq!(options.path, Some("child".into()));
		assert_eq!(options.tokens, tokens);
	}
}

// The merged page orders the items by version, keeps the first page's item for each specifier, and continues when a page has more items.
#[test]
fn merge_pages_orders_by_version_and_keeps_the_first_item() {
	for reverse in [false, true] {
		let local = item("1");
		let local_id = local.id().clone();
		let local = Page {
			data: vec![item("10"), local],
			next: None,
		};
		let remote = Page {
			data: vec![item("2"), item("1"), item("01")],
			next: None,
		};
		let page = merge_pages(vec![local, remote], 10, reverse);
		let specifiers = page
			.data
			.iter()
			.map(|item| item.specifier().to_string())
			.collect::<Vec<_>>();
		let mut expected = vec!["01", "1", "2", "10"];
		if reverse {
			expected.reverse();
		}
		assert_eq!(specifiers, expected);
		let one = page
			.data
			.iter()
			.find(|item| item.specifier().to_string() == "1")
			.unwrap();
		assert_eq!(one.id(), &local_id);
		assert!(page.next.is_none());
	}
	let more = Page {
		data: vec![item("1"), item("2")],
		next: Some(Vec::new()),
	};
	let page = merge_pages(vec![more], 1, false);
	assert_eq!(page.data.len(), 1);
	assert_eq!(page.next, Some(page.data[0].specifier().key()));
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
