use tangram_client::prelude::*;

#[test]
fn capture_entries_stay_bounded_when_roots_share_a_token() {
	// Create a sync token that every root carries, as a finished process's outputs do.
	let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)],
		resource: tg::sync::Id::new().into(),
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();

	// Create the roots.
	let roots = (0..513)
		.map(|index| {
			let id = tg::blob::Id::new(format!("blob {index}").as_bytes());
			let mut root = tg::Referent::with_node(tg::Id::from(id));
			root.options.tokens = tg::authorization::Tokens::with_authorization([token.clone()]);
			root
		})
		.collect::<Vec<_>>();

	// Enqueue the capture and check the size of each entry.
	let principal = tg::Principal::Process(tg::process::Id::new());
	let resource = tg::Id::from(tg::process::Id::new());
	let items = super::enqueue_permission_capture_items(principal, resource, None, roots);
	let mut count = 0;
	for item in items {
		let tangram_index::batch::Item::EnqueuePermissionCapture(arg) = item else {
			continue;
		};
		count += arg.roots.len();
		let size = tangram_serialize::to_vec(&arg).unwrap().len();
		assert!(size < 10_000, "a capture entry is {size} bytes");
	}
	assert_eq!(count, 513);
}
