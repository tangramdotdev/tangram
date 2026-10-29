use {
	futures::{StreamExt as _, channel::mpsc},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_tests_util::{Config, server::Server},
};

/// Joining a sync requires the sync's own proof, so a sync's ID alone, another sync's proof, or a forged proof is rejected.
#[tokio::test]
async fn joining_a_sync_requires_its_proof() -> tg::Result<()> {
	// Start a server with a root token.
	let root_token = "root";
	let mut config = Config::new();
	config.merge(serde_json::json!({ "authentication": { "root": { "token": root_token } } }));
	let server = Server::start(config).await?;

	// A sync returns a referent with a proof, and joining it with that referent returns the same referent.
	let referent = open(&server, root_token, None).await?;
	let joined = open(&server, root_token, Some(referent.clone())).await?;
	assert_eq!(
		joined.node, referent.node,
		"a join should return the joined sync"
	);

	// Forge the proof by changing a character of its signature.
	let token = referent
		.options
		.tokens
		.local()
		.and_then(|entry| entry.authorization.first())
		.ok_or_else(|| tg::error!("expected the sync's proof"))?
		.to_string();
	let index = token.len() - 2;
	let replacement = if token.as_bytes()[index] == b'0' {
		"1"
	} else {
		"0"
	};
	let mut forged = token.clone();
	forged.replace_range(index..=index, replacement);
	let forged = forged.parse::<tg::authorization::Token>()?;
	let entry = tg::authorization::tokens::Entry {
		authorization: vec![forged],
	};
	let forged = tg::Referent::with_node_and_tokens(
		referent.node.clone(),
		tg::authorization::Tokens::with_local_entry(entry),
	);

	// Joining with the ID alone, another sync's proof, or the forged proof is rejected.
	let other = open(&server, root_token, None).await?;
	let without_proof = tg::Referent::with_node(referent.node.clone());
	let mismatched = tg::Referent::new(other.node.clone(), referent.options.clone());
	for join in [without_proof, mismatched, forged] {
		let error = open(&server, root_token, Some(join))
			.await
			.expect_err("an invalid join should be rejected");
		let message = error.trace().to_string();
		assert!(message.contains("invalid sync authorization"), "{message}");
	}

	server.stop().await?;

	Ok(())
}

/// Open a sync that gets a missing object, joining a sync if one is given, and return the sync's referent from the header.
async fn open(
	server: &Server,
	token: &str,
	join: Option<tg::Referent<tg::sync::Id>>,
) -> tg::Result<tg::Referent<tg::sync::Id>> {
	let missing = tg::blob::Id::new(tg::process::Id::new().to_string().as_bytes());
	let arg = tg::sync::Arg {
		get: vec![tg::Referent::with_node(tg::Selector::Id(missing.into()))],
		sync: join.map(tg::sync::Join::Sync),
		..Default::default()
	};
	let session = server
		.client()?
		.session(&tg::Context::new(Some(token.to_owned())));

	// Keep the request open until the header arrives, then end it.
	let (sender, receiver) = mpsc::unbounded::<tg::Result<tg::sync::Message>>();
	let result = session.sync(arg, receiver.boxed()).await;
	drop(sender);
	let header = match result? {
		ControlFlow::Break((header, _)) => header,
		ControlFlow::Continue(error) => return Err(error),
	};
	let referent = header
		.sync
		.ok_or_else(|| tg::error!("expected the sync's referent"))?;

	Ok(referent)
}
