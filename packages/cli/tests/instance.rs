use {
	tangram_client::prelude::*,
	tangram_tests_util::{Instance, cli::Server, instance},
};

/// The servers in a local instance share its storage.
#[tokio::test]
async fn local_instance_servers_share_storage() -> tg::Result<()> {
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let instance = Instance::local(instance::Arg::default())?;

	// The first server stores an object.
	let first = Server::start_with_instance(tangram, &instance, instance.config(None)?).await?;
	let id = first
		.tg(None, ["put", "--no-tokens", r#"tg.file("shared")"#])
		.await?;
	first.stop().await?;

	// The second server reads it.
	let second = Server::start_with_instance(tangram, &instance, instance.config(None)?).await?;
	let output = second.tg(None, ["read", &id]).await?;
	assert_eq!(output, "shared");
	second.stop().await?;

	Ok(())
}

/// The servers in each region of a cloud instance share the instance's storage.
#[cfg(all(
	feature = "foundationdb",
	feature = "nats",
	feature = "postgres",
	feature = "scylla"
))]
#[tokio::test]
async fn cloud_instance_servers_share_storage() -> tg::Result<()> {
	let tangram = env!("CARGO_BIN_EXE_tangram");
	let arg = instance::Arg {
		primary_region: Some("a".to_owned()),
		regions: vec!["a".to_owned(), "b".to_owned()],
		..Default::default()
	};
	let Some(instance) = Instance::cloud(arg).await? else {
		return Ok(());
	};

	// A server in each region stores an object, and another server in the same region reads it.
	for region in ["a", "b"] {
		let writer =
			Server::start_with_instance(tangram, &instance, instance.config(Some(region))?).await?;
		let id = writer
			.tg(None, ["put", "--no-tokens", r#"tg.file("shared")"#])
			.await?;
		writer.stop().await?;
		let reader =
			Server::start_with_instance(tangram, &instance, instance.config(Some(region))?).await?;
		let output = reader.tg(None, ["read", &id]).await?;
		assert_eq!(output, "shared");
		reader.stop().await?;
	}

	Ok(())
}
