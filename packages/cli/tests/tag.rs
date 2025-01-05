use insta::{assert_json_snapshot, assert_snapshot};
use tangram_cli::test::test;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn list_no_results() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let pattern = "test";
		let output = server
			.tg()
			.arg("tag")
			.arg("list")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_json_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#""""#);
	})
	.await;
	Ok(())
}

#[tokio::test]
async fn get_no_results() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let pattern = "test";
		let output = server
			.tg()
			.arg("tag")
			.arg("get")
			.arg(pattern)
			.output()
			.await
			.unwrap();
		assert!(!output.status.success());
	})
	.await;
	Ok(())
}

#[tokio::test]
async fn single() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		// Write the artifact to a temp
		let artifact: temp::Artifact = temp::file!("test").into();
		let temp = Temp::new();
		let path = temp.path();
		artifact.to_path(path).await.unwrap();

		// Check in
		let output = server
			.tg()
			.arg("checkin")
			.arg(path)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		let id = std::str::from_utf8(&output.stdout).unwrap().trim();

		// Put tag
		let pattern = "test";
		let output = server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(pattern)
			.arg(id)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// List tags
		let output = server
			.tg()
			.arg("tag")
			.arg("list")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"test");

		// Get tag
		let output = server
			.tg()
			.arg("tag")
			.arg("get")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01gtq62nh8tjjx5h9v0vn7k5gdr07p3es3wypse70hymnzn3dgrw8g");
	})
	.await;
	Ok(())
}

#[tokio::test]
async fn multiple() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;

		// Create a server.
		let server = context.spawn_server().await.unwrap();

		// Write the artifact to a temp.
		let artifact: temp::Artifact = temp::file!("Hello, World!").into();
		let temp = Temp::new();
		let path = temp.path();
		artifact.to_path(path).await.unwrap();

		// Check in.
		let output = server
			.tg()
			.arg("checkin")
			.arg(path)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		let id = std::str::from_utf8(&output.stdout).unwrap().trim();

		// Tag the objects on the remote server.
		let tags = [
			"foo",
			"bar",
			"test",
			"test/1.0.0",
			"test/1.1.0",
			"test/1.2.0",
			"test/10.0.0",
			"test/hello",
			"test/world",
		];
		for tag in tags {
			let artifact: temp::Artifact = temp::file!("Hello, World!").into();
			let temp = Temp::new();
			artifact.to_path(&temp).await.unwrap();
			let output = server
				.tg()
				.arg("tag")
				.arg("put")
				.arg(tag)
				.arg(id)
				.spawn()
				.unwrap()
				.wait_with_output()
				.await
				.unwrap();
			assert!(output.status.success());
		}

		// List
		let pattern = "test";
		let output = server
			.tg()
			.arg("tag")
			.arg("list")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"test");

		// List
		let pattern = "test/*";
		let output = server
			.tg()
			.arg("tag")
			.arg("list")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r"
  test/hello
  test/world
  test/1.0.0
  test/1.1.0
  test/1.2.0
  test/10.0.0
  ");

		// Get
		let pattern = "test";
		let output = server
			.tg()
			.arg("tag")
			.arg("get")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g");

		// Get
		let pattern = "test/^1";
		let output = server
			.tg()
			.arg("tag")
			.arg("get")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g");

		// Get
		let pattern = "test/^10";
		let output = server
			.tg()
			.arg("tag")
			.arg("get")
			.arg(pattern)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g");
	})
	.await;
	Ok(())
}

#[tokio::test]
async fn remote_put() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;

		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

		// Tag the objects on the remote server.
		let tag = "foo";
		let artifact: temp::Artifact = temp::file!("foo").into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let output = remote_server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Create a local server.
		let local_server = context.spawn_server().await.unwrap();
		let output = local_server
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(remote_server.url().to_string())
			.output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Tag the objects on the remote server.
		let tag = "foo";
		let artifact: temp::Artifact = temp::file!("foo").into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let output = local_server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(tag)
			.arg(temp.path())
			.arg("--remote")
			.arg("default")
			.output()
			.await
			.unwrap();
		assert!(output.status.success());

		let local_output = local_server
			.tg()
			.arg("tag")
			.arg("list")
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(local_output.status.success());

		let remote_output = remote_server
			.tg()
			.arg("tag")
			.arg("list")
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(remote_output.status.success());

		let local_output = std::str::from_utf8(&local_output.stdout).unwrap();
		let remote_output = std::str::from_utf8(&remote_output.stdout).unwrap();

		assert_eq!(local_output, remote_output);
	})
	.await;
	Ok(())
}
