use {
	indoc::indoc,
	insta::{assert_json_snapshot, assert_snapshot},
	tangram_cli_test::{Server, assert_failure, assert_success},
	tangram_client as tg,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn list_no_results() {
	let server = Server::new(TG).await.unwrap();
	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_json_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#""[]""#);
}

#[tokio::test]
async fn get_no_results() {
	let server = Server::new(TG).await.unwrap();

	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_failure!(output);
}

#[tokio::test]
async fn single() {
	let server = Server::new(TG).await.unwrap();

	// Write the artifact to a temp.
	let artifact: temp::Artifact = temp::file!("test").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	// Check in.
	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Put tag.
	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("put")
		.arg(pattern)
		.arg(id)
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// List tags.
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test","item":"fil_012qhh3dzv6bpymrpys6pgtvga6k2wm3jknx5y5ymp0ak4prm50eh0"}]"#);

	// Get tag.
	let output = server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"tag":"test","item":"fil_012qhh3dzv6bpymrpys6pgtvga6k2wm3jknx5y5ymp0ak4prm50eh0"}"#);
}

#[tokio::test]
async fn multiple() {
	// Create a server.
	let server = Server::new(TG).await.unwrap();

	// Write the artifact to a temp.
	let artifact: temp::Artifact = temp::file!("Hello, World!").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	// Check in.
	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Tag the objects on the remote server.
	let tags = [
		"foo",
		"bar",
		"test/0.0.1/foo/bar",
		"test/1.0.0",
		"test/1.1.0",
		"test/1.2.0",
		"test/10.0.0",
		"test/foo/bar/baz",
		"test/hello/1.0.0",
		"test/world/1.0.0",
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
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// List.
	let pattern = "";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"bar","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"foo","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test"}]"#);

	// List.
	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/foo"},{"tag":"test/hello"},{"tag":"test/world"},{"tag":"test/0.0.1"},{"tag":"test/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.1.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}]"#);

	// List.
	let pattern = "test/*/*";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/foo/bar"},{"tag":"test/hello/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/world/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/0.0.1/foo"}]"#);

	// List.
	let pattern = "test/*";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/foo"},{"tag":"test/hello"},{"tag":"test/world"},{"tag":"test/0.0.1"},{"tag":"test/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.1.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}]"#);

	// List.
	let pattern = "test/=0.0.1/*";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/0.0.1/foo"}]"#);

	// List.
	let pattern = "test/=0.0.1";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/0.0.1"}]"#);

	// List.
	let pattern = "test/*";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/foo"},{"tag":"test/hello"},{"tag":"test/world"},{"tag":"test/0.0.1"},{"tag":"test/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.1.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}]"#);

	// List.
	let pattern = "test/*";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.arg("--recursive")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test/foo"},{"tag":"test/foo/bar"},{"tag":"test/foo/bar/baz","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/hello"},{"tag":"test/hello/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/world"},{"tag":"test/world/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/0.0.1"},{"tag":"test/0.0.1/foo"},{"tag":"test/0.0.1/foo/bar","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.1.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}]"#);

	// List.
	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg(pattern)
		.arg("--recursive")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"[{"tag":"test"},{"tag":"test/foo"},{"tag":"test/foo/bar"},{"tag":"test/foo/bar/baz","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/hello"},{"tag":"test/hello/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/world"},{"tag":"test/world/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/0.0.1"},{"tag":"test/0.0.1/foo"},{"tag":"test/0.0.1/foo/bar","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.1.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"},{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}]"#);

	// Get.
	let pattern = "test";
	let output = server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}"#);

	// Get.
	let pattern = "test/^1";
	let output = server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"tag":"test/1.2.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}"#);

	// Get.
	let pattern = "test/^10";
	let output = server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(pattern)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"tag":"test/10.0.0","item":"fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60"}"#);
}

#[tokio::test]
async fn remote_put() {
	let remote_server = Server::new(TG).await.unwrap();

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
	assert_success!(output);

	// Create a local server.
	let local_server = Server::new(TG).await.unwrap();
	let output = local_server
		.tg()
		.arg("remote")
		.arg("put")
		.arg("default")
		.arg(remote_server.url().to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

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
		.arg("--remote=default")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let local_output = local_server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(local_output);

	let remote_output = remote_server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(remote_output);

	let local_output =
		serde_json::from_slice::<tg::tag::get::Output>(&local_output.stdout).unwrap();
	let remote_output =
		serde_json::from_slice::<tg::tag::get::Output>(&remote_output.stdout).unwrap();
	assert_eq!(local_output.item, remote_output.item);
}

#[tokio::test]
async fn delete() {
	let server = Server::new(TG).await.unwrap();

	// Create and tag an artifact.
	let artifact: temp::Artifact = temp::file!("test").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Create a mix of leaf tags and nested structure.
	let tags = ["test/1.0.0", "test/2.0.0", "test/foo/bar", "test/foo/baz"];
	for tag in tags {
		let output = server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(tag)
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Delete with star.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/*")
		.output()
		.await
		.unwrap();
	assert_failure!(output);

	// Delete a leaf tag.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/1.0.0")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let delete_output: tg::tag::delete::Output = serde_json::from_slice(&output.stdout).unwrap();
	assert_eq!(delete_output.deleted.len(), 1);

	// Try to delete branch tag with children - should fail.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/foo")
		.output()
		.await
		.unwrap();
	assert_failure!(output);
	let stderr = std::str::from_utf8(&output.stderr).unwrap();
	assert!(stderr.contains("cannot delete branch tag"));

	// Delete one child leaf.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/foo/bar")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Still can't delete branch with remaining child.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/foo")
		.output()
		.await
		.unwrap();
	assert_failure!(output);

	// Delete remaining child.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/foo/baz")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Now we can delete empty branch.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("test/foo")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let delete_output: tg::tag::delete::Output = serde_json::from_slice(&output.stdout).unwrap();
	assert_eq!(delete_output.deleted.len(), 1);

	// Try to delete with empty pattern - should fail.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("")
		.output()
		.await
		.unwrap();
	assert_failure!(output);
	let stderr = std::str::from_utf8(&output.stderr).unwrap();
	assert!(stderr.contains("cannot delete an empty pattern"));
}

#[tokio::test]
async fn delete_recursive() {
	let server = Server::new(TG).await.unwrap();

	// Create and tag an artifact.
	let artifact: temp::Artifact = temp::file!("test").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Create a nested tag structure: test/a/b/c, test/a/b/d, test/a/e
	let tags = ["test/a/b/c", "test/a/b/d", "test/a/e"];
	for tag in tags {
		let output = server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(tag)
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Verify tags exist.
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg("test/*")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Recursively delete from the root - should delete all children in correct order.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("--recursive")
		.arg("test/*")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"deleted":["test/a/b/c","test/a/b/d","test/a/b","test/a/e","test/a"]}"#);

	// Verify all tags are deleted.
	let output = server
		.tg()
		.arg("tag")
		.arg("list")
		.arg("test/*")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"[]");
}

#[tokio::test]
async fn delete_recursive_deep_hierarchy() {
	let server = Server::new(TG).await.unwrap();

	// Create and tag an artifact.
	let artifact: temp::Artifact = temp::file!("test").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Create a deep hierarchy to test sorting by length.
	let tags = ["test/1/2/3/4/5"];
	for tag in tags {
		let output = server
			.tg()
			.arg("tag")
			.arg("put")
			.arg(tag)
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Recursively delete - should process in order from deepest to shallowest.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("--recursive")
		.arg("test/*")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	// Verify the order: longest paths first (children before parents).
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"{"deleted":["test/1/2/3/4/5","test/1/2/3/4","test/1/2/3","test/1/2","test/1"]}"#);
}

#[tokio::test]
async fn outdated() {
	// Create a server.
	let server = Server::new(TG).await.unwrap();

	// Write the artifact to a temp.
	let artifact: temp::Artifact = temp::file!("Hello, World!").into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	// Check in.
	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Tag it a couple times.
	for version in ["1.0.0", "1.1.0", "2.0.0"] {
		let output = server
			.tg()
			.arg("tag")
			.arg(format!("hello/{version}"))
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Create something that uses it.
	let artifact: temp::Artifact = temp::directory! {
		"tangram.ts" => temp::file!(indoc!(r#"
			import hello from "hello/^1.0";
		"#)),
	}
	.into();
	let temp = Temp::new();
	let path = temp.path();
	artifact.to_path(path).await.unwrap();

	// Get the outdated.
	let output = server
		.tg()
		.arg("outdated")
		.arg(path)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"[{"current":"hello/1.1.0","compatible":"hello/1.1.0","latest":"hello/2.0.0"}]"#);
}
