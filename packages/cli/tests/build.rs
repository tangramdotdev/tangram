use self::common::Server;
use futures::FutureExt;
use indoc::indoc;
use serde_json::json;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

mod common;

#[tokio::test]
async fn build_file() -> std::io::Result<()> {
	// Create a local servers.
	let local_config = json!({
		"vfs": null
	});
	let mut server = Server::start(local_config.clone()).await?;

	let result = AssertUnwindSafe(async {
		// Create a package foo
		let foo_temp = Temp::new();
		let foo = temp::directory! {
			"foo.tg.ts" => indoc!(r#"
				export default tg.target(() => "foo");
			"#),
		};
		let foo: temp::Artifact = foo.into();
		foo.to_path(foo_temp.as_ref()).await?;

		// Tag foo
		let output = server
			.tg()
			.args([
				"build",
				foo_temp.path().join("foo.tg.ts").to_str().unwrap(),
				"--quiet",
			])
			.output()
			.await?;
		assert!(output.status.success());

		Ok::<_, std::io::Error>(())
	})
	.catch_unwind()
	.await;

	server.cleanup().await.ok();
	result.unwrap()
}

#[tokio::test]
async fn import_from_remote_tag() -> std::io::Result<()> {
	// Create a remote.
	let remote_config = json!({
		"build": null,
		"remotes": null,
		"vfs": null
	});
	let mut remote = Server::start(remote_config).await?;

	// Create two local servers.
	let local_config = json!({
		"remotes": {
			"default": {
				"url": remote.url(),
			},
		},
		"vfs": null
	});
	let mut local1 = Server::start(local_config.clone()).await?;
	let mut local2 = Server::start(local_config).await?;

	let result = AssertUnwindSafe(async {
		// Create a package foo.
		let foo_temp = Temp::new();
		let foo = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(() => "foo");
			"#),
		};
		let foo: temp::Artifact = foo.into();
		foo.to_path(foo_temp.as_ref()).await?;
		let foo_path = foo_temp.path();

		// Tag foo.
		let output = local1
			.tg()
			.args(["tag", "foo", foo_path.to_str().unwrap()])
			.output()
			.await?;
		assert!(output.status.success(), "{output:?}");

		// Push the tag.
		let output = local1.tg().args(["push", "foo"]).output().await?;
		assert!(output.status.success(), "{output:?}");

		// Check if the remote has the tag.
		let output = remote.tg().args(["get", "foo"]).output().await?;
		assert!(output.status.success(), "{output:?}");

		// Create a package bar that imports foo.
		let bar_temp = Temp::new();
		let bar = temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "foo";
				export default tg.target(() => foo());
			"#),
		};
		let bar: temp::Artifact = bar.into();
		bar.to_path(bar_temp.as_ref()).await?;
		let bar_path = bar_temp.path();

		// Build bar.
		let output = local1
			.tg()
			.args(["build", bar_path.to_str().unwrap(), "--detach"])
			.output()
			.await?;
		let build: tg::build::Id = String::from_utf8(output.stdout)
			.unwrap()
			.trim()
			.parse()
			.unwrap();

		// Wait for the build to complete.
		let output = local1
			.tg()
			.args(["build", "output", build.to_string().as_str()])
			.output()
			.await?;
		assert!(output.status.success(), "{output:?}");

		// Push the build.
		let output = local1
			.tg()
			.args(["push", &build.to_string()])
			.output()
			.await?;
		assert!(output.status.success(), "{output:?}");

		// The remote should have this build.
		let output = remote
			.tg()
			.args(["get", &build.to_string()])
			.output()
			.await?;
		assert!(output.status.success(), "{output:?}");

		// Build again but expect a cache hit.
		let output = local2
			.tg()
			.arg("build")
			.arg(bar_temp.path())
			.arg("--detach")
			.arg("--create=false")
			.output()
			.await?;
		let new_build: tg::build::Id = String::from_utf8(output.stdout)
			.unwrap()
			.trim()
			.parse()
			.unwrap();
		assert_eq!(build, new_build);

		Ok::<_, std::io::Error>(())
	})
	.catch_unwind()
	.await;

	local1.cleanup().await.ok();
	local2.cleanup().await.ok();
	remote.cleanup().await.ok();

	result.unwrap()
}
