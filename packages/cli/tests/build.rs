use self::common::Server;
use futures::FutureExt;
use indoc::indoc;
use serde_json::json;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

mod common;

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

		// Tag foo.
		let status = local1
			.tg()
			.args(["tag", "foo", foo_temp.path().to_str().unwrap()])
			.status()
			.await?;
		assert!(status.success());

		// Push the tag.
		let status = local1.tg().args(["push", "foo"]).status().await?;
		assert!(status.success());

		// Check if the remote has a tag.
		let status = remote.tg().args(["get", "foo"]).status().await?;
		assert!(status.success());

		// Create a package bar that imports foo.
		let temp = Temp::new();
		let bar = temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "foo";
				export default tg.target(() => foo());
			"#),
		};
		let bar: temp::Artifact = bar.into();
		bar.to_path(temp.as_ref()).await?;

		// Build bar.
		let output = local1
			.tg()
			.args(["build", temp.path().to_str().unwrap(), "--detach"])
			.output()
			.await?;
		let build: tg::build::Id = String::from_utf8(output.stdout)
			.unwrap()
			.trim()
			.parse()
			.unwrap();

		// Wait for the build to complete.
		let status = local1
			.tg()
			.args(["build", "output", build.to_string().as_str()])
			.status()
			.await?;
		assert!(status.success());

		// Push the build.
		let status = local1
			.tg()
			.args(["push", &build.to_string()])
			.status()
			.await?;
		assert!(status.success());

		// The remote should have this build.
		let status = remote
			.tg()
			.args(["get", &build.to_string()])
			.status()
			.await?;
		assert!(status.success());

		// Build again but expect a cache hit.
		let output = local2
			.tg()
			.arg("build")
			.arg(temp.path())
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
