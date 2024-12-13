use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use indoc::indoc;
use insta::assert_snapshot;
use serde_json::json;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

mod common;
use common::Server;

#[tokio::test]
async fn import_from_remote_tag() -> std::io::Result<()> {
	// Create a remote
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
				"url": remote.url()
			},
		},
		"vfs": null
	});
	let mut local1 = Server::start(local_config.clone()).await?;
	let mut local2 = Server::start(local_config).await?;

	let result = AssertUnwindSafe(async {
		// Create a package foo
		let foo_temp = Temp::new();
		let foo = temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(() => "foo");
				"#),
		};
		let foo: temp::Artifact = foo.into();
		foo.to_path(foo_temp.as_ref()).await?;

		// Tag foo
		let output = local1
			.tg()
			.args(["tag", "foo", foo_temp.path().to_str().unwrap()])
			.output()
			.await?;
		let stderr = String::from_utf8(output.stderr).unwrap();
		assert_eq!(stderr, "");

		// Push the tag.
		let output = local1.tg().args(["push", "foo"]).output().await?;
		assert!(output.stderr.is_empty());

		// Check if the remote has a tag.
		let output = remote.tg().args(["get", "foo"]).output().await?;
		let stdout = String::from_utf8(output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"{"entries":{"tangram.ts":"fil_010crryhp6zqjp0fsx9kyds5jmhq5zb5akvwbd579gexkyhr3kcmk0"}}"#);

		// Create a package bar that imports foo
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
		let build_id: tg::build::Id = String::from_utf8(output.stdout)
			.unwrap()
			.trim()
			.parse()
			.unwrap();

		// Push the build
		let output = local1
			.tg()
			.args(["push", &build_id.to_string()])
			.output()
			.await?;
		let stderr = String::from_utf8(output.stderr).unwrap();
		assert_eq!(stderr, "");

		// The remote should have this build
		let output = remote
			.tg()
			.args(["get", &build_id.to_string()])
			.output()
			.await?;
		assert!(!output.stdout.is_empty());

		// Build again but expect a cache hit.
		let output = local2
			.tg()
			.arg("build")
			.arg(temp.path())
			.arg("--create=false")
			.output()
			.await?;
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert_eq!(stderr, "");
		let new_build_id: tg::build::Id = String::from_utf8(output.stdout).unwrap().trim().parse().unwrap();
		assert_eq!(build_id, new_build_id);

		Ok::<_, std::io::Error>(())
	})
	.catch_unwind()
	.await;

	local1.cleanup().await.ok();
	local2.cleanup().await.ok();
	remote.cleanup().await.ok();

	result.unwrap()
}
