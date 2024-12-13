use indoc::indoc;
use insta::assert_snapshot;
use serde_json::json;
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

	// Create a local server
	let local_config = json!({
		"remotes": {
			"default": {
				"url": remote.url()
			},
		},
		"vfs": null
	});
	let mut local = Server::start(local_config.clone()).await?;

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
	let foo_tag_output = local
		.tg()
		.args(["tag", "foo", foo_temp.path().to_str().unwrap()])
		.output()
		.await?;
	assert!(foo_tag_output.stdout.is_empty());
	assert!(foo_tag_output.stderr.is_empty());

	// Push the tag.
	let push_foo_tag_output = local.tg().args(["push", "foo"]).output().await?;
	assert!(push_foo_tag_output.stderr.is_empty());

	// Does the remote have the tag?
	let remote_foo_get_output = remote.tg().args(["get", "foo"]).output().await?;
	let remote_foo_get_output_stdout = String::from_utf8(remote_foo_get_output.stdout).unwrap();
	assert_snapshot!(remote_foo_get_output_stdout, @r#"{"entries":{"tangram.ts":"fil_010crryhp6zqjp0fsx9kyds5jmhq5zb5akvwbd579gexkyhr3kcmk0"}}"#);

	// Create a package bar that imports foo
	let bar_temp = Temp::new();
	let bar = temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "foo";
				export default tg.target(() => foo());
			"#),
	};
	let bar: temp::Artifact = bar.into();
	bar.to_path(bar_temp.as_ref()).await?;

	// Build bar.
	let first_build_bar_output = local
		.tg()
		.args(["build", bar_temp.path().to_str().unwrap(), "--detach"])
		.output()
		.await?;
	let first_build_bar_id = String::from_utf8(first_build_bar_output.stdout).unwrap();
	let first_build_bar_id = first_build_bar_id.trim().to_string();
	assert!(!first_build_bar_id.is_empty());

	// Push the build
	let push_bar_output = local
		.tg()
		.args(["push", &first_build_bar_id])
		.output()
		.await?;
	let push_bar_output_stderr = String::from_utf8(push_bar_output.stderr).unwrap();
	assert!(push_bar_output_stderr.is_empty());

	// The remote should have this build
	let remote_get_build_output = remote
		.tg()
		.args(["get", &first_build_bar_id])
		.output()
		.await?;
	let remote_get_build_output_stdout = String::from_utf8(remote_get_build_output.stdout).unwrap();
	eprintln!("{remote_get_build_output_stdout}");
	assert!(!remote_get_build_output_stdout.is_empty());

	// Create a fresh server with the same config.
	let mut fresh = Server::start(local_config).await?;

	// Build again but expect a cache hit.
	let fresh_build_bar_output = fresh
		.tg()
		.args(["build", bar_temp.path().to_str().unwrap(), "--create=false"])
		.output()
		.await?;
	let err = String::from_utf8(fresh_build_bar_output.stderr).unwrap();
	eprintln!("err: {err}");
	let fresh_build_bar_id = String::from_utf8(fresh_build_bar_output.stdout).unwrap();
	let fresh_build_bar_id = fresh_build_bar_id.trim().to_string();
	assert_eq!(fresh_build_bar_id, first_build_bar_id);

	local.cleanup().await?;
	fresh.cleanup().await?;
	remote.cleanup().await?;
	Ok(())
}
