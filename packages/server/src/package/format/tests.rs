use crate::{Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Artifact, Temp};

#[tokio::test]
async fn test_path_dep() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		"foo.ts" => r#"export const foo = tg.target(() => tg.file("Hello, Foo!"))   ;"#,
		"bar.ts" => r#"export const bar = tg.target(() => tg.file("Hello, Bar!"))   ;"#,
		"baz" => temp::directory! {
			"tangram.ts" => r#"export const baz = tg.target(() => tg.file("Hello, Baz!"))   ;"#,
		},
		"tangram.ts" => r#"
				import * as baz from "./baz"  ;
				import foo from "./foo.ts"  ;
				import bar from "./bar.ts"  ;
			"#,
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to successfully format files with path dependencies"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{"bar.ts":{"File":{"contents":"export const bar = tg.target(() => tg.file(\"Hello, Bar!\"))   ;","executable":false}},"baz":{"Directory":{"entries":{"tangram.ts":{"File":{"contents":"export const baz = tg.target(() => tg.file(\"Hello, Baz!\"));\n","executable":false}}}}},"foo.ts":{"File":{"contents":"export const foo = tg.target(() => tg.file(\"Hello, Foo!\"))   ;","executable":false}},"tangram.ts":{"File":{"contents":"import * as baz from \"./baz\";\nimport foo from \"./foo.ts\";\nimport bar from \"./bar.ts\";\n","executable":false}}}}}"#
	);
	Ok(())
}

#[tokio::test]
async fn test_non_mod_path_dep() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		"tangram.ts" => r#"import patches from "./patches" with { type: "directory" }   ;"#,
		"patches" => temp::directory! {},
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to successfully format files with non-module path dependencies"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{"patches":{"Directory":{"entries":{}}},"tangram.ts":{"File":{"contents":"import patches from \"./patches\" with { type: \"directory\" };\n","executable":false}}}}}"#
	);
	Ok(())
}

#[tokio::test]
async fn test_no_root_mod() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		"not_tangram.ts" => r#"export default tg.target(() => tg.file("Hello, World!"))   ;"#,
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to succeed despite a missing root module"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{"not_tangram.ts":{"File":{"contents":"export default tg.target(() => tg.file(\"Hello, World!\"))   ;","executable":false}}}}}"#
	);
	Ok(())
}

#[tokio::test]
async fn test_multiple_root_mods() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		"tangram.ts" => r#"export default tg.target(() => tg.file("Hello, World!"))   ;"#,
		"tangram.js" => r#"export default tg.target(() => tg.file("Hello, World!"))   ;"#,
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to succeed despite multiple root modules"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{"tangram.js":{"File":{"contents":"export default tg.target(() => tg.file(\"Hello, World!\"));\n","executable":false}},"tangram.ts":{"File":{"contents":"export default tg.target(() => tg.file(\"Hello, World!\"));\n","executable":false}}}}}"#
	);
	Ok(())
}

#[tokio::test]
async fn test_invalid_syntax() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		"tangram.ts" => r#"export%de(((((((((((((fault tg.target(() => tg.file("Hello, World!"))   ;"#,
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to succeed in spite of invalid syntax"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{"tangram.ts":{"File":{"contents":"export\n%de(((((((((((((fault tg.target(() => tg.file(\"Hello, World!\"))\n","executable":false}}}}}"#
	);
	Ok(())
}

#[tokio::test]
async fn test_nested_dir_plus_ignore() -> tg::Result<()> {
	let output = run_format(temp::directory! {
		".tgignore" => indoc::formatdoc!(r"
			/foo/bar
			/foo/mod.tg.ts
		"),
		"foo" => temp::directory! {
			"foo.ts" => r#"export const foo = tg.target(() => tg.file("Hello, Foo!"))   ;"#,
			"bar.ts" => r#"export const bar = tg.target(() => tg.file("Hello, Bar!"))   ;"#,
			"mod.tg.ts" => r#"export const mod = tg.target(() => tg.file("Hello, Mod!"))   ;"#,
			"tangram.ts" => r#"export const baz = tg.target(() => tg.file("Hello, World!"))   ;"#,
			"bar" => temp::directory!{
				"tangram.ts" => r#"export const bar = tg.target(() => tg.file("Hello, Subdir!"))   ;"#,
				"mod.tg.ts" => r#"export const mod = tg.target(() => tg.file("Hello, Mod!"))   ;"#,
			},
		},
		"tangram.ts" => r#"
				import * as baz from "./baz"  ;
				import foo from "./foo.ts"  ;
				import bar from "./bar.ts"  ;
			"#,
	})
	.await;
	assert!(
		output.is_ok(),
		"expected the test to successfully format files with path dependencies"
	);
	assert_snapshot!(
		output.unwrap(),
		@r#"{"Directory":{"entries":{".tgignore":{"File":{"contents":"/foo/bar\n/foo/mod.tg.ts\n","executable":false}},"foo":{"Directory":{"entries":{"bar":{"Directory":{"entries":{"mod.tg.ts":{"File":{"contents":"export const mod = tg.target(() => tg.file(\"Hello, Mod!\"))   ;","executable":false}},"tangram.ts":{"File":{"contents":"export const bar = tg.target(() => tg.file(\"Hello, Subdir!\"))   ;","executable":false}}}}},"bar.ts":{"File":{"contents":"export const bar = tg.target(() => tg.file(\"Hello, Bar!\"))   ;","executable":false}},"foo.ts":{"File":{"contents":"export const foo = tg.target(() => tg.file(\"Hello, Foo!\"))   ;","executable":false}},"mod.tg.ts":{"File":{"contents":"export const mod = tg.target(() => tg.file(\"Hello, Mod!\"))   ;","executable":false}},"tangram.ts":{"File":{"contents":"export const baz = tg.target(() => tg.file(\"Hello, World!\"));\n","executable":false}}}}},"tangram.ts":{"File":{"contents":"import * as baz from \"./baz\";\nimport foo from \"./foo.ts\";\nimport bar from \"./bar.ts\";\n","executable":false}}}}}"#
	);
	Ok(())
}

async fn run_format(artifact: temp::Artifact) -> tg::Result<temp::Artifact> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
		let arg = tg::package::format::Arg {
			path: directory.to_path_buf(),
		};
		server.format_package(arg).await?;
		Artifact::from_path(directory.as_ref()).await
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
