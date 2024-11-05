use crate::{Config, Server};
use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn test_path_dep() -> tg::Result<()> {
	assert!(
		test(temp::directory! {
			"foo.ts" => r#"export const foo = tg.target(() => tg.file("Hello, Foo!"));"#,
			"bar.ts" => r#"export const bar = tg.target(() => tg.file("Hello, Bar!"));"#,
			"baz" => temp::directory! {
				"tangram.ts" => r#"export const baz = tg.target(() => tg.file("Hello, Baz!"));"#,

			},
			"tangram.ts" => r"
				import * as baz from ./baz;
				import foo from ./foo.ts;
				import bar from ./bar.ts;
			",
		})
		.await
		.is_ok(),
		"expected the test to successfully format files with path dependencies"
	);
	Ok(())
}

#[tokio::test]
async fn test_non_mod_path_dep() -> tg::Result<()> {
	assert!(
		test(temp::directory! {
			"tangram.ts" => r#"import patches from "./patches" with { type: "directory" };"#,
			"patches" => temp::directory! {},
		})
		.await
		.is_ok(),
		"expected the test to successfully format files with non-module path dependencies"
	);
	Ok(())
}

#[tokio::test]
async fn test_no_root_mod() -> tg::Result<()> {
	assert!(
		test(temp::directory! {
			"not_tangram.ts" => r#"export default tg.target(() => tg.file("Hello, World!"));"#,
		})
		.await
		.is_err(),
		"expected the test to fail due to a missing root module"
	);
	Ok(())
}

#[tokio::test]
async fn test_multiple_root_mods() -> tg::Result<()> {
	assert!(
		test(temp::directory! {
			"tangram.ts" => r#"export default tg.target(() => tg.file("Hello, World!"));"#,
			"tangram.js" => r#"export default tg.target(() => tg.file("Hello, World!"));"#,
		})
		.await
		.is_err(),
		"expected the test to panic due to multiple root modules"
	);
	Ok(())
}

#[tokio::test]
async fn test_invalid_syntax() -> tg::Result<()> {
	assert!(
		test(temp::directory! {
			"tangram.ts" => r#"export%de(((((((((((((fault tg.target(() => tg.file("Hello, World!"));"#,
		})
		.await
		.is_ok(),
		"expected the test to succeed in spite of invalid syntax"
	);
	Ok(())
}

async fn test(artifact: temp::Artifact) -> tg::Result<()> {
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
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
