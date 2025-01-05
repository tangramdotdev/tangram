use indoc::indoc;
use insta::assert_snapshot;
use std::future::Future;
use tangram_cli::test::test;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test building a module without a package.
#[tokio::test]
async fn build_module_without_package() -> tg::Result<()> {
	let directory = temp::directory! {
		"foo.tg.ts" => indoc!(r#"
			export default tg.target(() => "Hello, World!");
		"#),
	};
	let assertions = |output: String| async move {
		assert_snapshot!(output, @r#""Hello, World!""#);
		Ok(())
	};
	let path = "foo.tg.ts";
	let target = "default";
	let args = vec![];
	test_build(directory, path, target, args, assertions).await
}

#[tokio::test]
async fn hello_world() -> tg::Result<()> {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"export default tg.target(() => "Hello, World!")"#,
		}
	};
	let assertions = |output: String| async move {
		assert_snapshot!(output, @r###""Hello, World!""###);
		Ok(())
	};
	let args = vec![];
	let path = "foo";
	let target = "default";
	test_build(directory, path, target, args, assertions).await
}

#[tokio::test]
async fn accepts_target_with_no_return_value() -> tg::Result<()> {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r"export default tg.target(() => {})",
		}
	};
	let assertions = |output: String| async move {
		assert_snapshot!(output, @r"null");
		Ok(())
	};
	let path = "foo";
	let target = "default";
	let args = vec![];
	test_build(directory, path, target, args, assertions).await
}

#[tokio::test]
async fn accepts_arg() -> tg::Result<()> {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r"export default tg.target((name: string) => `Hello, ${name}!`)",
		}
	};
	let assertions = |output: String| async move {
		assert_snapshot!(output, @r###""Hello, Tangram!""###);
		Ok(())
	};
	let path = "foo";
	let target = "default";
	let args = vec![r#""Tangram""#.into()];
	test_build(directory, path, target, args, assertions).await
}

async fn test_build<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	target: &str,
	args: Vec<String>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(String) -> Fut + Send + 'static,
	Fut: Future<Output = tg::Result<()>> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = artifact.into();
		// Create a directory with a module.
		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.unwrap();

		let path = temp.path().join(path);
		let target = format!("{path}#{target}", path = path.display());

		// Build the module.
		let mut command = server.tg();
		command.arg("build").arg("--quiet").arg(target);
		for arg in args {
			command.arg("--arg");
			command.arg(arg);
		}
		let output = command.spawn().unwrap().wait_with_output().await.unwrap();
		dbg!(&output);
		assert!(output.status.success());

		// Assert the output.
		assertions(std::str::from_utf8(&output.stdout).unwrap().to_owned())
			.await
			.unwrap();
	})
	.await;
	Ok(())
}
