use indoc::indoc;
use insta::assert_snapshot;
use tangram_cli::{assert_output_success, test::test};
use tangram_temp::{self as temp, Temp};
use tokio::io::AsyncWriteExt as _;

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test building a module without a package.
#[tokio::test]
async fn build_module_without_package() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a directory with a module.
		let temp = Temp::new();
		let directory = temp::directory! {
			"foo.tg.ts" => indoc!(r#"
				export default tg.target(() => "Hello, World!");
			"#),
		};
		directory.to_path(temp.as_ref()).await.unwrap();

		// Build the module.
		let output = server
			.tg()
			.arg("build")
			.arg(temp.path().join("foo.tg.ts"))
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#""Hello, World!""#);
	})
	.await;
}
