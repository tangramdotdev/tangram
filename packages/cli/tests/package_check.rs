use indoc::indoc;
use insta::assert_json_snapshot;
use tangram_cli::test::test;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a package.
		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(() => "Hello, World!");
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		// Format the package.
		let output = server
			.tg()
			.arg("check")
			.arg(temp.path())
			.output()
			.await
			.unwrap();

		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_json_snapshot!(stdout, @r#""""#);
		assert_json_snapshot!(stderr, @r#""""#);
	})
	.await;
}

#[tokio::test]
async fn nonexistent_function() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a package.
		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r"
				export default tg.target(() => foo());
			"),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		// Format the package.
		let output = server
			.tg()
			.arg("check")
			.arg(temp.path())
			.output()
			.await
			.unwrap();

		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_json_snapshot!(stdout, @r#""""#);
		assert_json_snapshot!(stderr, @r#""\u001b[38;5;9m\u001b[1merror\u001b[0m: Cannot find name 'foo'.\n   \u001b[38;5;11mdir_01sb97esva2e75cyaar1szc6835pcvkzd0eb486m5fmye3etcd3nc0:tangram.ts:1:32\u001b[39m\n\u001b[38;5;9m\u001b[1merror\u001b[0m failed to run the command\n\u001b[38;5;9m->\u001b[39m type checking failed\n""#);
	})
	.await;
}
