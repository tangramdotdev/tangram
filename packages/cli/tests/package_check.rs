use indoc::indoc;
use tangram_cli::test::{assert_failure, assert_success, test};
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
		assert_success!(output);
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
		assert_failure!(output);
	})
	.await;
}
