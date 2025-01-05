use indoc::indoc;
use insta::assert_snapshot;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn get_symlink() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a directory with a module.
		let temp = Temp::new();
		let directory = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					let directory = await tg.directory({
						"hello.txt": "Hello, World!",
						"link": tg.symlink("hello.txt"),
					});
					return directory.get("link");
				});
			"#),
		};
		directory.to_path(temp.as_ref()).await.unwrap();

		// Build the module.
		let output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Assert the output.
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g");
	})
	.await;
}
#[tokio::test]
async fn get_file_through_symlink() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a directory with a module.
		let temp = Temp::new();
		let directory = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					let directory = await tg.directory({
						"subdirectory": {
							"hello.txt": "Hello, World!",
						},
						"link": tg.symlink("subdirectory"),
					});
					return directory.get("link/hello.txt");
				});
			"#),
		};
		directory.to_path(temp.as_ref()).await.unwrap();

		// Build the module.
		let output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g");
	})
	.await;
}
