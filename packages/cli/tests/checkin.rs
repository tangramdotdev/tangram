use indoc::indoc;
use tangram_cli::{assert_output_success, config::Config, test::test};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};
use tokio::io::AsyncWriteExt as _;

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test checking in a file with a lockfile in its xattrs.
#[tokio::test]
async fn checkin_locked_file() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let config = Config {
			remotes: None,
			..Default::default()
		};
		let server = context.spawn_server_with_config(config).await.unwrap();

		// Create a file with dependencies.
		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					const d = await tg.directory({
						entry: tg.file("inner")
					});
					const d_id = await d.id();
					const f = await tg.file("contents");
					return await f.id();
				});
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		let output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
		// let id: tg::file::Id = String::from_utf8(output.stdout).unwrap().parse().unwrap();

		// Check out the file with dependencies=false, lockfile=true.
		let checkout_temp = Temp::new();
		tokio::fs::create_dir_all(checkout_temp.path())
			.await
			.unwrap();
		// let checkout_output = server
		// 	.tg()
		// 	.arg("checkout")
		// 	.arg("--dependencies=false")
		// 	.arg("--lockfile=true")
		// 	.arg(id.to_string())
		// 	.arg(checkout_temp.path().join("orig"))
		// 	.output()
		// 	.await
		// 	.unwrap();
		// assert_output_success!(checkout_output);

		//
		// Check in the file
		//
		// Assert the contents and dependencies match the original.
	})
	.await;
}
