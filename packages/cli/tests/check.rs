use indoc::indoc;
use tangram_cli::test::{assert_failure, assert_success, test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "Hello, World!";
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

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
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r"
				export default () => foo();
			"),
		};
		package.to_path(temp.as_ref()).await.unwrap();

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
