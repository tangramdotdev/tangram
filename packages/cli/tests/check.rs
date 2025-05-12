use indoc::indoc;
use tangram_cli::test::{assert_failure, assert_success, test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => "Hello, World!";
		"#),
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
	};
	let path = "";
	test_check(directory, path, assertions).await;
}

#[tokio::test]
async fn nonexistent_function() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => foo();
		"),
	};
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	let path = "";
	test_check(directory, path, assertions).await;
}

#[tokio::test]
async fn blob_template_rejects_non_strings() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file from "./file.txt";
			export default () => tg.blob`\n\t${file}\n`;
		"#,
		"file.txt" => "Hello, world!"
	};
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	let path = "";
	test_check(directory, path, assertions).await;
}

#[tokio::test]
async fn file_template_rejects_non_strings() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file from "./file.txt";
			export default () => tg.file`\n\t${file}\n`;
		"#,
		"file.txt" => "Hello, world!"
	};
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	let path = "";
	test_check(directory, path, assertions).await;
}

async fn test_check<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	assertions: F,
) where
	F: FnOnce(std::process::Output) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.unwrap();

		let path = temp.path().join(path);
		let command_ = format!("{path}", path = path.display());

		let mut command = server.tg();
		command.arg("check").arg(command_);
		let output = command.output().await.unwrap();

		assertions(output).await;
	})
	.await;
}
