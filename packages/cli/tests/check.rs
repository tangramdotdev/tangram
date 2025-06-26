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
	let reference = ".";
	test_check(directory, reference, assertions).await;
}

#[tokio::test]
async fn hello_world_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => tg.file("Hello, World!");
		"#),
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
	};
	let reference = ".";
	test_check(directory, reference, assertions).await;
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
	let reference = ".";
	test_check(directory, reference, assertions).await;
}

#[tokio::test]
async fn blob_template_rejects_non_strings() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./file.txt";
			export default () => tg.blob`\n\t${file}\n`;
		"#),
		"file.txt" => "Hello, world!"
	};
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	let reference = ".";
	test_check(directory, reference, assertions).await;
}

#[tokio::test]
async fn file_template_rejects_non_strings() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./file.txt";
			export default () => tg.file`\n\t${file}\n`;
		"#),
		"file.txt" => "Hello, world!"
	};
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	let reference = ".";
	test_check(directory, reference, assertions).await;
}

async fn test_check<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	reference: &str,
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

		let mut command = server.tg();
		command.current_dir(temp.path()).arg("check").arg(reference);
		let output = command.output().await.unwrap();

		assertions(output).await;
	})
	.await;
}
