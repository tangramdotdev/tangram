use {
	indoc::indoc,
	tangram_cli_test::{Server, assert_failure, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => "Hello, World!";
		"#),
	}
	.into();
	let reference = ".";
	let output = test(artifact, reference).await;
	assert_success!(output);
}

#[tokio::test]
async fn hello_world_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => tg.file("Hello, World!");
		"#),
	}
	.into();
	let reference = ".";
	let output = test(artifact, reference).await;
	assert_success!(output);
}

#[tokio::test]
async fn nonexistent_function() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => foo();
		"),
	}
	.into();
	let reference = ".";
	let output = test(artifact, reference).await;
	assert_failure!(output);
}

#[tokio::test]
async fn blob_template_rejects_non_strings() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./file.txt";
			export default () => tg.blob`\n\t${file}\n`;
		"#),
		"file.txt" => "Hello, world!"
	}
	.into();
	let reference = ".";
	let output = test(artifact, reference).await;
	assert_failure!(output);
}

#[tokio::test]
async fn file_template_rejects_non_strings() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./file.txt";
			export default () => tg.file`\n\t${file}\n`;
		"#),
		"file.txt" => "Hello, world!"
	}
	.into();
	let reference = ".";
	let output = test(artifact, reference).await;
	assert_failure!(output);
}

async fn test(artifact: temp::Artifact, reference: &str) -> std::process::Output {
	let server = Server::new(TG).await.unwrap();

	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();

	let mut command = server.tg();
	command.current_dir(temp.path()).arg("check").arg(reference);
	command.output().await.unwrap()
}
