use {
	insta::assert_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn create_from_file() {
	let server = Server::new(TG).await.unwrap();

	let temp = Temp::new();
	let artifact: temp::Artifact = temp::file!("hello, world!\n").into();
	artifact.to_path(temp.path()).await.unwrap();

	let mut file = tokio::fs::File::open(temp.path()).await.unwrap();

	let mut child = server
		.tg()
		.arg("write")
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped())
		.spawn()
		.unwrap();
	let mut stdin = child.stdin.take().expect("Failed to get stdin");
	tokio::io::copy(&mut file, &mut stdin).await.unwrap();
	drop(stdin);
	let output = child.wait_with_output().await.unwrap();
	assert_success!(output);

	let id = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();
	let output = server
		.tg()
		.arg("get")
		.arg(id)
		.arg("--blobs")
		.arg("--depth=inf")
		.arg("--pretty")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.blob("hello, world!\n")"#);
}
