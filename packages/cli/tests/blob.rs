use insta::assert_snapshot;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn create_from_file() {
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let temp = Temp::new();
		let artifact: temp::Artifact = temp::file!("hello, world!\n").into();
		artifact.to_path(temp.path()).await.unwrap();

		let mut file = tokio::fs::File::open(temp.path()).await.unwrap();

		let mut child = server
			.tg()
			.arg("blob")
			.arg("create")
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
			.arg("--format")
			.arg("tgvn")
			.arg("--pretty")
			.arg("true")
			.arg("--recursive")
			.output()
			.await
			.unwrap();
		assert_success!(output);
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @r#"tg.leaf("hello, world!\n")"#);
	})
	.await;
}
