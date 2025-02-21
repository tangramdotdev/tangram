use indoc::indoc;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn push_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.file("Hello, World!")
			})
	"#),
	};
	test_object_push(directory).await;
}

#[tokio::test]
async fn push_simple_directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.directory({
					"hello.txt": tg.file("Hello, world!"),
					"subdirectory": tg.directory({
						"nested.txt": tg.file("I'm nested!")
					})
				})
			})
		"#)
	};
	test_object_push(directory).await;
}

async fn test_object_push(artifact: impl Into<temp::Artifact> + Send + 'static) {
	test(TG, async move |context| {
		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

		// Create a local server.
		let local_server = context.spawn_server().await.unwrap();
		let output = local_server
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(remote_server.url().to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let artifact: temp::Artifact = artifact.into();

		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		// Build the module.
		let output = local_server
			.tg()
			.arg("build")
			.arg(artifact_temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let id = std::str::from_utf8(&output.stdout).unwrap().trim();

		// Push the object.
		let output = local_server
			.tg()
			.arg("object")
			.arg("push")
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Confirm the object is on the remote and the same.
		let local_object_output = local_server
			.tg()
			.arg("object")
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
		assert_success!(local_object_output);
		let remote_object_output = remote_server
			.tg()
			.arg("object")
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
		assert_success!(remote_object_output);
		let local_object = std::str::from_utf8(&local_object_output.stdout).unwrap();
		let remote_object = std::str::from_utf8(&remote_object_output.stdout).unwrap();
		assert_eq!(local_object, remote_object);
	})
	.await;
}
