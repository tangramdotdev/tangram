use indoc::indoc;
use tangram_cli_test::{Server, assert_success};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn push_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("Hello, World!")
			}
		"#),
	}
	.into();
	test(artifact).await;
}

#[tokio::test]
async fn push_simple_directory() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": tg.file("Hello, world!"),
					"subdirectory": tg.directory({
						"nested.txt": tg.file("I'm nested!")
					})
				})
			}
		"#)
	}
	.into();
	test(artifact).await;
}

async fn test(artifact: temp::Artifact) {
	// Create a remote server.
	let remote_server = Server::new(TG).await.unwrap();

	// Create a local server.
	let local_server = Server::new(TG).await.unwrap();
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
		.arg("push")
		.arg(id)
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Confirm the object is on the remote and the same.
	let local_object_output = local_server
		.tg()
		.arg("get")
		.arg(id)
		.arg("--format=tgon")
		.arg("--print-blobs")
		.arg("--print-depth=inf")
		.arg("--print-pretty=true")
		.output()
		.await
		.unwrap();
	assert_success!(local_object_output);
	let remote_object_output = remote_server
		.tg()
		.arg("get")
		.arg(id)
		.arg("--format=tgon")
		.arg("--print-blobs")
		.arg("--print-depth=inf")
		.arg("--print-pretty=true")
		.output()
		.await
		.unwrap();
	assert_success!(remote_object_output);
	let local_object = std::str::from_utf8(&local_object_output.stdout).unwrap();
	let remote_object = std::str::from_utf8(&remote_object_output.stdout).unwrap();
	assert_eq!(local_object, remote_object);

	// Get the metadata.
	let local_metadata_output = local_server
		.tg()
		.arg("object")
		.arg("metadata")
		.arg(id)
		.arg("--pretty")
		.arg("true")
		.output()
		.await
		.unwrap();
	let _ = remote_server
	.tg()
	.arg("index").output().await;
	let remote_metadata_output = remote_server
		.tg()
		.arg("object")
		.arg("metadata")
		.arg(id)
		.arg("--pretty")
		.arg("true")
		.output()
		.await
		.unwrap();
	assert_eq!(local_metadata_output, remote_metadata_output);
}
