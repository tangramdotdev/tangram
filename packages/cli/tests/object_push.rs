use indoc::indoc;
use tangram_cli::test::test;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn push_file() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.file("Hello, World!")
			})
	"#),
	};
	test_object_push(build).await?;
	Ok(())
}

#[tokio::test]
async fn push_simple_directory() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.directory({
					"hello.txt": tg.file("Hello, world!"),
					"subdirectory": tg.directory({
						"nested.txt": tg.file("I'm nested!")
					})
				})
			})
		"#)
	};
	test_object_push(build).await?;
	Ok(())
}

async fn test_object_push(artifact: impl Into<temp::Artifact> + Send + 'static) -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
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
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let artifact: temp::Artifact = artifact.into();

		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		// Build the module.
		let output = local_server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path())
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let id = std::str::from_utf8(&output.stdout).unwrap().trim();

		// Push the object.
		let output = local_server
			.tg()
			.arg("object")
			.arg("push")
			.arg(id)
			.arg("--server")
			.arg(remote_server.url().to_string())
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Confirm the object is on the remote and the same.
		let local_object_output = local_server
			.tg()
			.arg("object")
			.arg("get")
			.arg(id)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(local_object_output.status.success());
		let remote_object_output = remote_server
			.tg()
			.arg("object")
			.arg("get")
			.arg(id)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(remote_object_output.status.success());
		let local_object = std::str::from_utf8(&local_object_output.stdout).unwrap();
		let remote_object = std::str::from_utf8(&remote_object_output.stdout).unwrap();
		assert_eq!(local_object, remote_object);
	})
	.await;
	Ok(())
}
