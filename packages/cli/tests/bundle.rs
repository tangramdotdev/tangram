use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::future::Future;
use tangram_cli::test::test;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
/// Test bundling a file with no dependencies.
async fn file_no_dependencies_js() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let build = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					let file = await tg.file("hello");
					return tg.bundle(file);
				});
		"#),
		};
		let artifact: temp::Artifact = build.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		// Build the module.
		let output = server
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

		let temp = Temp::new();
		let path = temp.path().to_owned();

		// Check out the artifact.
		let output = server
			.tg()
			.arg("checkout")
			.arg(id)
			.arg(path)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let artifact = temp::Artifact::with_path(temp.path()).await.unwrap();

		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "hello",
    "executable": false
  }
  "#);
	})
	.await;
	Ok(())
}

#[tokio::test]
/// Test bundling a file with no dependencies.
async fn file_no_dependencies() -> tg::Result<()> {
	let file = temp::file!("hello!");
	let assertions = |object: String| async move {
		dbg!(&object);
		assert_snapshot!(object, @r#""#);
		Ok::<_, tg::Error>(())
	};
	test_bundle(file, assertions).await
}

// /// Test bundling a directory that contains no files with dependencies
// #[tokio::test]
// async fn directory_no_dependencies() -> tg::Result<()> {
// 	todo!()
// }

// /// Test bundling an executable file with a dependency.
// #[tokio::test]
// async fn executable_file_with_dependency() -> tg::Result<()> {
// 	todo!()
// }

// /// Test bundling a directory that contains files with dependencies.
// #[tokio::test]
// async fn directory_containing_file_with_file_dependency() -> tg::Result<()> {
// 	todo!()
// }

// /// Test bundling dependencies that contain target symlinks.
// #[tokio::test]
// async fn directory_containing_file_with_directory_dependency_target_symlink() -> tg::Result<()> {
// 	todo!()
// }

// /// Test bundling dependencies that contain artifact/path symlinks.
// #[tokio::test]
// async fn directory_containing_file_with_directory_dependency_artifact_path_symlink(
// ) -> tg::Result<()> {
// 	todo!()
// }

async fn test_bundle<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(String) -> Fut + Send + 'static,
	Fut: Future<Output = tg::Result<()>> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		// Write the artifact to a temp.
		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let path = temp.path();

		// Check in.
		let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
		assert!(output.status.success());

		// Get the object.
		let id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();
		let output = server
			.tg()
			.arg("artifact")
			.arg("bundle")
			.arg(id.clone())
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		assertions(std::str::from_utf8(&output.stdout).unwrap().to_owned());
	})
	.await;
	Ok(())
}
