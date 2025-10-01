use {
	indoc::indoc,
	tangram_cli_test::{Server, assert_success},
	tangram_client as tg,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn simple_package() {
	// Create a remote server.
	let remote_server = Server::new(TG).await.unwrap();

	// Create a local server and set up the remote.
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

	// Create a simple package with metadata export.
	let package = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => "Hello, World!";

			export let metadata = {
				name: "test-pkg",
				version: "1.0.0",
			};
		"#),
	};
	let artifact: temp::Artifact = package.into();
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();

	// Check in the package to get its ID.
	let output = local_server
		.tg()
		.current_dir(temp.path())
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let package_id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Run tg publish.
	let output = local_server
		.tg()
		.current_dir(temp.path())
		.arg("publish")
		.arg("--remote=default")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Verify the tag was created on the local.
	let tag = "test-pkg/1.0.0";
	let output = local_server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let tag_output = serde_json::from_slice::<tg::tag::get::Output>(&output.stdout).unwrap();
	assert_eq!(
		tag_output.item.map(|item| item.to_string()),
		Some(package_id.to_string())
	);

	// Verify the tag was created on the remote.
	let output = remote_server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let tag_output = serde_json::from_slice::<tg::tag::get::Output>(&output.stdout).unwrap();

	// Verify the tag points to the correct item.
	assert_eq!(
		tag_output.item.map(|item| item.to_string()),
		Some(package_id.to_string())
	);

	// Confirm the object is on the remote and the same.
	let local_object_output = local_server
		.tg()
		.arg("get")
		.arg(package_id)
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
		.arg(package_id)
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

	// Index.
	let output = local_server.tg().arg("index").output().await.unwrap();
	assert_success!(output);
	let output = remote_server.tg().arg("index").output().await.unwrap();
	assert_success!(output);

	// Verify the metadata on the local and the remote match.
	let local_metadata_output = local_server
		.tg()
		.arg("object")
		.arg("metadata")
		.arg(package_id)
		.arg("--pretty")
		.arg("true")
		.output()
		.await
		.unwrap();
	let _ = remote_server.tg().arg("index").output().await;
	let remote_metadata_output = remote_server
		.tg()
		.arg("object")
		.arg("metadata")
		.arg(package_id)
		.arg("--pretty")
		.arg("true")
		.output()
		.await
		.unwrap();
	assert_eq!(local_metadata_output, remote_metadata_output);
}

// TODO - one dependency
// TODO - one dependency which in turn has a dependency
// TODO - multiple dependencies, some of which have transitive dependencies which may be shared.
// TODO - dependency cycle.
