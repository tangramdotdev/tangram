use {
	indoc::indoc,
	tangram_cli_test::{Server, assert_success},
	tangram_client as tg,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn simple_package() {
	let ctx = TestContext::new().await;

	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "Hello, World!";

			export let metadata = {
				name: "test-pkg",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	ctx.publish(&temp).await;

	let tag = "test-pkg/1.0.0";
	ctx.assert_tag_on_local(tag, &package_id).await;
	ctx.assert_tag_on_remote(tag, &package_id).await;
	ctx.assert_object_synced(&package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn package_with_dependency_pre_publish() {
	let ctx = TestContext::new().await;

	// Create and publish the dependency package.
	let (dep_temp, dep_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "I am a dependency!";

			export let metadata = {
				name: "test-dep",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;
	ctx.publish(&dep_temp).await;

	// Create a package that depends on the first package.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;
	ctx.publish(&temp).await;

	// Verify tags and objects for both packages.
	ctx.assert_tag_on_local("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_local("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&dep_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&dep_package_id).await;
}

#[tokio::test]
async fn package_with_unpublished_dependency() {
	let ctx = TestContext::new().await;

	// Create a dependency package but DON'T publish it yet.
	let (_dep_temp, dep_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "I am a dependency!";

			export let metadata = {
				name: "test-dep",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the dependency on the local server so it can be resolved.
	ctx.create_tag("test-dep/1.0.0", &dep_package_id).await;

	// Create a package that depends on the unpublished package.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Publish the main package - this should also publish the dependency.
	ctx.publish(&temp).await;

	// Verify both packages are tagged and synced on the remote.
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&dep_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&dep_package_id).await;
}

#[tokio::test]
async fn package_with_transitive_dependency() {
	let ctx = TestContext::new().await;

	// Create the transitive dependency (C) - no dependencies.
	let (_transitive_temp, transitive_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "I am the transitive dependency!";

			export let metadata = {
				name: "test-transitive",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the transitive dependency on the local server so it can be resolved.
	ctx.create_tag("test-transitive/1.0.0", &transitive_package_id)
		.await;

	// Create the intermediate dependency (B) - depends on C.
	let (_dep_temp, dep_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import transitive from "test-transitive";

			export default () => `Dependency using: ${transitive()}`;

			export let metadata = {
				name: "test-dep",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the dependency on the local server so it can be resolved.
	ctx.create_tag("test-dep/1.0.0", &dep_package_id).await;

	// Create the main package (A) - depends on B.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Publish the main package - this should publish C, then B, then A.
	let publish_output = ctx.publish_with_output(&temp).await;

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let transitive_pos = stderr.find("publishing test-transitive/1.0.0");
	let dep_pos = stderr.find("publishing test-dep/1.0.0");
	let main_pos = stderr.find("publishing test-main/1.0.0");

	// All packages should be published.
	assert!(
		transitive_pos.is_some(),
		"test-transitive should be published"
	);
	assert!(dep_pos.is_some(), "test-dep should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify the order: transitive < dep < main.
	assert!(
		transitive_pos < dep_pos,
		"test-transitive should be published before test-dep"
	);
	assert!(
		dep_pos < main_pos,
		"test-dep should be published before test-main"
	);

	// Verify all three packages are tagged and synced on the remote.
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_tag_on_remote("test-transitive/1.0.0", &transitive_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&dep_package_id).await;
	ctx.assert_object_synced(&transitive_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&dep_package_id).await;
	ctx.assert_metadata_synced(&transitive_package_id).await;
}

// TODO - multiple dependencies, some of which have transitive dependencies which may be shared.
// TODO - dependency cycle.
//
struct TestContext {
	local_server: Server,
	remote_server: Server,
}

impl TestContext {
	async fn new() -> Self {
		let remote_server = Server::new(TG).await.unwrap();
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
		Self {
			local_server,
			remote_server,
		}
	}

	async fn create_package(&self, content: String) -> (Temp, String) {
		let package = temp::directory! {
			"tangram.ts" => content,
		};
		let artifact: temp::Artifact = package.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();

		let output = self
			.local_server
			.tg()
			.current_dir(temp.path())
			.arg("checkin")
			.arg(".")
			.output()
			.await
			.unwrap();
		assert_success!(output);
		let package_id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();

		(temp, package_id)
	}

	async fn publish(&self, temp: &Temp) {
		let output = self.publish_with_output(temp).await;
		assert_success!(output);
	}

	async fn publish_with_output(&self, temp: &Temp) -> std::process::Output {
		self.local_server
			.tg()
			.current_dir(temp.path())
			.arg("publish")
			.arg("--remote=default")
			.output()
			.await
			.unwrap()
	}

	async fn create_tag(&self, tag: &str, id: &str) {
		let output = self
			.local_server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(id)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	async fn assert_tag_on_local(&self, tag: &str, expected_id: &str) {
		let output = self
			.local_server
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
			Some(expected_id.to_string())
		);
	}

	async fn assert_tag_on_remote(&self, tag: &str, expected_id: &str) {
		let output = self
			.remote_server
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
			Some(expected_id.to_string())
		);
	}

	async fn assert_object_synced(&self, package_id: &str) {
		let local_object_output = self
			.local_server
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
		let remote_object_output = self
			.remote_server
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
	}

	async fn index_servers(&self) {
		let output = self.local_server.tg().arg("index").output().await.unwrap();
		assert_success!(output);
		let output = self.remote_server.tg().arg("index").output().await.unwrap();
		assert_success!(output);
	}

	async fn assert_metadata_synced(&self, package_id: &str) {
		let local_metadata_output = self
			.local_server
			.tg()
			.arg("object")
			.arg("metadata")
			.arg(package_id)
			.arg("--pretty")
			.arg("true")
			.output()
			.await
			.unwrap();
		let remote_metadata_output = self
			.remote_server
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
}
