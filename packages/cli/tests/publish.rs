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

#[tokio::test]
async fn package_with_diamond_dependency() {
	let ctx = TestContext::new().await;

	// Create the bottom package (D) - no dependencies.
	let (_bottom_temp, bottom_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "I am the bottom of the diamond!";

			export let metadata = {
				name: "test-bottom",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the bottom package on the local server so it can be resolved.
	ctx.create_tag("test-bottom/1.0.0", &bottom_package_id)
		.await;

	// Create the left package (A) - depends on bottom.
	let (_left_temp, left_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import bottom from "test-bottom";

			export default () => `Left using: ${bottom()}`;

			export let metadata = {
				name: "test-left",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the left package on the local server so it can be resolved.
	ctx.create_tag("test-left/1.0.0", &left_package_id).await;

	// Create the right package (B) - depends on bottom.
	let (_right_temp, right_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import bottom from "test-bottom";

			export default () => `Right using: ${bottom()}`;

			export let metadata = {
				name: "test-right",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the right package on the local server so it can be resolved.
	ctx.create_tag("test-right/1.0.0", &right_package_id).await;

	// Create the main package - depends on both left and right.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import left from "test-left";
			import right from "test-right";

			export default () => `Main using: ${left()} and ${right()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Publish the main package - this should publish bottom, then left and right, then main.
	let publish_output = ctx.publish_with_output(&temp).await;

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let bottom_pos = stderr.find("publishing test-bottom/1.0.0");
	let left_pos = stderr.find("publishing test-left/1.0.0");
	let right_pos = stderr.find("publishing test-right/1.0.0");
	let main_pos = stderr.find("publishing test-main/1.0.0");

	// All packages should be published.
	assert!(bottom_pos.is_some(), "test-bottom should be published");
	assert!(left_pos.is_some(), "test-left should be published");
	assert!(right_pos.is_some(), "test-right should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify the order: bottom comes first.
	assert!(
		bottom_pos < left_pos,
		"test-bottom should be published before test-left"
	);
	assert!(
		bottom_pos < right_pos,
		"test-bottom should be published before test-right"
	);

	// Left and right can be in either order, but both must come before main.
	assert!(
		left_pos < main_pos,
		"test-left should be published before test-main"
	);
	assert!(
		right_pos < main_pos,
		"test-right should be published before test-main"
	);

	// Verify all four packages are tagged and synced on the remote.
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-left/1.0.0", &left_package_id)
		.await;
	ctx.assert_tag_on_remote("test-right/1.0.0", &right_package_id)
		.await;
	ctx.assert_tag_on_remote("test-bottom/1.0.0", &bottom_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&left_package_id).await;
	ctx.assert_object_synced(&right_package_id).await;
	ctx.assert_object_synced(&bottom_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&left_package_id).await;
	ctx.assert_metadata_synced(&right_package_id).await;
	ctx.assert_metadata_synced(&bottom_package_id).await;
}

#[tokio::test]
async fn package_with_diamond_dependency_and_shared_import() {
	let ctx = TestContext::new().await;

	// Create the bottom package (D) - no dependencies.
	let (_bottom_temp, bottom_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			export default () => "I am the bottom of the diamond!";

			export let metadata = {
				name: "test-bottom",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the bottom package on the local server so it can be resolved.
	ctx.create_tag("test-bottom/1.0.0", &bottom_package_id)
		.await;

	// Create the left package (A) - depends on bottom.
	let (_left_temp, left_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import bottom from "test-bottom";

			export default () => `Left using: ${bottom()}`;

			export let metadata = {
				name: "test-left",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the left package on the local server so it can be resolved.
	ctx.create_tag("test-left/1.0.0", &left_package_id).await;

	// Create the right package (B) - depends on bottom.
	let (_right_temp, right_package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import bottom from "test-bottom";

			export default () => `Right using: ${bottom()}`;

			export let metadata = {
				name: "test-right",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Create a tag for the right package on the local server so it can be resolved.
	ctx.create_tag("test-right/1.0.0", &right_package_id).await;

	// Create the main package - depends on left, right, AND bottom directly.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import left from "test-left";
			import right from "test-right";
			import bottom from "test-bottom";

			export default () => `Main using: ${left()}, ${right()}, and ${bottom()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Publish the main package - this should publish bottom, then left and right, then main.
	let publish_output = ctx.publish_with_output(&temp).await;

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let bottom_pos = stderr.find("publishing test-bottom/1.0.0");
	let left_pos = stderr.find("publishing test-left/1.0.0");
	let right_pos = stderr.find("publishing test-right/1.0.0");
	let main_pos = stderr.find("publishing test-main/1.0.0");

	// All packages should be published.
	assert!(bottom_pos.is_some(), "test-bottom should be published");
	assert!(left_pos.is_some(), "test-left should be published");
	assert!(right_pos.is_some(), "test-right should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify the order: bottom comes first.
	assert!(
		bottom_pos < left_pos,
		"test-bottom should be published before test-left"
	);
	assert!(
		bottom_pos < right_pos,
		"test-bottom should be published before test-right"
	);

	// Left and right can be in either order, but both must come before main.
	assert!(
		left_pos < main_pos,
		"test-left should be published before test-main"
	);
	assert!(
		right_pos < main_pos,
		"test-right should be published before test-main"
	);

	// Verify all four packages are tagged and synced on the remote.
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-left/1.0.0", &left_package_id)
		.await;
	ctx.assert_tag_on_remote("test-right/1.0.0", &right_package_id)
		.await;
	ctx.assert_tag_on_remote("test-bottom/1.0.0", &bottom_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&left_package_id).await;
	ctx.assert_object_synced(&right_package_id).await;
	ctx.assert_object_synced(&bottom_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&left_package_id).await;
	ctx.assert_metadata_synced(&right_package_id).await;
	ctx.assert_metadata_synced(&bottom_package_id).await;
}

#[tokio::test]
#[ignore = "pending unimplemented checkin feature for local path imports"]
async fn package_with_local_path_import() {
	let ctx = TestContext::new().await;

	// Create a shared temp directory with both packages as siblings.
	let shared_artifact = temp::directory! {
		"dep" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				export default () => "I am a local dependency!";

				export let metadata = {
					name: "test-dep",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import dep from "test-dep" with { local: "../dep" };

				export default () => `Main package using: ${dep()}`;

				export let metadata = {
					name: "test-main",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let dep_path = shared_temp.path().join("dep");
	let main_path = shared_temp.path().join("main");

	// Checkin the dep package to get its ID, but don't create a tag.
	let dep_checkin_output = ctx
		.local_server
		.tg()
		.current_dir(&dep_path)
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(dep_checkin_output);
	let dep_package_id = std::str::from_utf8(&dep_checkin_output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Checkin the main package to get its ID, but don't create a tag.
	let main_checkin_output = ctx
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(main_checkin_output);
	let main_package_id = std::str::from_utf8(&main_checkin_output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Publish the main package without having created tags beforehand.
	// This should discover the local dep, create its tag, publish it, then publish main.
	let publish_output = ctx
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("publish")
		.output()
		.await
		.unwrap();
	assert_success!(publish_output);

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let dep_pos = stderr.find("publishing test-dep/1.0.0");
	let main_pos = stderr.find("publishing test-main/1.0.0");

	// Both packages should be published.
	assert!(dep_pos.is_some(), "test-dep should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify the order: dep must be published before main.
	assert!(
		dep_pos < main_pos,
		"test-dep should be published before test-main"
	);

	// Verify both packages are tagged on local and remote servers.
	ctx.assert_tag_on_local("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_tag_on_local("test-main/1.0.0", &main_package_id)
		.await;
	ctx.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	ctx.assert_tag_on_remote("test-main/1.0.0", &main_package_id)
		.await;

	// Verify both packages are synced.
	ctx.assert_object_synced(&dep_package_id).await;
	ctx.assert_object_synced(&main_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&dep_package_id).await;
	ctx.assert_metadata_synced(&main_package_id).await;

	// Verify that main package has dependency on dep by tag, not by local path.
	let main_object_output = ctx
		.local_server
		.tg()
		.arg("get")
		.arg(&main_package_id)
		.arg("--format=tgon")
		.arg("--print-blobs")
		.arg("--print-depth=inf")
		.output()
		.await
		.unwrap();
	assert_success!(main_object_output);
	let main_object_tgon = std::str::from_utf8(&main_object_output.stdout).unwrap();
	dbg!(&main_object_tgon);

	// The object should NOT contain the local path reference.
	assert!(
		!main_object_tgon.contains("?local="),
		"main package should not reference dep by local path after publishing"
	);

	// The object should contain a reference to the dep tag.
	assert!(
		main_object_tgon.contains("test-dep"),
		"main package should reference dep by tag name"
	);
}

#[tokio::test]
async fn package_with_dependency_cycle() {
	let ctx = TestContext::new().await;

	// Create an import cycle but NOT a process cycle:
	let shared_artifact = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import b from "test-b" with { local: "../b" };
				export default () => `A using: ${b()}`;
				export let greeting = () => "Hello from A";
				export let metadata = {
					name: "test-a",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"b" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import * as a from "test-a" with { local: "../a" };
				export default () => `B using: ${a.greeting()}`;
				export let metadata = {
					name: "test-b",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let b_path = shared_temp.path().join("b");

	// Publish package B - this should detect the import cycle and handle it gracefully.
	let output = ctx
		.local_server
		.tg()
		.current_dir(&b_path)
		.arg("publish")
		.output()
		.await
		.unwrap();

	// Verify that publish succeeds - import cycles should be handled gracefully.
	assert_success!(output);

	// Extract the published IDs from the stderr
	let stderr = String::from_utf8_lossy(&output.stderr);
	let a_published_id = stderr
		.lines()
		.find(|line| line.contains("publishing test-a/1.0.0 (real package"))
		.and_then(|line| line.split("dir_").nth(1))
		.map(|s| format!("dir_{}", s.trim_end_matches(')')))
		.expect("test-a should be published");

	let b_published_id = stderr
		.lines()
		.find(|line| line.contains("publishing test-b/1.0.0 (real package"))
		.and_then(|line| line.split("dir_").nth(1))
		.map(|s| format!("dir_{}", s.trim_end_matches(')')))
		.expect("test-b should be published");

	// Verify both packages are tagged correctly.
	ctx.assert_tag_on_local("test-a/1.0.0", &a_published_id)
		.await;
	ctx.assert_tag_on_local("test-b/1.0.0", &b_published_id)
		.await;
	ctx.assert_tag_on_remote("test-a/1.0.0", &a_published_id)
		.await;
	ctx.assert_tag_on_remote("test-b/1.0.0", &b_published_id)
		.await;

	// Verify objects are synced to remote.
	ctx.assert_object_synced(&a_published_id).await;
	ctx.assert_object_synced(&b_published_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&a_published_id).await;
	ctx.assert_metadata_synced(&b_published_id).await;
}

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
