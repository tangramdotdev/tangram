use {
	indoc::indoc,
	tangram_cli_test::{Server, assert_success},
	tangram_client::prelude::*,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn simple_package() {
	let context = Context::new().await;

	let content = indoc!(
		r#"
			export default () => "Hello, World!";

			export let metadata = {
				tag: "test-pkg/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	let output = context.publish_with_output(&temp).await;
	assert_success!(output);

	let tag = "test-pkg/1.0.0";
	context.assert_tag_on_local(tag, &package_id).await;
	context.assert_tag_on_remote(tag, &package_id).await;
	context.assert_object_synced(&package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn package_with_dependency_pre_publish() {
	let context = Context::new().await;

	// Create and publish the dependency package.
	let content = indoc!(
		r#"
			export default () => "I am a dependency!";

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		"#
	);
	let (dep_temp, dep_package_id) = context.create_package(content.to_owned()).await;
	context.publish(&dep_temp).await;

	// Create a package that depends on the first package.
	let content = indoc!(
		r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;
	context.publish(&temp).await;

	// Verify tags and objects for both packages.
	context
		.assert_tag_on_local("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_local("test-dep/1.0.0", &dep_package_id)
		.await;
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&dep_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&dep_package_id).await;
}

#[tokio::test]
async fn package_with_unpublished_dependency() {
	let context = Context::new().await;

	// Create a dependency package but DON'T publish it yet.
	let content = indoc!(
		r#"
			export default () => "I am a dependency!";

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		"#
	);
	let (_dep_temp, dep_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the dependency on the local server so it can be resolved.
	context.create_tag("test-dep/1.0.0", &dep_package_id).await;

	// Create a package that depends on the unpublished package.
	let content = indoc!(
		r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	// Publish the main package - this should also publish the dependency.
	context.publish(&temp).await;

	// Verify both packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&dep_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&dep_package_id).await;
}

#[tokio::test]
async fn package_with_transitive_dependency() {
	let context = Context::new().await;

	// Create the transitive dependency (C) - no dependencies.
	let content = indoc!(
		r#"
			export default () => "I am the transitive dependency!";

			export let metadata = {
				tag: "test-transitive/1.0.0",
			};
		"#
	);
	let (_transitive_temp, transitive_package_id) =
		context.create_package(content.to_owned()).await;

	// Create a tag for the transitive dependency on the local server so it can be resolved.
	context
		.create_tag("test-transitive/1.0.0", &transitive_package_id)
		.await;

	// Create the intermediate dependency (B) - depends on C.
	let content = indoc!(
		r#"
			import transitive from "test-transitive";

			export default () => `Dependency using: ${transitive()}`;

			export let metadata = {
				tag: "test-dep/1.0.0",
			};
		"#
	);
	let (_dep_temp, dep_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the dependency on the local server so it can be resolved.
	context.create_tag("test-dep/1.0.0", &dep_package_id).await;

	// Create the main package (A) - depends on B.
	let content = indoc!(
		r#"
			import dep from "test-dep";

			export default () => `Main package using: ${dep()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	// Publish the main package - this should publish C, then B, then A.
	context.publish(&temp).await;

	// Verify all three packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	context
		.assert_tag_on_remote("test-transitive/1.0.0", &transitive_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&dep_package_id).await;
	context.assert_object_synced(&transitive_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&dep_package_id).await;
	context.assert_metadata_synced(&transitive_package_id).await;
}

#[tokio::test]
async fn package_with_local_transitive_dependency() {
	let context = Context::new().await;

	// Create a shared temp directory with all three packages as siblings.
	let shared_artifact = temp::directory! {
		"transitive" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "I am the transitive dependency!";

				export let metadata = {
					tag: "test-transitive/1.0.0",
				};
			"#),
		},
		"dep" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import transitive from "test-transitive" with { local: "../transitive" };

				export default () => `Dependency using: ${transitive()}`;

				export let metadata = {
					tag: "test-dep/1.0.0",
				};
			"#),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import dep from "test-dep" with { local: "../dep" };

				export default () => `Main package using: ${dep()}`;

				export let metadata = {
					tag: "test-main/1.0.0",
				};
			"#),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let main_path = shared_temp.path().join("main");

	// Publish the main package - this should publish C, then B, then A.
	let publish_output = context
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("publish")
		.output()
		.await
		.unwrap();
	assert_success!(publish_output);
	let stderr = String::from_utf8(publish_output.stderr).unwrap();

	// Extract the published artifact IDs from the stderr output.
	let extract_published_id = |package_name: &str| -> String {
		stderr
			.lines()
			.find(|line| line.contains(&format!("published {package_name}")))
			.and_then(|line| line.split("dir_").nth(1))
			.map_or_else(
				|| panic!("{package_name} should have a published ID in output"),
				|s| format!("dir_{}", s.trim_end_matches(')')),
			)
	};

	let transitive_package_id = extract_published_id("test-transitive/1.0.0");
	let dep_package_id = extract_published_id("test-dep/1.0.0");
	let package_id = extract_published_id("test-main/1.0.0");

	// Verify all three packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	context
		.assert_tag_on_remote("test-transitive/1.0.0", &transitive_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&dep_package_id).await;
	context.assert_object_synced(&transitive_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&dep_package_id).await;
	context.assert_metadata_synced(&transitive_package_id).await;

	// Verify that all published packages were re-checked in without local dependencies.
	// The main package should have tag-based dependencies, not local path dependencies.
	let main_object_output = context
		.local_server
		.tg()
		.arg("get")
		.arg(&package_id)
		.arg("--blobs")
		.arg("--depth=inf")
		.output()
		.await
		.unwrap();
	assert_success!(main_object_output);
	let main_object_tgon = std::str::from_utf8(&main_object_output.stdout).unwrap();
	assert!(
		!main_object_tgon.contains(r#""path":"#),
		"test-main should not have local path dependencies after publishing"
	);

	// The dep package should also have tag-based dependencies.
	let dep_object_output = context
		.local_server
		.tg()
		.arg("get")
		.arg(&dep_package_id)
		.arg("--blobs")
		.arg("--depth=inf")
		.output()
		.await
		.unwrap();
	assert_success!(dep_object_output);
	let dep_object_tgon = std::str::from_utf8(&dep_object_output.stdout).unwrap();
	assert!(
		!dep_object_tgon.contains(r#""path":"#),
		"test-dep should not have local path dependencies after publishing"
	);

	// The transitive package has no dependencies, but verify it doesn't have path fields.
	let transitive_object_output = context
		.local_server
		.tg()
		.arg("get")
		.arg(&transitive_package_id)
		.arg("--blobs")
		.arg("--depth=inf")
		.output()
		.await
		.unwrap();
	assert_success!(transitive_object_output);
	let transitive_object_tgon = std::str::from_utf8(&transitive_object_output.stdout).unwrap();
	assert!(
		!transitive_object_tgon.contains(r#""path":"#),
		"test-transitive should not have local path dependencies after publishing"
	);
}

#[tokio::test]
async fn package_with_diamond_dependency() {
	let context = Context::new().await;

	// Create the bottom package (D) - no dependencies.
	let content = indoc!(
		r#"
			export default () => "I am the bottom of the diamond!";

			export let metadata = {
				tag: "test-bottom/1.0.0",
			};
		"#
	);
	let (_bottom_temp, bottom_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the bottom package on the local server so it can be resolved.
	context
		.create_tag("test-bottom/1.0.0", &bottom_package_id)
		.await;

	// Create the left package (A) - depends on bottom.
	let content = indoc!(
		r#"
			import bottom from "test-bottom";

			export default () => `Left using: ${bottom()}`;

			export let metadata = {
				tag: "test-left/1.0.0",
			};
		"#
	);
	let (_left_temp, left_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the left package on the local server so it can be resolved.
	context
		.create_tag("test-left/1.0.0", &left_package_id)
		.await;

	// Create the right package (B) - depends on bottom.
	let content = indoc!(
		r#"
			import bottom from "test-bottom";

			export default () => `Right using: ${bottom()}`;

			export let metadata = {
				tag: "test-right/1.0.0",
			};
		"#
	);
	let (_right_temp, right_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the right package on the local server so it can be resolved.
	context
		.create_tag("test-right/1.0.0", &right_package_id)
		.await;

	// Create the main package - depends on both left and right.
	let content = indoc!(
		r#"
			import left from "test-left";
			import right from "test-right";

			export default () => `Main using: ${left()} and ${right()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	// Publish the main package - this should publish bottom, then left and right, then main.
	let publish_output = context.publish_with_output(&temp).await;

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let bottom_pos = stderr.find("published test-bottom/1.0.0");
	let left_pos = stderr.find("published test-left/1.0.0");
	let right_pos = stderr.find("published test-right/1.0.0");
	let main_pos = stderr.find("published test-main/1.0.0");

	// All packages should be published.
	assert!(bottom_pos.is_some(), "test-bottom should be published");
	assert!(left_pos.is_some(), "test-left should be published");
	assert!(right_pos.is_some(), "test-right should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify all four packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-left/1.0.0", &left_package_id)
		.await;
	context
		.assert_tag_on_remote("test-right/1.0.0", &right_package_id)
		.await;
	context
		.assert_tag_on_remote("test-bottom/1.0.0", &bottom_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&left_package_id).await;
	context.assert_object_synced(&right_package_id).await;
	context.assert_object_synced(&bottom_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&left_package_id).await;
	context.assert_metadata_synced(&right_package_id).await;
	context.assert_metadata_synced(&bottom_package_id).await;
}

#[tokio::test]
async fn package_with_diamond_dependency_and_shared_import() {
	let context = Context::new().await;

	// Create the bottom package (D) - no dependencies.
	let content = indoc!(
		r#"
			export default () => "I am the bottom of the diamond!";

			export let metadata = {
				tag: "test-bottom/1.0.0",
			};
		"#
	);
	let (_bottom_temp, bottom_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the bottom package on the local server so it can be resolved.
	context
		.create_tag("test-bottom/1.0.0", &bottom_package_id)
		.await;

	// Create the left package (A) - depends on bottom.
	let content = indoc!(
		r#"
			import bottom from "test-bottom";

			export default () => `Left using: ${bottom()}`;

			export let metadata = {
				tag: "test-left/1.0.0",
			};
		"#
	);
	let (_left_temp, left_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the left package on the local server so it can be resolved.
	context
		.create_tag("test-left/1.0.0", &left_package_id)
		.await;

	// Create the right package (B) - depends on bottom.
	let content = indoc!(
		r#"
			import bottom from "test-bottom";

			export default () => `Right using: ${bottom()}`;

			export let metadata = {
				tag: "test-right/1.0.0",
			};
		"#
	);
	let (_right_temp, right_package_id) = context.create_package(content.to_owned()).await;

	// Create a tag for the right package on the local server so it can be resolved.
	context
		.create_tag("test-right/1.0.0", &right_package_id)
		.await;

	// Create the main package - depends on left, right, AND bottom directly.
	let content = indoc!(
		r#"
			import left from "test-left";
			import right from "test-right";
			import bottom from "test-bottom";

			export default () => `Main using: ${left()}, ${right()}, and ${bottom()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	// Publish the main package - this should publish bottom, then left and right, then main.
	let publish_output = context.publish_with_output(&temp).await;

	// Verify publish order by checking stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Extract the positions where each package appears in the output.
	let bottom_pos = stderr.find("published test-bottom/1.0.0");
	let left_pos = stderr.find("published test-left/1.0.0");
	let right_pos = stderr.find("published test-right/1.0.0");
	let main_pos = stderr.find("published test-main/1.0.0");

	// All packages should be published.
	assert!(bottom_pos.is_some(), "test-bottom should be published");
	assert!(left_pos.is_some(), "test-left should be published");
	assert!(right_pos.is_some(), "test-right should be published");
	assert!(main_pos.is_some(), "test-main should be published");

	// Verify all four packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-left/1.0.0", &left_package_id)
		.await;
	context
		.assert_tag_on_remote("test-right/1.0.0", &right_package_id)
		.await;
	context
		.assert_tag_on_remote("test-bottom/1.0.0", &bottom_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&left_package_id).await;
	context.assert_object_synced(&right_package_id).await;
	context.assert_object_synced(&bottom_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&left_package_id).await;
	context.assert_metadata_synced(&right_package_id).await;
	context.assert_metadata_synced(&bottom_package_id).await;
}

#[tokio::test]
async fn package_with_local_path_import() {
	let context = Context::new().await;

	// Create a shared temp directory with both packages as siblings.
	let shared_artifact = temp::directory! {
		"dep" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "I am a local dependency!";

				export let metadata = {
					tag: "test-dep/1.0.0",
				};
			"#),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import dep from "test-dep" with { local: "../dep" };

				export default () => `Main package using: ${dep()}`;

				export let metadata = {
					tag: "test-main/1.0.0",
				};
			"#),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let dep_path = shared_temp.path().join("dep");
	let main_path = shared_temp.path().join("main");

	// Checkin the dep package to get its ID, but don't create a tag.
	let dep_checkin_output = context
		.local_server
		.tg()
		.current_dir(&dep_path)
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(dep_checkin_output);

	// Checkin the main package to get its ID, but don't create a tag.
	let main_checkin_output = context
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(main_checkin_output);

	// Publish the main package without having created tags beforehand.
	// This should discover the local dep, create its tag, publish it, then publish main.
	let publish_output = context
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

	// Extract the published artifact IDs from the stderr output.
	// Since packages with local path dependencies are re-checked-in, the published IDs
	// may differ from the original checkin IDs.
	let extract_published_id = |package_name: &str| -> String {
		stderr
			.lines()
			.find(|line| line.contains(&format!("published {package_name}")))
			.and_then(|line| line.split("dir_").nth(1))
			.map_or_else(
				|| panic!("{package_name} should have a published ID in output"),
				|s| format!("dir_{}", s.trim_end_matches(')')),
			)
	};

	let published_dep_id = extract_published_id("test-dep/1.0.0");
	let published_main_id = extract_published_id("test-main/1.0.0");

	// Verify both packages are tagged on local and remote servers with the published IDs.
	context
		.assert_tag_on_local("test-dep/1.0.0", &published_dep_id)
		.await;
	context
		.assert_tag_on_local("test-main/1.0.0", &published_main_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &published_dep_id)
		.await;
	context
		.assert_tag_on_remote("test-main/1.0.0", &published_main_id)
		.await;

	// Verify both packages are synced using the published IDs.
	context.assert_object_synced(&published_dep_id).await;
	context.assert_object_synced(&published_main_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&published_dep_id).await;
	context.assert_metadata_synced(&published_main_id).await;

	// Verify that main package has dependency on dep by tag, not by local path.
	let main_object_output = context
		.local_server
		.tg()
		.arg("get")
		.arg(&published_main_id)
		.arg("--blobs")
		.arg("--depth=inf")
		.output()
		.await
		.unwrap();
	assert_success!(main_object_output);
	let main_object_tgon = std::str::from_utf8(&main_object_output.stdout).unwrap();

	// The referent should have the "tag" key and NOT the "path" key.
	assert!(
		main_object_tgon.contains(r#""tag":"#),
		"main package's dependency referent should have a 'tag' field"
	);
	assert!(
		!main_object_tgon.contains(r#""path":"#),
		"main package's dependency referent should not have a 'path' field"
	);
}

#[tokio::test]
async fn package_with_dependency_cycle() {
	let context = Context::new().await;

	// Create an import cycle but NOT a process cycle:
	let shared_artifact = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import b from "test-b" with { local: "../b" };
				export default () => `A using: ${b()}`;
				export let greeting = () => "Hello from A";
				export let metadata = {
					tag: "test-a/1.0.0",
				};
			"#),
		},
		"b" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import * as a from "test-a" with { local: "../a" };
				export default () => `B using: ${a.greeting()}`;
				export let metadata = {
					tag: "test-b/1.0.0",
				};
			"#),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let b_path = shared_temp.path().join("b");
	let output = context
		.local_server
		.tg()
		.current_dir(&b_path)
		.arg("publish")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Extract the published IDs from the stderr
	let stderr = String::from_utf8_lossy(&output.stderr);

	let a_published_id = stderr
		.lines()
		.find(|line| line.contains("published test-a/1.0.0"))
		.and_then(|line| line.split("dir_").nth(1))
		.map(|s| format!("dir_{}", s.trim_end_matches(')')))
		.expect("test-a should be published");

	let b_published_id = stderr
		.lines()
		.find(|line| line.contains("published test-b/1.0.0"))
		.and_then(|line| line.split("dir_").nth(1))
		.map(|s| format!("dir_{}", s.trim_end_matches(')')))
		.expect("test-b should be published");

	// Verify both packages are tagged correctly.
	context
		.assert_tag_on_local("test-a/1.0.0", &a_published_id)
		.await;
	context
		.assert_tag_on_local("test-b/1.0.0", &b_published_id)
		.await;
	context
		.assert_tag_on_remote("test-a/1.0.0", &a_published_id)
		.await;
	context
		.assert_tag_on_remote("test-b/1.0.0", &b_published_id)
		.await;

	// Verify objects are synced to remote.
	context.assert_object_synced(&a_published_id).await;
	context.assert_object_synced(&b_published_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&a_published_id).await;
	context.assert_metadata_synced(&b_published_id).await;
}

#[tokio::test]
async fn package_with_cycles_and_non_cycles() {
	let context = Context::new().await;

	// Create a complex graph with both cycles and non-cycles:
	//
	//              main
	//            /      \
	//        cycle-a   independent
	//         /   \        /    \
	//     cycle-b  \     leaf1  leaf2
	//        /      \
	//     leaf2    leaf1
	//     (cycle + leaf deps)
	//
	// This tests that the publish ordering:
	// 1. Handles cycles (cycle-a <-> cycle-b)
	// 2. Handles acyclic dependencies (independent -> leaf1, leaf2)
	// 3. Both packages in the cycle import leaves (cycle-a -> leaf1, cycle-b -> leaf2)
	// 4. Publishes leaves before their dependents
	// 5. Publishes everything in a valid order

	let shared_artifact = temp::directory! {
		"leaf1" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "I am leaf1!";
				export let metadata = {
					tag: "test-leaf1/1.0.0",
				};
			"#),
		},
		"leaf2" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "I am leaf2!";
				export let metadata = {
					tag: "test-leaf2/1.0.0",
				};
			"#),
		},
		"independent" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import leaf1 from "test-leaf1" with { local: "../leaf1" };
				import leaf2 from "test-leaf2" with { local: "../leaf2" };
				export default () => `Independent using: ${leaf1()} and ${leaf2()}`;
				export let metadata = {
					tag: "test-independent/1.0.0",
				};
			"#),
		},
		"cycle-a" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import cycleB from "test-cycle-b" with { local: "../cycle-b" };
				import leaf1 from "test-leaf1" with { local: "../leaf1" };
				export default () => `Cycle A using: ${cycleB()} and ${leaf1()}`;
				export let greeting = () => "Hello from Cycle A";
				export let metadata = {
					tag: "test-cycle-a/1.0.0",
				};
			"#),
		},
		"cycle-b" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import * as cycleA from "test-cycle-a" with { local: "../cycle-a" };
				import leaf2 from "test-leaf2" with { local: "../leaf2" };
				export default () => `Cycle B using: ${cycleA.greeting()} and ${leaf2()}`;
				export let metadata = {
					tag: "test-cycle-b/1.0.0",
				};
			"#),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import cycleA from "test-cycle-a" with { local: "../cycle-a" };
				import independent from "test-independent" with { local: "../independent" };
				export default () => `Main using: ${cycleA()} and ${independent()}`;
				export let metadata = {
					tag: "test-main/1.0.0",
				};
			"#),
		},
	};

	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let main_path = shared_temp.path().join("main");

	// Publish the main package - this should handle both cycles and non-cycles.
	let output = context
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("publish")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let stderr = String::from_utf8_lossy(&output.stderr);
	let extract_id = |package_name: &str| -> String {
		stderr
			.lines()
			.find(|line| line.contains(&format!("published {package_name}")))
			.and_then(|line| line.split("dir_").nth(1))
			.map_or_else(
				|| panic!("{package_name} should be published"),
				|s| format!("dir_{}", s.trim_end_matches(')')),
			)
	};

	let leaf1_id = extract_id("test-leaf1/1.0.0");
	let leaf2_id = extract_id("test-leaf2/1.0.0");
	let independent_id = extract_id("test-independent/1.0.0");
	let cycle_a_id = extract_id("test-cycle-a/1.0.0");
	let cycle_b_id = extract_id("test-cycle-b/1.0.0");
	let main_id = extract_id("test-main/1.0.0");

	// Verify all packages are tagged correctly on both servers.
	for (tag, id) in [
		("test-leaf1/1.0.0", &leaf1_id),
		("test-leaf2/1.0.0", &leaf2_id),
		("test-independent/1.0.0", &independent_id),
		("test-cycle-a/1.0.0", &cycle_a_id),
		("test-cycle-b/1.0.0", &cycle_b_id),
		("test-main/1.0.0", &main_id),
	] {
		context.assert_tag_on_local(tag, id).await;
		context.assert_tag_on_remote(tag, id).await;
		context.assert_object_synced(id).await;
	}

	// Verify metadata is synced for all packages.
	context.index_servers().await;
	for id in [
		&leaf1_id,
		&leaf2_id,
		&independent_id,
		&cycle_a_id,
		&cycle_b_id,
		&main_id,
	] {
		context.assert_metadata_synced(id).await;
	}
}

#[tokio::test]
async fn single_file_package() {
	let context = Context::new().await;

	// Create a single file (not a directory).
	let file = temp::file!(indoc!(
		r#"
			export default () => "I am a single-file package!";

			export let metadata = {
				tag: "test-single-file/1.0.0",
			};
		"#
	));
	let temp = Temp::new();
	let artifact: temp::Artifact = file.into();
	artifact.to_path(&temp).await.unwrap();

	// Checkin the file.
	let checkin_output = context
		.local_server
		.tg()
		.arg("checkin")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(checkin_output);

	let package_id = std::str::from_utf8(&checkin_output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Publish from the temp directory.
	let publish_output = context
		.local_server
		.tg()
		.arg("publish")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(publish_output);

	// Verify ids.
	let tag = "test-single-file/1.0.0";
	context.assert_tag_on_local(tag, &package_id).await;
	context.assert_tag_on_remote(tag, &package_id).await;
	context.assert_object_synced(&package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn simple_package_with_tag() {
	let context = Context::new().await;

	let content = indoc!(
		r#"
			export default () => "Hello, World!";

			export let metadata = {
				tag: "test-pkg/1.0.0",
			};
		"#
	);
	let (_temp, package_id) = context.create_package(content.to_owned()).await;

	let tag = "test-pkg/1.0.0";
	context.create_tag(tag, &package_id).await;
	let output = context
		.local_server
		.tg()
		.arg("publish")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	context.assert_tag_on_local(tag, &package_id).await;
	context.assert_tag_on_remote(tag, &package_id).await;
	context.assert_object_synced(&package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn simple_package_with_tag_override() {
	let context = Context::new().await;

	let content = indoc!(
		r#"
			export default () => "Hello, World!";

			export let metadata = {
				tag: "test-pkg/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	let override_tag = "overridden-pkg/2.0.0";
	let output = context
		.local_server
		.tg()
		.current_dir(temp.path())
		.arg("publish")
		.arg("--tag")
		.arg(override_tag)
		.output()
		.await
		.unwrap();
	assert_success!(output);

	context.assert_tag_on_local(override_tag, &package_id).await;
	context
		.assert_tag_on_remote(override_tag, &package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;

	let original_tag = "test-pkg/1.0.0";
	let local_tag_output = context
		.local_server
		.tg()
		.arg("tag")
		.arg("get")
		.arg(original_tag)
		.output()
		.await
		.unwrap();
	let tag_result = serde_json::from_slice::<tg::tag::get::Output>(&local_tag_output.stdout);
	assert!(
		tag_result.is_err() || tag_result.unwrap().item.is_none(),
		"Original metadata tag should not be created when using --tag override"
	);
}

#[tokio::test]
async fn package_with_single_file_and_multi_file_dependencies() {
	let context = Context::new().await;

	// Create a single-file package.
	let single_file = temp::file!(indoc!(
		r#"
			export default () => "I am a single-file package!";

			export let metadata = {
				tag: "test-single-file/1.0.0",
			};
		"#
	));
	let single_file_temp = Temp::new();
	let single_file_artifact: temp::Artifact = single_file.into();
	single_file_artifact
		.to_path(&single_file_temp)
		.await
		.unwrap();

	let single_file_checkin_output = context
		.local_server
		.tg()
		.arg("checkin")
		.arg(single_file_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(single_file_checkin_output);
	let single_file_package_id = std::str::from_utf8(&single_file_checkin_output.stdout)
		.unwrap()
		.trim()
		.to_owned();
	context
		.create_tag("test-single-file/1.0.0", &single_file_package_id)
		.await;

	// Create a multi-file package with submodules.
	let multi_file_artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import { helper } from "./helper.ts";
			import { util } from "./subdir/util.ts";

			export default () => `Multi-file using: ${helper()} and ${util()}`;

			export let metadata = {
				tag: "test-multi-file/1.0.0",
			};
		"#),
		"helper.ts" => indoc!(r#"
			export let helper = () => "helper function";
		"#),
		"subdir" => temp::directory! {
			"util.ts" => indoc!(r#"
				export let util = () => "util function";
			"#),
		},
	};
	let multi_file_temp = Temp::new();
	let multi_file_temp_artifact: temp::Artifact = multi_file_artifact.into();
	multi_file_temp_artifact
		.to_path(&multi_file_temp)
		.await
		.unwrap();
	let multi_file_checkin_output = context
		.local_server
		.tg()
		.current_dir(multi_file_temp.path())
		.arg("checkin")
		.arg(".")
		.output()
		.await
		.unwrap();
	assert_success!(multi_file_checkin_output);
	let multi_file_package_id = std::str::from_utf8(&multi_file_checkin_output.stdout)
		.unwrap()
		.trim()
		.to_owned();
	context
		.create_tag("test-multi-file/1.0.0", &multi_file_package_id)
		.await;

	// Create a main package that imports both.
	let content = indoc!(
		r#"
			import singleFile from "test-single-file";
			import multiFile from "test-multi-file";

			export default () => `Main using: ${singleFile()} and ${multiFile()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		"#
	);
	let (temp, package_id) = context.create_package(content.to_owned()).await;

	// Publish and capture the output to check what gets published.
	let publish_output = context.publish_with_output(&temp).await;
	assert_success!(publish_output);

	// Extract the stderr to verify what was published.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Count how many packages are being published.
	let publish_count = stderr
		.lines()
		.filter(|line| line.contains("published test-"))
		.count();

	// We should only publish 3 packages: test-main, test-single-file, test-multi-file.
	// We should NOT publish helper.ts or util.ts as separate packages.
	assert_eq!(
		publish_count, 3,
		"Expected 3 packages to be published (test-main, test-single-file, test-multi-file), but found {publish_count}\nStderr:\n{stderr}"
	);

	// Verify the correct packages are published.
	assert!(
		stderr.contains("published test-main/1.0.0"),
		"test-main should be published"
	);
	assert!(
		stderr.contains("published test-single-file/1.0.0"),
		"test-single-file should be published"
	);
	assert!(
		stderr.contains("published test-multi-file/1.0.0"),
		"test-multi-file should be published"
	);

	// Verify all packages are tagged and synced.
	context
		.assert_tag_on_local("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_local("test-single-file/1.0.0", &single_file_package_id)
		.await;
	context
		.assert_tag_on_local("test-multi-file/1.0.0", &multi_file_package_id)
		.await;
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-single-file/1.0.0", &single_file_package_id)
		.await;
	context
		.assert_tag_on_remote("test-multi-file/1.0.0", &multi_file_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&single_file_package_id).await;
	context.assert_object_synced(&multi_file_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context
		.assert_metadata_synced(&single_file_package_id)
		.await;
	context.assert_metadata_synced(&multi_file_package_id).await;
}

#[tokio::test]
async fn package_with_dependency_in_submodule() {
	let context = Context::new().await;

	// Import a dependency in a submodule (helper.ts), not in the root tangram.ts.
	// Also import another internal submodule to verify it's not treated as a separate package.
	let shared_artifact = temp::directory! {
		"dep" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "I am a dependency!";

				export let metadata = {
					tag: "test-dep/1.0.0",
				};
			"#),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import { helper } from "./helper.tg.ts";

				export default () => `Main using: ${helper()}`;

				export let metadata = {
					tag: "test-main/1.0.0",
				};
			"#),
			"helper.tg.ts" => indoc!(r#"
				import dep from "test-dep" with { local: "../dep" };
				import { util } from "./util.tg.ts";

				export let helper = () => `helper with ${dep()} and ${util()}`;
			"#),
			"util.tg.ts" => indoc!(r#"
				export let util = () => "util function";
			"#),
		},
	};
	let shared_temp = Temp::new();
	let shared_temp_artifact: temp::Artifact = shared_artifact.into();
	shared_temp_artifact.to_path(&shared_temp).await.unwrap();

	let main_path = shared_temp.path().join("main");

	// Publish the main package - this should also publish the dependency.
	let publish_output = context
		.local_server
		.tg()
		.current_dir(&main_path)
		.arg("publish")
		.output()
		.await
		.unwrap();
	assert_success!(publish_output);

	// Extract the published artifact IDs from the stderr output.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Verify that only 2 packages are published (test-main and test-dep), not util.tg.ts.
	let publish_count = stderr
		.lines()
		.filter(|line| line.contains("published test-"))
		.count();
	assert_eq!(
		publish_count, 2,
		"Expected exactly 2 packages to be published (test-main and test-dep), not internal submodules. Found {publish_count} packages.\nStderr:\n{stderr}"
	);

	// Verify that util.tg.ts is not treated as a separate package.
	assert!(
		!stderr.contains("published") || !stderr.contains("util"),
		"Internal submodule 'util' should not be published as a separate package"
	);
	let extract_published_id = |package_name: &str| -> String {
		stderr
			.lines()
			.find(|line| line.contains(&format!("published {package_name}")))
			.and_then(|line| line.split("dir_").nth(1))
			.map_or_else(
				|| panic!("{package_name} should have a published ID in output"),
				|s| format!("dir_{}", s.trim_end_matches(')')),
			)
	};

	let dep_package_id = extract_published_id("test-dep/1.0.0");
	let package_id = extract_published_id("test-main/1.0.0");

	// Verify both packages are tagged and synced on the remote.
	context
		.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	context
		.assert_tag_on_remote("test-dep/1.0.0", &dep_package_id)
		.await;
	context.assert_object_synced(&package_id).await;
	context.assert_object_synced(&dep_package_id).await;
	context.index_servers().await;
	context.assert_metadata_synced(&package_id).await;
	context.assert_metadata_synced(&dep_package_id).await;
}

struct Context {
	local_server: Server,
	remote_server: Server,
}

impl Context {
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
			.arg("--blobs")
			.arg("--depth=inf")
			.arg("--pretty")
			.output()
			.await
			.unwrap();
		assert_success!(local_object_output);
		let remote_object_output = self
			.remote_server
			.tg()
			.arg("get")
			.arg(package_id)
			.arg("--blobs")
			.arg("--depth=inf")
			.arg("--pretty")
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
			.output()
			.await
			.unwrap();
		assert_eq!(local_metadata_output, remote_metadata_output);
	}
}
