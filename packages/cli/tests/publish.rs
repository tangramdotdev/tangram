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
	let output = ctx
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
		.find(|line| line.contains("publishing test-a/1.0.0"))
		.and_then(|line| line.split("dir_").nth(1))
		.map(|s| format!("dir_{}", s.trim_end_matches(')')))
		.expect("test-a should be published");

	let b_published_id = stderr
		.lines()
		.find(|line| line.contains("publishing test-b/1.0.0"))
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

#[tokio::test]
async fn package_with_cycles_and_non_cycles() {
	let ctx = TestContext::new().await;

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
			"tangram.ts" => indoc!(
				r#"
				export default () => "I am leaf1!";
				export let metadata = {
					name: "test-leaf1",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"leaf2" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				export default () => "I am leaf2!";
				export let metadata = {
					name: "test-leaf2",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"independent" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import leaf1 from "test-leaf1" with { local: "../leaf1" };
				import leaf2 from "test-leaf2" with { local: "../leaf2" };
				export default () => `Independent using: ${leaf1()} and ${leaf2()}`;
				export let metadata = {
					name: "test-independent",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"cycle-a" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import cycleB from "test-cycle-b" with { local: "../cycle-b" };
				import leaf1 from "test-leaf1" with { local: "../leaf1" };
				export default () => `Cycle A using: ${cycleB()} and ${leaf1()}`;
				export let greeting = () => "Hello from Cycle A";
				export let metadata = {
					name: "test-cycle-a",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"cycle-b" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import * as cycleA from "test-cycle-a" with { local: "../cycle-a" };
				import leaf2 from "test-leaf2" with { local: "../leaf2" };
				export default () => `Cycle B using: ${cycleA.greeting()} and ${leaf2()}`;
				export let metadata = {
					name: "test-cycle-b",
					version: "1.0.0",
				};
			"#
			).to_owned(),
		},
		"main" => temp::directory! {
			"tangram.ts" => indoc!(
				r#"
				import cycleA from "test-cycle-a" with { local: "../cycle-a" };
				import independent from "test-independent" with { local: "../independent" };
				export default () => `Main using: ${cycleA()} and ${independent()}`;
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

	let main_path = shared_temp.path().join("main");

	// Publish the main package - this should handle both cycles and non-cycles.
	let output = ctx
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
			.find(|line| line.contains(&format!("publishing {package_name}")))
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

	let get_pos = |name: &str| stderr.find(&format!("publishing {name}"));

	let leaf1_pos = get_pos("test-leaf1/1.0.0").expect("leaf1 should be published");
	let leaf2_pos = get_pos("test-leaf2/1.0.0").expect("leaf2 should be published");
	let independent_pos =
		get_pos("test-independent/1.0.0").expect("independent should be published");
	let cycle_a_pos = get_pos("test-cycle-a/1.0.0").expect("cycle-a should be published");
	let cycle_b_pos = get_pos("test-cycle-b/1.0.0").expect("cycle-b should be published");
	let main_pos = get_pos("test-main/1.0.0").expect("main should be published");

	// Non-cyclic ordering constraints:
	// leaf1 and leaf2 must come before independent
	assert!(
		leaf1_pos < independent_pos,
		"test-leaf1 should be published before test-independent"
	);
	assert!(
		leaf2_pos < independent_pos,
		"test-leaf2 should be published before test-independent"
	);

	// leaf1 must come before cycle-a (since cycle-a depends on it)
	assert!(
		leaf1_pos < cycle_a_pos,
		"test-leaf1 should be published before test-cycle-a"
	);

	// leaf2 must come before cycle-b (since cycle-b depends on it)
	assert!(
		leaf2_pos < cycle_b_pos,
		"test-leaf2 should be published before test-cycle-b"
	);

	// Both independent and the cycle packages must come before main
	assert!(
		independent_pos < main_pos,
		"test-independent should be published before test-main"
	);
	assert!(
		cycle_a_pos < main_pos,
		"test-cycle-a should be published before test-main"
	);
	assert!(
		cycle_b_pos < main_pos,
		"test-cycle-b should be published before test-main"
	);

	// Verify all packages are tagged correctly on both servers.
	for (tag, id) in [
		("test-leaf1/1.0.0", &leaf1_id),
		("test-leaf2/1.0.0", &leaf2_id),
		("test-independent/1.0.0", &independent_id),
		("test-cycle-a/1.0.0", &cycle_a_id),
		("test-cycle-b/1.0.0", &cycle_b_id),
		("test-main/1.0.0", &main_id),
	] {
		ctx.assert_tag_on_local(tag, id).await;
		ctx.assert_tag_on_remote(tag, id).await;
		ctx.assert_object_synced(id).await;
	}

	// Verify metadata is synced for all packages.
	ctx.index_servers().await;
	for id in [
		&leaf1_id,
		&leaf2_id,
		&independent_id,
		&cycle_a_id,
		&cycle_b_id,
		&main_id,
	] {
		ctx.assert_metadata_synced(id).await;
	}
}

#[tokio::test]
async fn single_file_package() {
	let ctx = TestContext::new().await;

	// Create a single file (not a directory).
	let file = temp::file!(indoc!(
		r#"
		export default () => "I am a single-file package!";

		export let metadata = {
			name: "test-single-file",
			version: "1.0.0",
		};
	"#
	));
	let temp = Temp::new();
	let artifact: temp::Artifact = file.into();
	artifact.to_path(&temp).await.unwrap();

	// Checkin the file.
	let checkin_output = ctx
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
	dbg!(&package_id);

	// Publish from the temp directory.
	let publish_output = ctx
		.local_server
		.tg()
		.arg("publish")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(publish_output);

	let tag = "test-single-file/1.0.0";
	ctx.assert_tag_on_local(tag, &package_id).await;
	ctx.assert_tag_on_remote(tag, &package_id).await;
	ctx.assert_object_synced(&package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn simple_package_with_tag() {
	let ctx = TestContext::new().await;

	let (_temp, package_id) = ctx
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

	let tag = "test-pkg/1.0.0";
	ctx.create_tag(tag, &package_id).await;
	let output = ctx
		.local_server
		.tg()
		.arg("publish")
		.arg(tag)
		.output()
		.await
		.unwrap();
	assert_success!(output);

	ctx.assert_tag_on_local(tag, &package_id).await;
	ctx.assert_tag_on_remote(tag, &package_id).await;
	ctx.assert_object_synced(&package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
}

#[tokio::test]
async fn package_with_single_file_and_multi_file_dependencies() {
	let ctx = TestContext::new().await;

	// Create a single-file package.
	let single_file = temp::file!(indoc!(
		r#"
		export default () => "I am a single-file package!";

		export let metadata = {
			name: "test-single-file",
			version: "1.0.0",
		};
	"#
	));
	let single_file_temp = Temp::new();
	let single_file_artifact: temp::Artifact = single_file.into();
	single_file_artifact
		.to_path(&single_file_temp)
		.await
		.unwrap();

	let single_file_checkin_output = ctx
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
	ctx.create_tag("test-single-file/1.0.0", &single_file_package_id)
		.await;

	// Create a multi-file package with submodules.
	let multi_file_artifact = temp::directory! {
		"tangram.ts" => indoc!(
			r#"
			import { helper } from "./helper.ts";
			import { util } from "./subdir/util.ts";

			export default () => `Multi-file using: ${helper()} and ${util()}`;

			export let metadata = {
				name: "test-multi-file",
				version: "1.0.0",
			};
		"#
		).to_owned(),
		"helper.ts" => indoc!(
			r#"
			export let helper = () => "helper function";
		"#
		).to_owned(),
		"subdir" => temp::directory! {
			"util.ts" => indoc!(
				r#"
				export let util = () => "util function";
			"#
			).to_owned(),
		},
	};
	let multi_file_temp = Temp::new();
	let multi_file_temp_artifact: temp::Artifact = multi_file_artifact.into();
	multi_file_temp_artifact
		.to_path(&multi_file_temp)
		.await
		.unwrap();
	let multi_file_checkin_output = ctx
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
	ctx.create_tag("test-multi-file/1.0.0", &multi_file_package_id)
		.await;

	// Create a main package that imports both.
	let (temp, package_id) = ctx
		.create_package(
			indoc!(
				r#"
			import singleFile from "test-single-file";
			import multiFile from "test-multi-file";

			export default () => `Main using: ${singleFile()} and ${multiFile()}`;

			export let metadata = {
				name: "test-main",
				version: "1.0.0",
			};
		"#
			)
			.to_owned(),
		)
		.await;

	// Publish and capture the output to check what gets published.
	let publish_output = ctx.publish_with_output(&temp).await;
	assert_success!(publish_output);

	// Extract the stderr to verify what was published.
	let stderr = String::from_utf8_lossy(&publish_output.stderr);

	// Count how many packages are being published.
	let publish_count = stderr
		.lines()
		.filter(|line| line.contains("publishing test-"))
		.count();

	// We should only publish 3 packages: test-main, test-single-file, test-multi-file.
	// We should NOT publish helper.ts or util.ts as separate packages.
	assert_eq!(
		publish_count, 3,
		"Expected 3 packages to be published (test-main, test-single-file, test-multi-file), but found {publish_count}\nStderr:\n{stderr}"
	);

	// Verify the correct packages are published.
	assert!(
		stderr.contains("publishing test-main/1.0.0"),
		"test-main should be published"
	);
	assert!(
		stderr.contains("publishing test-single-file/1.0.0"),
		"test-single-file should be published"
	);
	assert!(
		stderr.contains("publishing test-multi-file/1.0.0"),
		"test-multi-file should be published"
	);

	// Verify all packages are tagged and synced.
	ctx.assert_tag_on_local("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_local("test-single-file/1.0.0", &single_file_package_id)
		.await;
	ctx.assert_tag_on_local("test-multi-file/1.0.0", &multi_file_package_id)
		.await;
	ctx.assert_tag_on_remote("test-main/1.0.0", &package_id)
		.await;
	ctx.assert_tag_on_remote("test-single-file/1.0.0", &single_file_package_id)
		.await;
	ctx.assert_tag_on_remote("test-multi-file/1.0.0", &multi_file_package_id)
		.await;
	ctx.assert_object_synced(&package_id).await;
	ctx.assert_object_synced(&single_file_package_id).await;
	ctx.assert_object_synced(&multi_file_package_id).await;
	ctx.index_servers().await;
	ctx.assert_metadata_synced(&package_id).await;
	ctx.assert_metadata_synced(&single_file_package_id).await;
	ctx.assert_metadata_synced(&multi_file_package_id).await;
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
