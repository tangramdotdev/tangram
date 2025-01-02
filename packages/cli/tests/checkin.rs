use indoc::indoc;
use tangram_cli::{assert_output_success, config::Config, test::test};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};
use tokio::io::AsyncWriteExt as _;

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test checking in a file with a lockfile in its xattrs.
#[tokio::test]
async fn checkin_locked_file() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let config = Config {
			remotes: Some(None),
			..Default::default()
		};
		let server = context.spawn_server_with_config(config).await.unwrap();

		// Create a file with dependencies.
		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					const d = await tg.directory({
						entry: tg.file("inner")
					});
					const d_id = await d.id();
					const f = await tg.file("contents", { dependencies: { [d_id]: { item: d }}});
					return await f.id();
				});
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		let output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
		let stdout = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.trim_matches('"');
		let orig_id: tg::file::Id = stdout.parse().unwrap();

		// Store the output of `tg get`.
		let orig_get_output = server
			.tg()
			.arg("get")
			.arg(orig_id.to_string())
			.output()
			.await
			.unwrap();
		let orig_get_stdout = std::str::from_utf8(&orig_get_output.stdout).unwrap().trim();

		// Check out the file with dependencies=false, lockfile=true.
		let checkout_temp = Temp::new();
		tokio::fs::create_dir_all(checkout_temp.path())
			.await
			.unwrap();
		let checkout_output = server
			.tg()
			.arg("checkout")
			.arg("--dependencies=false")
			.arg("--lockfile=true")
			.arg(orig_id.to_string())
			.arg(checkout_temp.path().join("orig"))
			.output()
			.await
			.unwrap();
		assert_output_success!(checkout_output);

		// Check in the file
		let checkin_output = server
			.tg()
			.arg("checkin")
			.arg(checkout_temp.path().join("orig"))
			.output()
			.await
			.unwrap();
		assert_output_success!(checkin_output);
		let checkin_stdout = std::str::from_utf8(&checkin_output.stdout)
			.unwrap()
			.trim()
			.trim_matches('"');
		let checkin_id: tg::file::Id = checkin_stdout.parse().unwrap();

		// The checked in file should match the original file.
		assert_eq!(orig_id, checkin_id);

		// The `tg get` output of each should be identical.
		let checkin_get_output = server
			.tg()
			.arg("get")
			.arg(checkin_id.to_string())
			.output()
			.await
			.unwrap();
		let checkin_get_stdout = std::str::from_utf8(&checkin_get_output.stdout)
			.unwrap()
			.trim();
		assert_eq!(orig_get_stdout, checkin_get_stdout);
		// assert_eq!("a", checkin_get_stdout);
	})
	.await;
}

/// Test checking in a file with a lockfile in its xattrs that has transitive dependencies.
#[tokio::test]
async fn checkin_locked_file_with_transitive_dependency() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let config = Config {
			remotes: Some(None),
			..Default::default()
		};
		let server = context.spawn_server_with_config(config).await.unwrap();

		// Create a file with dependencies.
		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(async () => {
					const inner_dep = await tg.file("transive dep");
					const inner_dep_id = await inner_dep.id();
					const d = await tg.directory({
						entry: tg.file("inner", { dependencies: { [inner_dep_id]: { item: inner_dep }}}),
					});
					const d_id = await d.id();
					const f = await tg.file("contents", { dependencies: { [d_id]: { item: d }}});
					return await f.id();
				});
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		let output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
		let stdout = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.trim_matches('"');
		let orig_id: tg::file::Id = stdout.parse().unwrap();

		// Store the output of `tg get`.
		let orig_get_output = server
			.tg()
			.arg("get")
			.arg(orig_id.to_string())
			.output()
			.await
			.unwrap();
		let orig_get_stdout = std::str::from_utf8(&orig_get_output.stdout).unwrap().trim();

		// Check out the file with dependencies=false, lockfile=true.
		let checkout_temp = Temp::new();
		tokio::fs::create_dir_all(checkout_temp.path())
			.await
			.unwrap();
		let checkout_output = server
			.tg()
			.arg("checkout")
			.arg("--dependencies=false")
			.arg("--lockfile=true")
			.arg(orig_id.to_string())
			.arg(checkout_temp.path().join("orig"))
			.output()
			.await
			.unwrap();
		assert_output_success!(checkout_output);

		// Check in the file
		let checkin_output = server
			.tg()
			.arg("checkin")
			.arg(checkout_temp.path().join("orig"))
			.output()
			.await
			.unwrap();
		assert_output_success!(checkin_output);
		let checkin_stdout = std::str::from_utf8(&checkin_output.stdout)
			.unwrap()
			.trim()
			.trim_matches('"');
		let checkin_id: tg::file::Id = checkin_stdout.parse().unwrap();

		// The checked in file should match the original file.
		assert_eq!(orig_id, checkin_id);

		// The `tg get` output of each should be identical.
		let checkin_get_output = server
			.tg()
			.arg("get")
			.arg(checkin_id.to_string())
			.output()
			.await
			.unwrap();
		let checkin_get_stdout = std::str::from_utf8(&checkin_get_output.stdout)
			.unwrap()
			.trim();
		assert_eq!(orig_get_stdout, checkin_get_stdout);
		// assert_eq!("a", checkin_get_stdout);
	})
	.await;
}
