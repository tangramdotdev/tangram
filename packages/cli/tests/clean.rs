use indoc::indoc;
use std::path::Path;
use tangram_cli::test::{test, Server};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn builds() -> tg::Result<()> {
	test(TG, move |context| async move {
		let build = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export let e = tg.target(() => "e");
				export let d = tg.target(() => "d");
				export let c = tg.target(() => "c");
				export let b = tg.target(async () => {
					await e();
					await d();
					return "b";
				});
				export let a = tg.target(async () => {
					await b();
					await c();
					return "a";
				});
			"#),
		};
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = build.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		let a = build_target_get_build_id("a", &server, artifact_temp.path()).await;
		let b = build_target_get_build_id("b", &server, artifact_temp.path()).await;
		let c = build_target_get_build_id("c", &server, artifact_temp.path()).await;
		let d = build_target_get_build_id("d", &server, artifact_temp.path()).await;
		let e = build_target_get_build_id("e", &server, artifact_temp.path()).await;

		// Tag the builds.
		for (pattern, id) in [("b", &b), ("d", &d)] {
			let output = server
				.tg()
				.arg("tag")
				.arg(pattern)
				.arg(id)
				.spawn()
				.unwrap()
				.wait_with_output()
				.await
				.unwrap();
			assert!(output.status.success());
		}

		// Clean.
		let output = server
			.tg()
			.arg("clean")
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Confirm presence/absence of builds.
		let a_output = server
			.tg()
			.arg("build")
			.arg("get")
			.arg(a)
			.output()
			.await
			.unwrap();
		assert!(!a_output.status.success());

		let output = server
			.tg()
			.arg("build")
			.arg("get")
			.arg(b)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let output = server
			.tg()
			.arg("build")
			.arg("get")
			.arg(c)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let output = server
			.tg()
			.arg("build")
			.arg("get")
			.arg(d)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		let output = server
			.tg()
			.arg("build")
			.arg("get")
			.arg(e)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());
	})
	.await;
	Ok(())
}

#[tokio::test]
async fn objects() -> tg::Result<()> {
	test(TG, move |context| async move {
		let build = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export let h = tg.target(() => tg.file("h"));
				export let g = tg.target(() => tg.file("g"));
				export let f = tg.target(() => tg.file("f"));
				export let e = tg.target(() => tg.file("e"));
				export let d = tg.target(() => tg.directory({
					"h": h(),
				}));
				export let c = tg.target(() => tg.directory({
					"g": g(),
				}));
				export let b = tg.target(() => tg.directory({
					"f": f(),
				}));
				export let a = tg.target(() => tg.directory({
					"b": b(),
					"c": c(),
					"d": d(),
					"e": e(),
				}));
			"#),
		};

		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = build.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		let c = build_target_get_object_id("c", &server, artifact_temp.path()).await;
		let h = build_target_get_object_id("h", &server, artifact_temp.path()).await;

		// sleep 10 secs
		tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

		// Tag c.
		let pattern = "c";
		let output = server
			.tg()
			.arg("tag")
			.arg(pattern)
			.arg(c.clone())
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Tag h.
		let pattern = "h";
		let output = server
			.tg()
			.arg("tag")
			.arg(pattern)
			.arg(h)
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Clean.
		let output = server
			.tg()
			.arg("clean")
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(output.status.success());

		// Confirm presence/absence of objects.
		let a_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("a.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(a_output.status.success());

		let b_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("b.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(b_output.status.success());

		let c_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("c.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(c_output.status.success());

		let d_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("d.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(d_output.status.success());

		let e_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("e.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(e_output.status.success());

		let f_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("f.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(f_output.status.success());

		let g_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("g.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(g_output.status.success());

		let h_output = server
			.tg()
			.arg("build")
			.arg("--quiet")
			.arg(artifact_temp.path().join("h.tg.ts"))
			.spawn()
			.unwrap()
			.wait_with_output()
			.await
			.unwrap();
		assert!(h_output.status.success());
	})
	.await;
	Ok(())
}

async fn build_target_get_build_id(name: &str, server: &Server, path: &Path) -> String {
	let output = server
		.tg()
		.arg("build")
		.arg("--quiet")
		.arg(path.join(format!("{name}.tg.ts")))
		.spawn()
		.unwrap()
		.wait_with_output()
		.await
		.unwrap();
	assert!(output.status.success());
	let build_id = std::str::from_utf8(&output.stdout).unwrap().trim();
	let output = server
		.tg()
		.arg("build")
		.arg("output")
		.arg(build_id)
		.spawn()
		.unwrap()
		.wait_with_output()
		.await
		.unwrap();
	assert!(output.status.success());
	build_id.to_owned()
}

async fn build_target_get_object_id(name: &str, server: &Server, path: &Path) -> String {
	let output = server
		.tg()
		.arg("build")
		.arg("--quiet")
		.arg(path.join(format!("{name}.tg.ts")))
		.spawn()
		.unwrap()
		.wait_with_output()
		.await
		.unwrap();
	assert!(output.status.success());
	let output = std::str::from_utf8(&output.stdout).unwrap().trim();
	output.to_owned()
}
