use indoc::indoc;
use std::path::Path;
use tangram_cli::{
	assert_failure, assert_success,
	test::{test, Server},
};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn processes() {
	test(TG, move |context| async move {
		let directory = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export let e = tg.command(() => "e");
				export let d = tg.command(() => "d");
				export let c = tg.command(() => "c");
				export let b = tg.command(async () => {
					await e();
					await d();
					return "b";
				});
				export let a = tg.command(async () => {
					await b();
					await c();
					return "a";
				});
			"#),
		};
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = directory.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		let a = build_command_get_process_id("a", &server, artifact_temp.path()).await;
		let b = build_command_get_process_id("b", &server, artifact_temp.path()).await;
		let c = build_command_get_process_id("c", &server, artifact_temp.path()).await;
		let d = build_command_get_process_id("d", &server, artifact_temp.path()).await;
		let e = build_command_get_process_id("e", &server, artifact_temp.path()).await;

		// Tag the processes.
		for (pattern, id) in [("b", &b), ("d", &d)] {
			let output = server
				.tg()
				.arg("tag")
				.arg(pattern)
				.arg(id)
				.output()
				.await
				.unwrap();
			assert_success!(output);
		}

		// Clean.
		let output = server.tg().arg("clean").output().await.unwrap();
		assert_success!(output);

		// Confirm presence/absence of processes.
		let a_output = server
			.tg()
			.arg("process")
			.arg("get")
			.arg(a)
			.output()
			.await
			.unwrap();
		assert_failure!(a_output);

		let output = server
			.tg()
			.arg("process")
			.arg("get")
			.arg(b)
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let output = server
			.tg()
			.arg("process")
			.arg("get")
			.arg(c)
			.output()
			.await
			.unwrap();
		assert_failure!(output);

		let output = server
			.tg()
			.arg("process")
			.arg("get")
			.arg(d)
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let output = server
			.tg()
			.arg("process")
			.arg("get")
			.arg(e)
			.output()
			.await
			.unwrap();
		assert_success!(output);
	})
	.await;
}

#[tokio::test]
async fn objects() {
	test(TG, move |context| async move {
		let directory = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export let h = tg.command(() => tg.file("h"));
				export let g = tg.command(() => tg.file("g"));
				export let f = tg.command(() => tg.file("f"));
				export let e = tg.command(() => tg.file("e"));
				export let d = tg.command(() => tg.directory({
					"h": h(),
				}));
				export let c = tg.command(() => tg.directory({
					"g": g(),
				}));
				export let b = tg.command(() => tg.directory({
					"f": f(),
				}));
				export let a = tg.command(() => tg.directory({
					"b": b(),
					"c": c(),
					"d": d(),
					"e": e(),
				}));
			"#),
		};

		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = directory.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		let a = build_command_get_object_id("a", &server, artifact_temp.path()).await;
		let b = build_command_get_object_id("b", &server, artifact_temp.path()).await;
		let c = build_command_get_object_id("c", &server, artifact_temp.path()).await;
		let d = build_command_get_object_id("d", &server, artifact_temp.path()).await;
		let e = build_command_get_object_id("e", &server, artifact_temp.path()).await;
		let f = build_command_get_object_id("f", &server, artifact_temp.path()).await;
		let g = build_command_get_object_id("g", &server, artifact_temp.path()).await;
		let h = build_command_get_object_id("h", &server, artifact_temp.path()).await;

		// Tag c.
		let output = server
			.tg()
			.arg("tag")
			.arg("c")
			.arg(c.clone())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Tag h.
		let output = server
			.tg()
			.arg("tag")
			.arg("h")
			.arg(h.clone())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Clean.
		let output = server.tg().arg("clean").output().await.unwrap();
		assert_success!(output);

		// Confirm presence/absence of objects.
		let a_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(a)
			.output()
			.await
			.unwrap();
		assert_failure!(a_output);

		let b_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(b)
			.output()
			.await
			.unwrap();
		assert_failure!(b_output);

		let c_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(c)
			.output()
			.await
			.unwrap();
		assert_success!(c_output);

		let d_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(d)
			.output()
			.await
			.unwrap();
		assert_failure!(d_output);

		let e_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(e)
			.output()
			.await
			.unwrap();
		assert_failure!(e_output);

		let f_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(f)
			.output()
			.await
			.unwrap();
		assert_failure!(f_output);

		let g_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(g)
			.output()
			.await
			.unwrap();
		assert_failure!(g_output);

		let h_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(h)
			.output()
			.await
			.unwrap();
		assert_success!(h_output);
	})
	.await;
}

async fn build_command_get_process_id(name: &str, server: &Server, path: &Path) -> String {
	let output = server
		.tg()
		.arg("process")
		.arg("--detach")
		.arg(format!("{}#{}", path.display(), name))
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();
	let output = server
		.tg()
		.arg("process")
		.arg("output")
		.arg(id)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	id.to_owned()
}

async fn build_command_get_object_id(name: &str, server: &Server, path: &Path) -> String {
	let output = server
		.tg()
		.arg("process")
		.arg(format!("{}#{}", path.display(), name))
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let output = std::str::from_utf8(&output.stdout).unwrap().trim();
	output.to_owned()
}
