use {
	indoc::indoc,
	std::path::Path,
	tangram_cli_test::{Server, assert_failure, assert_success},
	tangram_client::prelude::*,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn processes() {
	let server = Server::new(TG).await.unwrap();

	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export let a = async () => {
				await tg.build(c);
				await tg.build(b);
				return tg.file("a");
			};
			export let b = async () => {
				await tg.build(e);
				await tg.build(d);
				return tg.file("b");
			};
			export let c = () => tg.file("c");
			export let d = () => tg.file("d");
			export let e = () => tg.file("e");
		"#),
	};

	let artifact: temp::Artifact = directory.into();
	let artifact_temp = Temp::new();
	artifact.to_path(artifact_temp.as_ref()).await.unwrap();

	let (a, _) = build(&server, artifact_temp.path(), "a").await;
	let (b, _) = build(&server, artifact_temp.path(), "b").await;
	let (c, _) = build(&server, artifact_temp.path(), "c").await;
	let (d, _) = build(&server, artifact_temp.path(), "d").await;
	let (e, _) = build(&server, artifact_temp.path(), "e").await;

	// Tag the processes.
	for (pattern, id) in [("b", &b), ("d", &d)] {
		let output = server
			.tg()
			.arg("tag")
			.arg(pattern)
			.arg(id.to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Clean.
	let output = server.tg().arg("clean").output().await.unwrap();
	assert_success!(output);

	// Confirm the presence of the processes.
	let a_output = server
		.tg()
		.arg("process")
		.arg("get")
		.arg(a.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(a_output);

	let output = server
		.tg()
		.arg("process")
		.arg("get")
		.arg(b.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let output = server
		.tg()
		.arg("process")
		.arg("get")
		.arg(c.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(output);

	let output = server
		.tg()
		.arg("process")
		.arg("get")
		.arg(d.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let output = server
		.tg()
		.arg("process")
		.arg("get")
		.arg(e.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);
}

#[tokio::test]
async fn objects() {
	let server = Server::new(TG).await.unwrap();
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export let a = () => tg.directory({
				"b": tg.build(b),
				"c": tg.build(c),
				"d": tg.build(d),
				"e": tg.build(e),
			});
			export let b = () => tg.directory({
				"f": tg.build(f),
			});
			export let c = () => tg.directory({
				"g": tg.build(g),
			});
			export let d = () => tg.directory({
				"h": tg.build(h),
			});
			export let e = () => tg.file("e");
			export let f = () => tg.file("f");
			export let g = () => tg.file("g");
			export let h = () => tg.file("h");
		"#),
	};

	let artifact: temp::Artifact = directory.into();
	let artifact_temp = Temp::new();
	artifact.to_path(artifact_temp.as_ref()).await.unwrap();

	let (_, a) = build(&server, artifact_temp.path(), "a").await;
	let (_, b) = build(&server, artifact_temp.path(), "b").await;
	let (_, c) = build(&server, artifact_temp.path(), "c").await;
	let (_, d) = build(&server, artifact_temp.path(), "d").await;
	let (_, e) = build(&server, artifact_temp.path(), "e").await;
	let (_, f) = build(&server, artifact_temp.path(), "f").await;
	let (_, g) = build(&server, artifact_temp.path(), "g").await;
	let (_, h) = build(&server, artifact_temp.path(), "h").await;

	// Tag c.
	let output = server
		.tg()
		.arg("tag")
		.arg("c")
		.arg(c.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Tag h.
	let output = server
		.tg()
		.arg("tag")
		.arg("h")
		.arg(h.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Clean.
	let output = server.tg().arg("clean").output().await.unwrap();
	assert_success!(output);

	// Confirm the presence of the objects.
	let a_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(a.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(a_output);

	let b_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(b.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(b_output);

	let c_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(c.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(c_output);

	let d_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(d.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(d_output);

	let e_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(e.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(e_output);

	let f_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(f.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(f_output);

	let g_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(g.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(g_output);

	let h_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(h.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(h_output);
}

#[tokio::test]
async fn double_tagged_item() {
	let server = Server::new(TG).await.unwrap();
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export let a = () => tg.file("a");
		"#),
	};

	let artifact: temp::Artifact = directory.into();
	let artifact_temp = Temp::new();
	artifact.to_path(artifact_temp.as_ref()).await.unwrap();

	let (_, a) = build(&server, artifact_temp.path(), "a").await;

	// Tag a with the same tag twice.
	let output = server
		.tg()
		.arg("tag")
		.arg("mytag")
		.arg(a.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let output = server
		.tg()
		.arg("tag")
		.arg("mytag")
		.arg(a.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Delete the tag.
	let output = server
		.tg()
		.arg("tag")
		.arg("delete")
		.arg("mytag")
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Clean.
	let output = server.tg().arg("clean").output().await.unwrap();
	assert_success!(output);

	// Confirm that a was cleaned (no longer tagged).
	let a_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(a.to_string())
		.output()
		.await
		.unwrap();
	assert_failure!(a_output);
}

async fn build(
	server: &tangram_cli_test::Server,
	path: &Path,
	name: &str,
) -> (tg::process::Id, tg::object::Id) {
	let output = server
		.tg()
		.arg("process")
		.arg("spawn")
		.arg("--sandbox")
		.arg(format!("{}#{}", path.display(), name))
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let id = serde_json::from_slice::<tg::process::spawn::Output>(&output.stdout)
		.unwrap()
		.process;
	let output = server
		.tg()
		.arg("process")
		.arg("output")
		.arg(id.to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let output = std::str::from_utf8(&output.stdout)
		.unwrap()
		.parse()
		.unwrap();
	(id, output)
}
