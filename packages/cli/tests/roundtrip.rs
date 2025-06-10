use insta::assert_snapshot;
use std::sync::Arc;
use tangram_cli::{
	assert_success,
	test::{self, test},
};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};
const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let directory = temp::directory! {
		"file" => temp::file!("hello!"),
		"link" => temp::symlink!("file"),
	};
	let assertions = async move |_server, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
	};
	test_roundtrip(directory, "", false, [], assertions).await;
}

#[tokio::test]
async fn file() {
	let file = temp::file!("hello, world!");
	let assertions = async move |_server, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
	};
	test_roundtrip(file, "", false, [], assertions).await;
}

#[tokio::test]
async fn tagged_dependency() {
	let foo = temp::directory! {
		"tangram.ts" => temp::file!(""),
	};
	let directory = temp::directory! {
		"tangram.ts" => temp::file!(r#"import * as foo from "foo/*"#),
	};
	let assertions = async move |_server, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
	};
	test_roundtrip(
		directory,
		"",
		false,
		[("foo/1.0.0".into(), foo.into())],
		assertions,
	)
	.await;
}

#[tokio::test]
async fn symlink() {
	let directory = temp::directory! {
		"file" => temp::file!("hello!"),
		"link" => temp::symlink!("file"),
	};
	let assertions = async move |_server, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
	};
	test_roundtrip(directory, "link", false, [], assertions).await;
}

#[tokio::test]
async fn artifact_symlink() {
	let tags = [("empty".to_owned(), temp::directory!().into())];
	let directory = temp::directory! {
		".tangram" => temp::directory! {
			"artifacts" => temp::directory! {
				"dir_01s3sv3tm5ntjq5fdst2zc28zy23wq0t20m2hyhnkaqvtdkhwqa2tg" => temp::directory!()
			}
		},
		"link" => temp::symlink!(".tangram/artifacts/dir_01s3sv3tm5ntjq5fdst2zc28zy23wq0t20m2hyhnkaqvtdkhwqa2tg"),
	};
	let assertions = async move |server: Arc<test::Server>, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
		assert_snapshot!(display(server.as_ref(), a).await, @r#"
		tg.directory({
		  "link": tg.symlink({
		    "artifact": tg.directory({}),
		  }),
		})
		"#);
	};
	test_roundtrip(directory, "", false, tags, assertions).await;
}

async fn display(server: &test::Server, object: impl Into<tg::Object>) -> String {
	let object = object.into().id().to_string();
	let object_output = server
		.tg()
		.arg("object")
		.arg("get")
		.arg(object)
		.arg("--depth=inf")
		.arg("--format=tgvn")
		.arg("--pretty=true")
		.output()
		.await
		.unwrap();
	assert_success!(object_output);
	std::str::from_utf8(&object_output.stdout).unwrap().into()
}

#[tokio::test]
async fn object_dependency() {
	let tags = [("empty".into(), temp::directory! {}.into())];
	let directory = temp::directory! {
		"file" => temp::File {
			contents: "".into(),
			executable: false,
			xattrs: [
				("user.tangram.dependencies".into(), r#"["dir_01s3sv3tm5ntjq5fdst2zc28zy23wq0t20m2hyhnkaqvtdkhwqa2tg"]"#.into())
			].into_iter().collect(),
		}
	};
	let assertions = async move |_server, a: tg::Artifact, b: tg::Artifact| {
		assert_eq!(a.id(), b.id());
	};
	test_roundtrip(directory, "", true, tags, assertions).await;
}

async fn test_roundtrip<'a, F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	deterministic: bool,
	tags: impl IntoIterator<Item = (String, temp::Artifact)> + Send + 'static,
	assertions: F,
) where
	F: FnOnce(Arc<test::Server>, tg::Artifact, tg::Artifact) -> Fut + Send + 'static + 'a,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let config = tangram_cli::Config {
			advanced: Some(tangram_cli::config::Advanced {
				error_trace_options: Some(tg::error::TraceOptions {
					internal: true,
					reverse: false,
				}),
				..Default::default()
			}),
			remotes: Some(Vec::new()),
			..Default::default()
		};
		let server = context.spawn_server_with_config(config).await.unwrap();

		// Tag the objects.
		let tags = tags.into_iter().collect::<Vec<_>>();
		for (tag, artifact) in &tags {
			let temp = Temp::new();
			artifact.to_path(&temp).await.unwrap();

			// Tag the dependency
			let output = server
				.tg()
				.arg("tag")
				.arg(tag)
				.arg(temp.path())
				.output()
				.await
				.unwrap();
			assert_success!(output);
		}

		// Write the artifact to a temp.
		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();

		// Check in.
		let mut command = server.tg();
		command.arg("checkin").arg(temp.path().join(path));
		if deterministic {
			command.arg("--deterministic");
		}
		let output = command.output().await.unwrap();
		assert_success!(output);

		// Get the object.
		let id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();
		let a = tg::Artifact::with_id(id.parse().unwrap());

		// Index.
		let output = server.tg().arg("index").output().await.unwrap();
		assert_success!(output);

		// Checkout.
		let temp = temp::Temp::new();
		let output = server
			.tg()
			.arg("checkout")
			.arg(&id)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Check back in.
		let mut command = server.tg();
		command.arg("checkin").arg(temp.path());
		if deterministic {
			command.arg("--deterministic");
		}
		let output = command.output().await.unwrap();

		assert_success!(output);
		let id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();
		let b = tg::Artifact::with_id(id.parse().unwrap());
		assertions(server, a, b).await;
	})
	.await;
}
