use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe, path::Path};
use tangram_client as tg;
use tangram_either::Either;
use tangram_temp::{self as temp, Temp};

#[allow(dead_code)]
struct TestArtifact {
	// The server the artifact exists upon.
	server: Server,

	// The tangram artifact.
	artifact: tg::Artifact,

	// The artifact that was checked in.
	checkin: temp::Artifact,

	// The artifact that was checked out.
	checkout: temp::Artifact,
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello.txt" => "hello, world!",
		},
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.directory({
            	"hello.txt": tg.file({
            		"contents": tg.leaf("hello, world!"),
            	}),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(
		temp::file!("hello, world!"),
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.file({
            	"contents": tg.leaf("hello, world!"),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"link" => temp::symlink!("hello.txt"),
			"hello.txt" => "hello, world!",
		},
		"link".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.symlink({
            	"target": "hello.txt",
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#""#,
			},
		},
		"foo".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import * as bar from "../bar""),
   		"dependencies": {
   			"../bar": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   				}),
   				"path": "../bar",
   				"subpath": "tangram.ts",
   			},
   		},
   	}),
   })
   "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn lockfile_roundtrip() -> tg::Result<()> {
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;

	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![
			tg::graph::object::Node::Directory(tg::graph::object::Directory {
				entries: [
					("a".to_owned(), Either::Left(1)),
					("b".to_owned(), Either::Left(2)),
				]
				.into_iter()
				.collect(),
			}),
			tg::graph::object::Node::File(tg::graph::object::File {
				contents: "a".into(),
				dependencies: [(
					tg::Reference::with_path("./b"),
					tg::Referent {
						item: Either::Left(0),
						subpath: Some("b".into()),
						path: None,
						tag: None,
					},
				)]
				.into_iter()
				.collect(),
				executable: false,
			}),
			tg::graph::object::Node::File(tg::graph::object::File {
				contents: "b".into(),
				dependencies: [(
					tg::Reference::with_path("./a"),
					tg::Referent {
						item: Either::Left(0),
						subpath: Some("a".into()),
						path: None,
						tag: None,
					},
				)]
				.into_iter()
				.collect(),
				executable: false,
			}),
		],
	});

	let result = AssertUnwindSafe(async {
		let artifact: tg::Artifact = tg::Directory::with_graph_and_node(graph.clone(), 0).into();
		let temp = Temp::new();

		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			dependencies: true,
			force: false,
		};
		let path = artifact.check_out(&server, arg).await?;

		let lockfile = server
			.parse_lockfile(&path.join(tg::package::LOCKFILE_FILE_NAME))
			.await?;
		let graph_id = graph.id(&server).await?;
		assert!(lockfile.graphs.contains_key(&graph_id));

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn cyclic_path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "../foo""#,
			},
		},
		"foo".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as bar from "../bar""),
   				"dependencies": {
   					"../bar": {
   						"item": 2,
   						"path": "../bar",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 3,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as foo from "../foo""),
   				"dependencies": {
   					"../foo": {
   						"item": 0,
   						"path": "",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			Ok(())
		},
	)
	.await
}

async fn test<F, Fut>(checkin: temp::Artifact, path: &Path, assertions: F) -> tg::Result<()>
where
	F: FnOnce(TestArtifact, TestArtifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	// Create the first server.
	let temp1 = Temp::new();
	let config = Config::with_path(temp1.path().to_owned());
	let server1 = Server::start(config.clone()).await?;

	// Create the second server.
	let temp2 = Temp::new();
	let config = Config::with_path(temp2.path().to_owned());
	let server2 = Server::start(config.clone()).await?;

	// Run the test.
	let result = AssertUnwindSafe(async {
		// Create an initial temp.
		let temp = Temp::new();

		// Realize the artifact.
		checkin.to_path(temp.path()).await.unwrap();

		// Check in the artifact to the first server.
		let arg = tg::artifact::checkin::Arg {
			path: temp.path().join(path),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		let artifact = tg::Artifact::check_in(&server1, arg).await?;

		// Check the artifact out from the first server.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			dependencies: true,
			force: false,
		};
		let path = artifact.check_out(&server1, arg).await?;
		let checkout = temp::Artifact::with_path(&path).await?;

		// Create the test data for the first server.
		let artifact1 = TestArtifact {
			server: server1.clone(),
			artifact,
			checkin,
			checkout,
		};

		// Check the artifact into a second server.
		let checkin = temp::Artifact::with_path(&path).await?;
		let arg = tg::artifact::checkin::Arg {
			path,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		let artifact = tg::Artifact::check_in(&server2, arg).await?;

		// Check it out.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			dependencies: true,
			force: false,
		};
		let path = artifact.check_out(&server2, arg).await?;

		let checkout = temp::Artifact::with_path(&path).await?;
		let artifact2 = TestArtifact {
			server: server2.clone(),
			artifact,
			checkin,
			checkout,
		};

		assertions(artifact1, artifact2).await
	})
	.catch_unwind()
	.await;
	// Cleanup the servers.
	cleanup(temp1, server1).await;
	cleanup(temp2, server2).await;
	result.unwrap()
}

impl TestArtifact {
	async fn output(&self) -> String {
		let object: tg::Object = self.artifact.clone().into();
		object.load_recursive(&self.server).await.unwrap();
		let value = tg::Value::Object(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		value.print(options)
	}

	async fn id(&self) -> tg::artifact::Id {
		self.artifact.id(&self.server).await.unwrap()
	}
}
