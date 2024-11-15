use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(tg::file!("Hello, World!"), |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": false
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

#[tokio::test]
async fn executable_file() -> tg::Result<()> {
	test(
		tg::file!("Hello, World!", executable = true),
		|_, artifact| async move {
			assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": true
  }
  "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		tg::directory! {
			"directory" => tg::directory! {
				"hello.txt" => "Hello, World!",
				"link" => tg::symlink!(None, Some(PathBuf::from("hello.txt"))),
			}
		},
		|_, artifact| async move {
			assert_json_snapshot!(artifact, @r#"
   {
     "kind": "directory",
     "entries": {
       "directory": {
         "kind": "directory",
         "entries": {
           "hello.txt": {
             "kind": "file",
             "contents": "Hello, World!",
             "executable": false
           },
           "link": {
             "kind": "file",
             "contents": "Hello, World!",
             "executable": false
           }
         }
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink_shared_target() -> tg::Result<()> {
	test(
		tg::directory! {
			"directory" => tg::directory! {
				"hello.txt" => "Hello, World!",
				"link1" => tg::symlink!(None, Some(PathBuf::from("hello.txt"))),
				"link2" => tg::symlink!(None, Some(PathBuf::from("hello.txt"))),
			}
		},
		|_, artifact| async move {
			assert_json_snapshot!(artifact, @r#"
   {
     "kind": "directory",
     "entries": {
       "directory": {
         "kind": "directory",
         "entries": {
           "hello.txt": {
             "kind": "file",
             "contents": "Hello, World!",
             "executable": false
           },
           "link1": {
             "kind": "file",
             "contents": "Hello, World!",
             "executable": false
           },
           "link2": {
             "kind": "file",
             "contents": "Hello, World!",
             "executable": false
           }
         }
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn graph() -> tg::Result<()> {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::File(tg::graph::object::File {
			contents: "Hello, World!".into(),
			dependencies: BTreeMap::new(),
			executable: false,
		})],
	});
	let file = tg::File::with_graph_and_node(graph, 0);
	test(file, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": false
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

async fn test<F, Fut>(artifact: impl Into<tg::Artifact>, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, temp::Artifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;
	let result = AssertUnwindSafe(async {
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			path: Some(temp.path().to_owned()),
		};
		let id = artifact.into().id(&server).await?;
		let stream = server.check_out_artifact(&id, arg).await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = temp::Artifact::with_path(temp.path()).await?;
		(assertions)(server.clone(), artifact).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
