use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
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
async fn deeply_nested_directory() -> tg::Result<()> {
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;
	let result = AssertUnwindSafe({
		let server = server.clone();
		async move {
			let mut last = tg::Artifact::from(tg::file!("hello"));
			for n in 0..512 {
				let entries = [(format!("{n}"), last.clone())].into_iter().collect();
				last = tg::Directory::with_entries(entries).into();
			}
			last.store(&server).await?;
			last.unload();
			last.check_out(&server, tg::artifact::checkout::Arg::default())
				.await?;
			Ok::<_, tg::Error>(())
		}
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
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
	let artifact = tg::File::with_graph_and_node(graph, 0);
	test(artifact, |_, artifact| async move {
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
async fn cyclic_symlink() -> tg::Result<()> {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![
			tg::graph::object::Node::Directory(tg::graph::object::Directory {
				entries: [("link".to_owned(), Either::Left(1))].into(),
			}),
			tg::graph::object::Node::Symlink(tg::graph::object::Symlink {
				artifact: Some(Either::Left(0)),
				subpath: Some("link".into()),
			}),
		],
	});
	let artifact = tg::Directory::with_graph_and_node(graph, 0);
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      ".tangram": {
        "kind": "directory",
        "entries": {
          "artifacts": {
            "kind": "directory",
            "entries": {
              "dir_014yyvsnfgj1dsd3s7dctta79hmjm3rq6sya1t7hymygjm97ynqhng": {
                "kind": "directory",
                "entries": {
                  "link": {
                    "kind": "symlink",
                    "target": "link"
                  }
                }
              }
            }
          }
        }
      },
      "link": {
        "kind": "symlink",
        "target": "link"
      }
    }
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
			dependencies: true,
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
