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
				"link" => tg::symlink!(PathBuf::from("hello.txt")),
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
				"link1" => tg::symlink!(PathBuf::from("hello.txt")),
				"link2" => tg::symlink!(PathBuf::from("hello.txt")),
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
			for _ in 0..256 {
				let entries = [("a".to_owned(), last.clone())].into_iter().collect();
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
			tg::graph::object::Node::Symlink(tg::graph::object::Symlink::Artifact {
				artifact: Either::Left(0),
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
              "dir_01jgpeycbs5s4yjr89jqf3kkvy1a0rmrk7j2fmedscvh495h5b3740": {
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

#[tokio::test]
async fn shared_directory_dependency() -> tg::Result<()> {
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;

	let result = AssertUnwindSafe(async {
		let a = tg::directory! {
			"bin" => tg::directory! {
				"a" => "",
			},
			"usr" => tg::symlink!(PathBuf::from(".")),
		};
		let b = tg::Symlink::with_artifact_and_subpath(a.clone().into(), Some("bin/a".into()));
		let c = tg::File::with_object(tg::file::Object::Normal {
			contents: tg::Blob::with_reader(&server, b"c".as_slice()).await?,
			dependencies: [
				(
					tg::Reference::with_object(&a.id(&server).await?.into()),
					tg::Referent {
						item: a.clone().into(),
						path: None,
						subpath: None,
						tag: None,
					},
				),
				(
					tg::Reference::with_object(&b.id(&server).await?.into()),
					tg::Referent {
						item: b.clone().into(),
						path: None,
						subpath: None,
						tag: None,
					},
				),
			]
			.into_iter()
			.collect(),
			executable: false,
		});
		let d = tg::directory! {
			"c" => c,
		};
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(temp.path().to_owned()),
		};
		tg::Artifact::from(d).check_out(&server, arg).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(temp, server).await;
	result.unwrap()
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
