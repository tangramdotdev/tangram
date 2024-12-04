use crate::{Config, Server};
use futures::{future, Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

/// Test checking out a directory.
#[tokio::test]
async fn directory() -> tg::Result<()> {
	let artifact = tg::directory! {
		"hello.txt" => "Hello, World!",
	};
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "hello.txt": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": false
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"hello.txt\":1}},{\"kind\":\"file\",\"contents\":\"lef_015258d9wz42hxdq6ds9vh7fnet5w7k0mpqqx7j4zt59hdjwkvz3w0\"}]}",
        "executable": false
      }
    }
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a single file.
#[tokio::test]
async fn file() -> tg::Result<()> {
	let artifact = tg::file!("Hello, World!");
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

/// Test checking out an executable file.
#[tokio::test]
async fn executable_file() -> tg::Result<()> {
	let artifact = tg::file!("Hello, World!", executable = true);
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": true
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a a file with a dependency.
#[tokio::test]
async fn file_with_dependency() -> tg::Result<()> {
	let artifact = tg::file!(
		"foo",
		dependencies = [(
			"bar".parse().unwrap(),
			tg::Referent::with_item(tg::file!("bar").into())
		)]
	);
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "foo",
    "executable": false
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a symlink.
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
             "kind": "symlink",
             "target": "hello.txt"
           }
         }
       },
       "tangram.lock": {
         "kind": "file",
         "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"directory\":1}},{\"kind\":\"directory\",\"entries\":{\"hello.txt\":2,\"link\":3}},{\"kind\":\"file\",\"contents\":\"lef_015258d9wz42hxdq6ds9vh7fnet5w7k0mpqqx7j4zt59hdjwkvz3w0\"},{\"kind\":\"symlink\",\"Target\":{\"target\":\"hello.txt\"}}]}",
         "executable": false
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

/// Test checking out a directory with a symlink.
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
             "kind": "symlink",
             "target": "hello.txt"
           },
           "link2": {
             "kind": "symlink",
             "target": "hello.txt"
           }
         }
       },
       "tangram.lock": {
         "kind": "file",
         "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"directory\":1}},{\"kind\":\"directory\",\"entries\":{\"hello.txt\":2,\"link1\":3,\"link2\":3}},{\"kind\":\"file\",\"contents\":\"lef_015258d9wz42hxdq6ds9vh7fnet5w7k0mpqqx7j4zt59hdjwkvz3w0\"},{\"kind\":\"symlink\",\"Target\":{\"target\":\"hello.txt\"}}]}",
         "executable": false
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

/// Test checking out a very deep directory.
#[ignore]
#[tokio::test]
async fn deeply_nested_directory() -> tg::Result<()> {
	let mut artifact = tg::Artifact::from(tg::file!("hello"));
	for _ in 0..256 {
		let entries = [("a".into(), artifact.clone())].into_iter().collect();
		artifact = tg::Directory::with_entries(entries).into();
	}
	test(artifact, |_, _| future::ok(())).await
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() -> tg::Result<()> {
	let artifact = tg::directory! {
		"foo" => tg::file!("foo", dependencies = [
			("bar".parse().unwrap(), tg::Referent::with_item(tg::file!("bar").into()))
		]),
	};
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
              "fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80": {
                "kind": "file",
                "contents": "bar",
                "executable": false
              }
            }
          }
        }
      },
      "foo": {
        "kind": "file",
        "contents": "foo",
        "executable": false
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"foo\":1}},{\"kind\":\"file\",\"contents\":\"lef_010kgbpefk1cd3ztw9ymvcjez1a1amgbfq91kmp06jdsd7axvq0bmg\",\"dependencies\":{\"bar\":{\"item\":2}}},{\"kind\":\"file\",\"contents\":\"lef_01ybm9fvpqt83cv1ax8gashyjj3ay7bampjmz9fg1gs5gjrc6154yg\"}]}",
        "executable": false
      }
    }
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() -> tg::Result<()> {
	let artifact = tg::directory! {
		"foo" => tg::symlink!(artifact = tg::file!("bar")),
	};
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
              "fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80": {
                "kind": "file",
                "contents": "bar",
                "executable": false
              }
            }
          }
        }
      },
      "foo": {
        "kind": "symlink",
        "target": ".tangram/artifacts/fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80"
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"foo\":1}},{\"kind\":\"symlink\",\"Artifact\":{\"artifact\":2}},{\"kind\":\"file\",\"contents\":\"lef_01ybm9fvpqt83cv1ax8gashyjj3ay7bampjmz9fg1gs5gjrc6154yg\"}]}",
        "executable": false
      }
    }
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() -> tg::Result<()> {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::Directory(
			tg::graph::object::Directory {
				entries: [(
					"hello.txt".to_owned(),
					Either::Right(tg::file!("Hello, World!").into()),
				)]
				.into(),
			},
		)],
	});
	let artifact = tg::Directory::with_graph_and_node(graph, 0);
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "hello.txt": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": false
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"hello.txt\":1}},{\"kind\":\"file\",\"contents\":\"lef_015258d9wz42hxdq6ds9vh7fnet5w7k0mpqqx7j4zt59hdjwkvz3w0\"}]}",
        "executable": false
      }
    }
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() -> tg::Result<()> {
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

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() -> tg::Result<()> {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::Symlink(
			tg::graph::object::Symlink::Target {
				target: "/bin/sh".into(),
			},
		)],
	});
	let artifact = tg::Symlink::with_graph_and_node(graph, 0);
	test(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "symlink",
    "target": "/bin/sh"
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() -> tg::Result<()> {
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
      "link": {
        "kind": "symlink",
        "target": "link"
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\"nodes\":[{\"kind\":\"directory\",\"entries\":{\"link\":1}},{\"kind\":\"symlink\",\"Artifact\":{\"artifact\":0,\"subpath\":\"link\"}}]}",
        "executable": false
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
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap()
}
