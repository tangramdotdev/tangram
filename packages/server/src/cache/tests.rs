use crate::{Server, test::test};
use futures::future;
use insta::assert_json_snapshot;
use std::{collections::BTreeMap, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryExt as _;
use tangram_temp as temp;

/// Test caching a directory.
#[tokio::test]
async fn directory() {
	let artifact = tg::directory! {
		"hello.txt" => "Hello, World!",
	};
	test_cache(artifact, |_, cache| async move {
		assert_json_snapshot!(cache, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_01gn2yn2wk00wh3w1tcse628ghxj734a2c6qjd8e7g4553qzq2vs1g": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!",
		          "executable": false
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a single file.
#[tokio::test]
async fn file() {
	let artifact = tg::file!("Hello, World!");
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": false
      }
    }
  }
  "#);
	})
	.await;
}

/// Test checking out an executable file.
#[tokio::test]
async fn executable_file() {
	let artifact = tg::file!("Hello, World!", executable = true);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "fil_01yf0xwv92p30wwyp5vpt925tyw4nwkyzt4b3fv4g3hc8wr4nsk8j0": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": true
      }
    }
  }
  "#);
	})
	.await;
}

/// Test caching a directory with two identical files.
#[tokio::test]
async fn directory_with_two_identical_files() {
	let artifact = tg::directory! {
		"hello.txt" => "Hello, World!",
		"world.txt" => "Hello, World!",
	};
	test_cache(artifact, |_, cache| async move {
		assert_json_snapshot!(cache, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_0184z14k1w0vne39fsb6ytk6d8yk5wannz3r9g94hyh0hn5tx5x1gg": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!",
		          "executable": false
		        },
		        "world.txt": {
		          "kind": "file",
		          "contents": "Hello, World!",
		          "executable": false
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a a file with a dependency.
#[tokio::test]
async fn file_with_dependency() {
	let artifact = tg::file!(
		"foo",
		dependencies = [(
			"bar".parse().unwrap(),
			tg::Referent::with_item(tg::file!("bar").into())
		)]
	);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80": {
        "kind": "file",
        "contents": "bar",
        "executable": false
      },
      "fil_01tsgfzwa97w008amycfw2zbywvj56hac3164dgqp9qj1we854rkg0": {
        "kind": "file",
        "contents": "foo",
        "executable": false
      }
    }
  }
  "#);
	})
	.await;
}

/// Test checking out a symlink.
#[tokio::test]
async fn symlink() {
	let artifact = tg::symlink!("/bin/sh");
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "sym_01xpnr55xrsjcwcc9ppryzqry6r2m15k17kzjxjakyfs4g5fvksqqg": {
        "kind": "symlink",
        "target": "/bin/sh"
      }
    }
  }
  "#);
	})
	.await;
}

/// Test checking out a directory with a symlink.
#[tokio::test]
async fn directory_with_symlink() {
	let artifact = tg::directory! {
		"directory" => tg::directory! {
			"hello.txt" => "Hello, World!",
			"link" => tg::symlink!("hello.txt"),
		}
	};
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_01zbk5rvcgyfg20ktzxkfbxsa848dc2ayyvtz0ca32x35nyfw7vc80": {
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
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a very deep directory.
#[ignore]
#[tokio::test]
async fn deeply_nested_directory() {
	let mut artifact = tg::Artifact::from(tg::file!("hello"));
	for _ in 0..256 {
		let entries = [("a".into(), artifact.clone())].into_iter().collect();
		artifact = tg::Directory::with_entries(entries).into();
	}
	test_cache(artifact, |_, _| future::ready(())).await;
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() {
	let artifact = tg::directory! {
		"foo" => tg::file!("foo", dependencies = [
			("bar".parse().unwrap(), tg::Referent::with_item(tg::file!("bar").into()))
		]),
	};
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_019at517ytj7gfg92358f53w7c2ht4gewvtg45fegwr6teffnnvxa0": {
		      "kind": "directory",
		      "entries": {
		        "foo": {
		          "kind": "file",
		          "contents": "foo",
		          "executable": false
		        }
		      }
		    },
		    "fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80": {
		      "kind": "file",
		      "contents": "bar",
		      "executable": false
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() {
	let artifact = tg::directory! {
		"foo" => tg::symlink!(artifact = tg::file!("bar")),
	};
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "dir_01tazznbadf4db8hadnz5h145x288ne3dsg3zkttcp6xnj82vy6wm0": {
        "kind": "directory",
        "entries": {
          "foo": {
            "kind": "symlink",
            "target": "../fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80"
          }
        }
      },
      "fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80": {
        "kind": "file",
        "contents": "bar",
        "executable": false
      }
    }
  }
  "#);
	})
	.await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
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
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_01s6gw40j6yme9cc6qq7sgdfck53e1ww64mx0cw9r401t3tg60yw60": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!",
		          "executable": false
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::File(tg::graph::object::File {
			contents: "Hello, World!".into(),
			dependencies: BTreeMap::new(),
			executable: false,
		})],
	});
	let artifact = tg::File::with_graph_and_node(graph, 0);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "fil_011d4n8a29e5pb8x8nmzkq16aawpn2ygeygv37t4ns2gpykgpd5kkg": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": false
      }
    }
  }
  "#);
	})
	.await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::Symlink(
			tg::graph::object::Symlink::Target {
				target: "/bin/sh".into(),
			},
		)],
	});
	let artifact = tg::Symlink::with_graph_and_node(graph, 0);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "sym_018t9cagpp8nzqv9vf0v3yrkpnm9betvtv82vjrh3n6qjhf7rk3df0": {
		      "kind": "symlink",
		      "target": "/bin/sh"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
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
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
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
		"#);
	})
	.await;
}

async fn test_cache<F, Fut>(artifact: impl Into<tg::Artifact> + Send + 'static, assertions: F)
where
	F: FnOnce(Server, temp::Artifact) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send + 'static,
{
	test(async move |context| {
		let server = context.start_server().await;
		let artifact = artifact.into().id(&server).await.unwrap();
		let arg = tg::checkout::Arg {
			artifact,
			dependencies: true,
			force: false,
			lockfile: false,
			path: None,
		};
		let stream = server.checkout(arg).await.unwrap();
		let _ = pin!(stream)
			.try_last()
			.await
			.unwrap()
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))
			.unwrap();
		let artifact = temp::Artifact::with_path(&server.cache_path())
			.await
			.unwrap();
		(assertions)(server, artifact).await;
	})
	.await;
}
