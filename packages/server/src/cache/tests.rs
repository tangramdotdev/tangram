use crate::{Server, test::test};
use insta::assert_json_snapshot;
use std::{collections::BTreeMap, pin::pin};
use tangram_client as tg;
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
		    "dir_01pwxwwvy02jgj944jbsf3a9e8ck2yaz3nfs3bxgk221tm5tkdhq40": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!"
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a file.
#[tokio::test]
async fn file() {
	let artifact = tg::file!("Hello, World!");
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60": {
		      "kind": "file",
		      "contents": "Hello, World!"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching an executable file.
#[tokio::test]
async fn executable_file() {
	let artifact = tg::file!("Hello, World!", executable = true);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "fil_01qdmew2fxgyx15vq2zh2726pvhbfpa6jzvhrr7sn1pv0dpt9ms0vg": {
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
		    "dir_018zb48hy214fgq3gnnbjcpdcer4ft89b366n5hp3d2ea2zm3awjsg": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!"
		        },
		        "world.txt": {
		          "kind": "file",
		          "contents": "Hello, World!"
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a file with a dependency.
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
		    "fil_012aqdy9h7v8bermq0hvx4jgpr8apz26st50tx734bha2hdfd0hpa0": {
		      "kind": "file",
		      "contents": "foo",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"bar\"]"
		      }
		    },
		    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		      "kind": "file",
		      "contents": "bar"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a symlink.
#[tokio::test]
async fn symlink() {
	let artifact = tg::symlink!("/bin/sh");
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "sym_01sjeq01fm5g3jqq57bams2kxsprz7e8mhtpkbagxhvb1xg8f12z2g": {
		      "kind": "symlink",
		      "path": "/bin/sh"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a directory with a symlink.
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
		    "dir_01xk6kmg1pke81bp3e73gza5xapstvn573g1vx700ndpm46ev5nn90": {
		      "kind": "directory",
		      "entries": {
		        "directory": {
		          "kind": "directory",
		          "entries": {
		            "hello.txt": {
		              "kind": "file",
		              "contents": "Hello, World!"
		            },
		            "link": {
		              "kind": "symlink",
		              "path": "hello.txt"
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

/// Test caching a directory with a file with a dependency.
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
		    "dir_0173ef9z62g971hya06x9qh9da0cv5nrksgff5g3e73texf2h9gx50": {
		      "kind": "directory",
		      "entries": {
		        "foo": {
		          "kind": "file",
		          "contents": "foo",
		          "xattrs": {
		            "user.tangram.dependencies": "[\"bar\"]"
		          }
		        }
		      }
		    },
		    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		      "kind": "file",
		      "contents": "bar"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a directory with a symlink with a dependency.
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
		    "dir_013da6hc37p6py2gq37em7npkhhxrr145ec6aw9qenx6d62nftbwsg": {
		      "kind": "directory",
		      "entries": {
		        "foo": {
		          "kind": "symlink",
		          "path": "../fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg"
		        }
		      }
		    },
		    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		      "kind": "file",
		      "contents": "bar"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::Directory(
			tg::graph::object::Directory {
				entries: [(
					"hello.txt".to_owned(),
					tg::graph::object::Edge::Object(tg::file!("Hello, World!").into()),
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
		    "dir_01xq6jv8d4ye1qkxf3kmc0jzm2szavhfypem762rms22sqmqaeab2g": {
		      "kind": "directory",
		      "entries": {
		        "hello.txt": {
		          "kind": "file",
		          "contents": "Hello, World!"
		        }
		      }
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a file that is a member of a graph.
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
		    "fil_01yw9bpm8d571j3jg3pqx9xsa087s8w3tpf0ewr7jcq53gvpe2hmjg": {
		      "kind": "file",
		      "contents": "Hello, World!"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![tg::graph::object::Node::Symlink(
			tg::graph::object::Symlink {
				artifact: None,
				path: Some("/bin/sh".into()),
			},
		)],
	});
	let artifact = tg::Symlink::with_graph_and_node(graph, 0);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "sym_01xcz8v0pcvedwb43gx6dcrdy7w87rnmv3wfyc6s2zsmchme6628cg": {
		      "kind": "symlink",
		      "path": "/bin/sh"
		    }
		  }
		}
		"#);
	})
	.await;
}

/// Test caching a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
	let graph = tg::Graph::with_object(tg::graph::Object {
		nodes: vec![
			tg::graph::object::Node::Directory(tg::graph::object::Directory {
				entries: [(
					"link".to_owned(),
					tg::graph::object::Edge::Reference(tg::graph::object::Reference {
						graph: None,
						node: 1,
					}),
				)]
				.into(),
			}),
			tg::graph::object::Node::Symlink(tg::graph::object::Symlink {
				artifact: Some(tg::graph::object::Edge::Reference(
					tg::graph::object::Reference {
						graph: None,
						node: 0,
					},
				)),
				path: Some("link".into()),
			}),
		],
	});
	let artifact = tg::Directory::with_graph_and_node(graph, 0);
	test_cache(artifact, |_, artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "dir_01ra86dj63y1evzwrnbf1779h09dhea2nps68sdk49gy1z8fwx56h0": {
		      "kind": "directory",
		      "entries": {
		        "link": {
		          "kind": "symlink",
		          "path": "link"
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
		let artifact = artifact.into().store(&server).await.unwrap();
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
