use indoc::indoc;
use insta::assert_json_snapshot;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
			})
		}
	"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "hello.txt": {
		      "kind": "file",
		      "contents": "Hello, World!"
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("Hello, World!") 
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "file",
		  "contents": "Hello, World!"
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn executable_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file({
					contents: "Hello, World!",
					executable: true,
				})
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "file",
		  "contents": "Hello, World!",
		  "executable": true
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn file_with_tag_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file({
					contents: "foo",
					dependencies: {
						"bar": {
							item: tg.file("bar")
						} 
					}
				})
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "file",
		  "contents": "foo",
		  "xattrs": {
		    "user.tangram.dependencies": "[\"bar\"]",
		    "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"bar\":\"fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg\"}}]}"
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn file_with_id_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let dependency = await tg.file("bar");
				return tg.file({
					contents: "foo",
					dependencies: {
						[dependency.id]: dependency, 
					}
				})
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "file",
		  "contents": "foo",
		  "xattrs": {
		    "user.tangram.dependencies": "[\"fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg\"]"
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link": tg.symlink("hello.txt")
				})
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
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
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a symlink.
#[tokio::test]
async fn symlink_shared_target() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link1": tg.symlink("hello.txt"),
					"link2": tg.symlink("hello.txt")
				})
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "hello.txt": {
		      "kind": "file",
		      "contents": "Hello, World!"
		    },
		    "link1": {
		      "kind": "symlink",
		      "path": "hello.txt"
		    },
		    "link2": {
		      "kind": "symlink",
		      "path": "hello.txt"
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"foo": tg.file({
						contents: "foo", 
						dependencies: {
							"bar": {
								item: tg.file("bar") 
							}
						}
					}) 
				})
			}
		"#),
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
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
		            "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		              "kind": "file",
		              "contents": "bar"
		            }
		          }
		        }
		      }
		    },
		    "foo": {
		      "kind": "file",
		      "contents": "foo",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"bar\"]"
		      }
		    },
		    "tangram.lock": {
		      "kind": "file",
		      "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"foo\": 1\n      }\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"bar\": \"fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg\"\n      }\n    }\n  ]\n}"
		    }
		  }
		}
		"#);
		let lockfile = artifact
			.unwrap_directory_ref()
			.entries
			.get("tangram.lock")
			.unwrap()
			.unwrap_file_ref();
		let lockfile = serde_json::from_str::<serde_json::Value>(&lockfile.contents).unwrap();
		assert_json_snapshot!(lockfile, @r#"
		{
		  "nodes": [
		    {
		      "entries": {
		        "foo": 1
		      },
		      "kind": "directory"
		    },
		    {
		      "dependencies": {
		        "bar": "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg"
		      },
		      "kind": "file"
		    }
		  ]
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_id_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let dependency = await tg.file("bar");
				return tg.directory({
					"foo": tg.file({
						contents: "foo", 
						dependencies: {
							[dependency.id]: dependency,
						},
					}) 
				})
			}
		"#),
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
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
		            "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		              "kind": "file",
		              "contents": "bar"
		            }
		          }
		        }
		      }
		    },
		    "foo": {
		      "kind": "file",
		      "contents": "foo",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg\"]"
		      }
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_id_dependency_with_tag_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let dependency = await tg.file("bar", { dependencies: { baz: tg.file("baz") } });
				return tg.directory({
					"foo": tg.file({
						contents: "foo", 
						dependencies: {
							[dependency.id]: dependency,
						},
					}) 
				})
			}
		"#),
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
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
		            "fil_019xf26zfnft8rmgdj92h3633z9sgwwtp0jqg1xkyyvtakznt5j4kg": {
		              "kind": "file",
		              "contents": "bar",
		              "xattrs": {
		                "user.tangram.dependencies": "[\"baz\"]"
		              }
		            },
		            "fil_01jbw9dcbd06t7zn44bgfvq6radajd68mpjqz2jf1xhypnakvs2tzg": {
		              "kind": "file",
		              "contents": "baz"
		            }
		          }
		        }
		      }
		    },
		    "foo": {
		      "kind": "file",
		      "contents": "foo",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"fil_019xf26zfnft8rmgdj92h3633z9sgwwtp0jqg1xkyyvtakznt5j4kg\"]"
		      }
		    },
		    "tangram.lock": {
		      "kind": "file",
		      "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"foo\": 1\n      }\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"fil_019xf26zfnft8rmgdj92h3633z9sgwwtp0jqg1xkyyvtakznt5j4kg\": {\n          \"item\": 2\n        }\n      }\n    },\n    {\n      \"kind\": \"file\",\n      \"contents\": \"blb_01p5qf596t7vpc0nnx8q9c5gpm3271t2cqj16yb0e5zyd880ncc3tg\",\n      \"dependencies\": {\n        \"baz\": \"fil_01jbw9dcbd06t7zn44bgfvq6radajd68mpjqz2jf1xhypnakvs2tzg\"\n      }\n    }\n  ]\n}"
		    }
		  }
		}
		"#);
		let lockfile = artifact
			.unwrap_directory_ref()
			.entries
			.get("tangram.lock")
			.unwrap()
			.unwrap_file_ref();
		let lockfile = serde_json::from_str::<serde_json::Value>(&lockfile.contents).unwrap();
		assert_json_snapshot!(lockfile, @r#"
		{
		  "nodes": [
		    {
		      "entries": {
		        "foo": 1
		      },
		      "kind": "directory"
		    },
		    {
		      "dependencies": {
		        "fil_019xf26zfnft8rmgdj92h3633z9sgwwtp0jqg1xkyyvtakznt5j4kg": {
		          "item": 2
		        }
		      },
		      "kind": "file"
		    },
		    {
		      "contents": "blb_01p5qf596t7vpc0nnx8q9c5gpm3271t2cqj16yb0e5zyd880ncc3tg",
		      "dependencies": {
		        "baz": "fil_01jbw9dcbd06t7zn44bgfvq6radajd68mpjqz2jf1xhypnakvs2tzg"
		      },
		      "kind": "file"
		    }
		  ]
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"foo": tg.symlink({artifact: tg.file("bar")})
				})
			}
		"#),
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
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
		            "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
		              "kind": "file",
		              "contents": "bar"
		            }
		          }
		        }
		      }
		    },
		    "foo": {
		      "kind": "symlink",
		      "path": ".tangram/artifacts/fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg"
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "directory",
							entries: { "hello.txt": tg.file("Hello, World!") },
						},
					],
				});
				return tg.directory({ graph, node: 0 });
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "hello.txt": {
		      "kind": "file",
		      "contents": "Hello, World!"
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [{
						kind: "file", 
						contents: "Hello, World!", 
						executable: false,
					}],
				});
				return tg.file({ graph, node: 0 });  
			};
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "file",
		  "contents": "Hello, World!"
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [{
						kind: "symlink", 
						path: "/bin/sh",
					}],
				});
				return tg.symlink({ graph, node: 0 });  
			}
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "symlink",
		  "path": "/bin/sh"
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph =  tg.graph({
					nodes: [{
							kind: "directory", 
							entries: {"link": 1}
						},
						{
							kind: "symlink", 
							artifact: 0, 
							path: "link"
						},
					],
				});
				return tg.directory({ graph, node: 0 });
			}
		"#)
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    "link": {
		      "kind": "symlink",
		      "path": "link"
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn shared_dependency_on_symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let dependency = await tg.directory({
					"file.txt": "contents",
					"link": tg.symlink("file.txt"),
				});
				let id = dependency.id;
				return tg.directory({
					"foo.txt": tg.file("foo", { dependencies: { [id]: { item: dependency }}}),
					"bar.txt": tg.file("bar", { dependencies: { [id]: { item: dependency }}})
				});
			}
		"#),
	};
	let dependencies = true;
	let assertions = |artifact: temp::Artifact| async move {
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
		            "dir_01xsy9adcmt0gjakvwt33stxa48tjgcjyrd6c3hdz0gjw9j3h2d680": {
		              "kind": "directory",
		              "entries": {
		                "file.txt": {
		                  "kind": "file",
		                  "contents": "contents"
		                },
		                "link": {
		                  "kind": "symlink",
		                  "path": "file.txt"
		                }
		              }
		            }
		          }
		        }
		      }
		    },
		    "bar.txt": {
		      "kind": "file",
		      "contents": "bar",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"dir_01xsy9adcmt0gjakvwt33stxa48tjgcjyrd6c3hdz0gjw9j3h2d680\"]"
		      }
		    },
		    "foo.txt": {
		      "kind": "file",
		      "contents": "foo",
		      "xattrs": {
		        "user.tangram.dependencies": "[\"dir_01xsy9adcmt0gjakvwt33stxa48tjgcjyrd6c3hdz0gjw9j3h2d680\"]"
		      }
		    }
		  }
		}
		"#);
	};
	test_checkout(directory, dependencies, assertions).await;
}

async fn test_checkout<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	dependencies: bool,
	assertions: F,
) where
	F: FnOnce(temp::Artifact) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = artifact.into();
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.as_ref()).await.unwrap();

		// Build the module.
		let output = server
			.tg()
			.arg("build")
			.arg(artifact_temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let id = std::str::from_utf8(&output.stdout).unwrap().trim();
		let temp = Temp::new();
		let path = temp.path().to_owned();

		// Check out the artifact.
		let mut command = server.tg();
		command.arg("checkout").arg(id).arg(path);
		command.arg(format!("--dependencies={dependencies}"));
		let output = command.output().await.unwrap();
		assert_success!(output);

		let artifact = temp::Artifact::with_path(temp.path()).await.unwrap();
		assertions(artifact).await;
	})
	.await;
}
