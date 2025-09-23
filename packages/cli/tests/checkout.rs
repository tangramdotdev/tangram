use {
	indoc::indoc,
	insta::assert_json_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
				})
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
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
}

#[tokio::test]
async fn file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("Hello, World!") 
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "file",
	  "contents": "Hello, World!"
	}
	"#);
}

#[tokio::test]
async fn executable_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file({
					contents: "Hello, World!",
					executable: true,
				})
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "file",
	  "contents": "Hello, World!",
	  "executable": true
	}
	"#);
}

#[tokio::test]
async fn file_with_tag_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let bar = await tg.file("bar");
				return tg.file({
					contents: "foo",
					dependencies: {
						"bar": {
							item: bar,
							options: {
								id: bar.id,
								tag: "bar"
							}
						} 
					}
				})
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "file",
	  "contents": "foo",
	  "xattrs": {
	    "user.tangram.dependencies": "[\"bar\"]",
	    "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"bar\":{\"item\":{\"node\":1},\"options\":{\"id\":\"fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg\",\"tag\":\"bar\"}}}},{\"kind\":\"file\"}]}"
	  }
	}
	"#);
}

#[tokio::test]
async fn file_with_id_dependency() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "file",
	  "contents": "foo",
	  "xattrs": {
	    "user.tangram.dependencies": "[\"fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg\"]"
	  }
	}
	"#);
}

#[tokio::test]
async fn symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link": tg.symlink("hello.txt")
				})
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
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
}

/// Test checking out a directory with a symlink.
#[tokio::test]
async fn symlink_shared_target() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link1": tg.symlink("hello.txt"),
					"link2": tg.symlink("hello.txt")
				})
			}
		"#),
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
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
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let bar = await tg.file("bar");
				return tg.directory({
					"foo": tg.file({
						contents: "foo",
						dependencies: {
							"bar": {
								item: bar,
								options: {
									id: bar.id,
									tag: "bar"
								}
							}
						}
					}) 
				})
			}
		"#),
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
	            "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
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
	      "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"foo\": {\n          \"node\": 1\n        }\n      }\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"bar\": {\n          \"item\": {\n            \"node\": 2\n          },\n          \"options\": {\n            \"id\": \"fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg\",\n            \"tag\": \"bar\"\n          }\n        }\n      }\n    },\n    {\n      \"kind\": \"file\"\n    }\n  ]\n}"
	    }
	  }
	}
	"#);
	let lock = artifact
		.unwrap_directory_ref()
		.entries
		.get("tangram.lock")
		.unwrap()
		.unwrap_file_ref();
	let lock = serde_json::from_str::<serde_json::Value>(&lock.contents).unwrap();
	assert_json_snapshot!(lock, @r#"
	{
	  "nodes": [
	    {
	      "entries": {
	        "foo": {
	          "node": 1
	        }
	      },
	      "kind": "directory"
	    },
	    {
	      "dependencies": {
	        "bar": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg",
	            "tag": "bar"
	          }
	        }
	      },
	      "kind": "file"
	    },
	    {
	      "kind": "file"
	    }
	  ]
	}
	"#);
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_id_dependency() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
	            "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
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
	        "user.tangram.dependencies": "[\"fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg\"]"
	      }
	    }
	  }
	}
	"#);
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_id_dependency_with_tag_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let dependency = await tg.file("bar", { dependencies: { baz: tg.file("baz") } });
				return tg.directory({
					"foo": tg.file({
						contents: "foo", 
						dependencies: {
							[dependency.id]: {
								item: dependency,
								options: {
									id: dependency.id
								}
							},
						},
					}) 
				})
			}
		"#),
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
	            "fil_011hdjrsfbx524jjqfarmgrak6devcgr77b54957vma8w76p2dnxj0": {
	              "kind": "file",
	              "contents": "bar",
	              "xattrs": {
	                "user.tangram.dependencies": "[\"baz\"]"
	              }
	            },
	            "fil_015yhgqg8rtz851kymkh3ge927e1vyjekbq96yavz8wb63w3cr92x0": {
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
	        "user.tangram.dependencies": "[\"fil_011hdjrsfbx524jjqfarmgrak6devcgr77b54957vma8w76p2dnxj0\"]"
	      }
	    }
	  }
	}
	"#);
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"foo": tg.symlink({artifact: tg.file("bar")})
				})
			}
		"#),
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
	            "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
	              "kind": "file",
	              "contents": "bar"
	            }
	          }
	        }
	      }
	    },
	    "foo": {
	      "kind": "symlink",
	      "path": ".tangram/artifacts/fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg"
	    }
	  }
	}
	"#);
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
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
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "file",
	  "contents": "Hello, World!"
	}
	"#);
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = false;
	let artifact = test(artifact, dependencies).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "symlink",
	  "path": "/bin/sh"
	}
	"#);
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
}

#[tokio::test]
async fn shared_dependency_on_symlink() {
	let artifact = temp::directory! {
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
	}
	.into();
	let dependencies = true;
	let artifact = test(artifact, dependencies).await;
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
	            "dir_019rby79qjde25dajqt2wt86ga8ryp5e2zbyddhqfgp2vd0ktmgvc0": {
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
	        "user.tangram.dependencies": "[\"dir_019rby79qjde25dajqt2wt86ga8ryp5e2zbyddhqfgp2vd0ktmgvc0\"]"
	      }
	    },
	    "foo.txt": {
	      "kind": "file",
	      "contents": "foo",
	      "xattrs": {
	        "user.tangram.dependencies": "[\"dir_019rby79qjde25dajqt2wt86ga8ryp5e2zbyddhqfgp2vd0ktmgvc0\"]"
	      }
	    }
	  }
	}
	"#);
}

async fn test(artifact: temp::Artifact, dependencies: bool) -> temp::Artifact {
	let server = Server::new(TG).await.unwrap();

	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();

	// Build.
	let output = server
		.tg()
		.arg("build")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Checkout.
	let temp = Temp::new();
	let mut command = server.tg();
	command.arg("checkout").arg(id).arg(temp.path());
	command.arg(format!("--dependencies={dependencies}"));
	let output = command.output().await.unwrap();
	assert_success!(output);

	temp::Artifact::with_path(temp.path()).await.unwrap()
}
