use indoc::indoc;
use insta::assert_json_snapshot;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
		export default tg.command(() => {
			return tg.directory({
				"hello.txt": "Hello, World!",
			})
		})
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
        "contents": "Hello, World!",
        "executable": false
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.file("Hello, World!") 
			})
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": false
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn executable_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.file({
					contents: "Hello, World!",
					executable: true,
				})
			})
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
	test_artifact_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn file_with_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.file({
					contents: "foo",
					dependencies: {
						"bar": {
							item: tg.file("bar")
						} 
					}
				})
			})
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "foo",
    "executable": false,
    "xattrs": {
      "user.tangram.lock": "{\"nodes\":[{\"kind\":\"file\",\"dependencies\":{\"bar\":{\"item\":\"fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80\"}},\"id\":\"fil_01tsgfzwa97w008amycfw2zbywvj56hac3164dgqp9qj1we854rkg0\"}]}"
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link": tg.symlink("hello.txt")
				})
			})
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
        "contents": "Hello, World!",
        "executable": false
      },
      "link": {
        "kind": "symlink",
        "target": "hello.txt"
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a symlink.
#[tokio::test]
async fn symlink_shared_target() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link1": tg.symlink("hello.txt"),
					"link2": tg.symlink("hello.txt")
				})
			})
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
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a very deep directory.
#[ignore]
#[tokio::test]
async fn deeply_nested_directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				let artifact = tg.file("hello");
				for (let i = 0; i < 10; i++) {
					let entries = { "a": artifact };
					artifact = tg.directory(entries);
				}
				return artifact;
			})
		"#),
	};
	let dependencies = false;
	let assertions = |_artifact: temp::Artifact| async move {};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
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
			})
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
        "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"foo\": 1\n      },\n      \"id\": \"dir_019at517ytj7gfg92358f53w7c2ht4gewvtg45fegwr6teffnnvxa0\"\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"bar\": {\n          \"item\": \"fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80\"\n        }\n      },\n      \"id\": \"fil_01tsgfzwa97w008amycfw2zbywvj56hac3164dgqp9qj1we854rkg0\"\n    }\n  ]\n}",
        "executable": false
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				return tg.directory({
					"foo": tg.symlink({artifact: tg.file("bar")})
				})
			})
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
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "directory",
							entries: { "hello.txt": tg.file("Hello, World!") },
						},
					],
				});
				return tg.directory({ graph: graph, node: 0 });
			})
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
        "contents": "Hello, World!",
        "executable": false
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				let graph = tg.graph({
					nodes: [{
						kind: "file", 
						contents: "Hello, World!", 
						executable: false,
					}],
				});
				return tg.file({ graph, node: 0 });  
			});
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "Hello, World!",
    "executable": false
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				let graph = tg.graph({
					nodes: [{
						kind: "symlink", 
						target:"/bin/sh",
					}],
				});
				return tg.symlink({ graph: graph, node: 0 });  
			})
		"#),
	};
	let dependencies = false;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "symlink",
    "target": "/bin/sh"
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(() => {
				let graph =  tg.graph({
					nodes: [{
							kind: "directory", 
							entries: {"link": 1}
						},
						{
							kind: "symlink", 
							artifact: 0, 
							subpath: "link"
						},
					],
				});
				return tg.directory({ graph: graph, node: 0 });
			})
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
		      "target": "link"
		    },
		    "tangram.lock": {
		      "kind": "file",
		      "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"link\": 1\n      },\n      \"id\": \"dir_014yyvsnfgj1dsd3s7dctta79hmjm3rq6sya1t7hymygjm97ynqhng\"\n    },\n    {\n      \"kind\": \"symlink\",\n      \"id\": \"sym_01gs8v0w26ks7573pm013qytg2p82fvhyzcwg6hnncjb1gx0077060\",\n      \"artifact\": 0,\n      \"subpath\": \"link\"\n    }\n  ]\n}",
		      "executable": false
		    }
		  }
		}
		"#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

#[tokio::test]
async fn shared_dependency_on_symlink() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.command(async () => {
				let depDir = await tg.directory({
					"file.txt": "contents",
					"link": tg.symlink("file.txt"),
				});
				let depDirId = await depDir.id();
				return tg.directory({
					"foo.txt": tg.file("foo", { dependencies: { depDirId: { item: depDir }}}),
					"bar.txt": tg.file("bar", { dependencies: { depDirId: { item: depDir }}})
				})
			})
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
              "dir_01014gqjpa2f7pd7kwzrnxqak9f933agwkfxmky2nk44kad98jenbg": {
                "kind": "directory",
                "entries": {
                  "file.txt": {
                    "kind": "file",
                    "contents": "contents",
                    "executable": false
                  },
                  "link": {
                    "kind": "symlink",
                    "target": "file.txt"
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
        "executable": false
      },
      "foo.txt": {
        "kind": "file",
        "contents": "foo",
        "executable": false
      },
      "tangram.lock": {
        "kind": "file",
        "contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"bar.txt\": 1,\n        \"foo.txt\": 2\n      },\n      \"id\": \"dir_01t4mg404e196gzdgbr212nt0f6c3t18h5nb1t24682xrh9wtev5rg\"\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"depDirId\": {\n          \"item\": \"dir_01014gqjpa2f7pd7kwzrnxqak9f933agwkfxmky2nk44kad98jenbg\"\n        }\n      },\n      \"id\": \"fil_019bx1epya1wzdnb94n3aj2enhhm5cf1j891gv0m6te01j7bfbdn9g\"\n    },\n    {\n      \"kind\": \"file\",\n      \"dependencies\": {\n        \"depDirId\": {\n          \"item\": \"dir_01014gqjpa2f7pd7kwzrnxqak9f933agwkfxmky2nk44kad98jenbg\"\n        }\n      },\n      \"id\": \"fil_01pd3m89913g0wkfdre0xhmj2mrth7tdfjtyr813d2v2dxvg9e2q9g\"\n    }\n  ]\n}",
        "executable": false
      }
    }
  }
  "#);
	};
	test_artifact_checkout(directory, dependencies, assertions).await;
}

async fn test_artifact_checkout<F, Fut>(
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
