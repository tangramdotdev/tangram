use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::future::Future;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test bundling a file with no dependencies.
#[tokio::test]
async fn file_no_dependencies_js() {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				let file = await tg.file("hello");
				return tg.bundle(file);
			});
		"#),
	};
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
    "kind": "file",
    "contents": "hello",
    "executable": false
  }
  "#);
	};
	test_bundle_js(build, assertions).await;
}

/// Test bundling a file with no dependencies.
#[tokio::test]
async fn file_no_dependencies() {
	let file = temp::file!("hello!");
	let assertions = |object: String| async move {
		assert_snapshot!(object, @r#"
  tg.file({
    "contents": tg.leaf("hello!"),
  })
  "#);
	};
	test_bundle(file, assertions).await;
}

// /// Test bundling a directory that contains no files with dependencies
#[tokio::test]
async fn directory_no_dependencies() {
	let directory = temp::directory! {
		"file" => temp::file!("hello"),
		"link" => temp::symlink!("link")
	};
	let assertions = |output: String| async move {
		assert_snapshot!(output, @r#"
  tg.directory({
    "file": tg.file({
      "contents": tg.leaf("hello"),
    }),
    "link": tg.symlink({
      "target": "link",
    }),
  })
  "#);
	};
	test_bundle(directory, assertions).await;
}

/// Test bundling an executable file with a dependency.
#[tokio::test]
async fn executable_file_with_dependency() {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				let dependency = tg.file("dependency");
				let file = await tg.file({
					contents: "f", 
					dependencies: {
						"dependency": {
							item: dependency
						}, 
					},
					executable: true
				});
				return tg.bundle(file);
			});
		"#),
	};
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
              "fil_01gkrw51xnwqmtdqg7eww1yzcgvwjber106q9j96z94zdgkr49073g": {
                "kind": "file",
                "contents": "dependency",
                "executable": false
              }
            }
          },
          "run": {
            "kind": "file",
            "contents": "f",
            "executable": true
          }
        }
      }
    }
  }
  "#);
	};
	test_bundle_js(build, assertions).await;
}

/// Test bundling a directory that contains files with dependencies.  #[tokio::test]
#[tokio::test]
async fn directory_containing_file_with_file_dependency() {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				let dependency = tg.file("dependency");
				let file = await tg.file({
					contents: "f", 
					dependencies: {
						"dependency": {
							item: dependency,
						}, 
					},
					executable: true
				});
				let dir = tg.directory({
					"file": file
				});
				return tg.bundle(dir);
			});
		"#),
	};
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
              "fil_01gkrw51xnwqmtdqg7eww1yzcgvwjber106q9j96z94zdgkr49073g": {
                "kind": "file",
                "contents": "dependency",
                "executable": false
              }
            }
          }
        }
      },
      "file": {
        "kind": "file",
        "contents": "f",
        "executable": true
      }
    }
  }
  "#);
	};
	test_bundle_js(build, assertions).await;
}

/// Test bundling dependencies that contain target symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_target_symlink() {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				let dependency = tg.directory({
					"dep": tg.file("dependency"),
					"link": tg.symlink("dep"),
				});
				let file = await tg.file({
					contents: "f", 
					dependencies: {
						"dependency": {
							item: dependency,
						}, 
					},
					executable: true,
				});
				let dir = tg.directory({
					"file": file,
					"link": tg.symlink("file"),
				});
				return tg.bundle(dir);
			});
		"#),
	};
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
              "dir_01neb60wpaemmf2p87vzyvbs1r4c06a664fek83685stzycqp4t9z0": {
                "kind": "directory",
                "entries": {
                  "dep": {
                    "kind": "file",
                    "contents": "dependency",
                    "executable": false
                  },
                  "link": {
                    "kind": "symlink",
                    "target": "dep"
                  }
                }
              }
            }
          }
        }
      },
      "file": {
        "kind": "file",
        "contents": "f",
        "executable": true
      },
      "link": {
        "kind": "symlink",
        "target": "file"
      }
    }
  }
  "#);
	};
	test_bundle_js(build, assertions).await;
}

/// Test bundling dependencies that contain artifact/path symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_artifact_path_symlink() {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				let inner_dir = tg.directory({
					"a": tg.file("a"),
				}); 
				let dependency = tg.directory({
					"dep": tg.file("dependency"),
					"link": tg.symlink({
						artifact: inner_dir,
						subpath: "a"
					}),
				});
				let file = await tg.file({
					contents: "f", 
					dependencies: {
						"dependency": {
							item: dependency
						}, 
					},
					executable: true,
				});
				let dir = tg.directory({
					"file": file,
					"link": tg.symlink("file")
				});
				return tg.bundle(dir);
			});
		"#),
	};
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
              "dir_010tcemg45skky72hzj6brbq9pqdm6anjc3y1n1367faavg0wvb5cg": {
                "kind": "directory",
                "entries": {
                  "dep": {
                    "kind": "file",
                    "contents": "dependency",
                    "executable": false
                  },
                  "link": {
                    "kind": "symlink",
                    "target": "../../../.tangram/artifacts/dir_015hd3d0r0njfxz43hby36168cdz80tbc7wgpnp105bmray9wv7wng/a"
                  }
                }
              },
              "dir_015hd3d0r0njfxz43hby36168cdz80tbc7wgpnp105bmray9wv7wng": {
                "kind": "directory",
                "entries": {
                  "a": {
                    "kind": "file",
                    "contents": "a",
                    "executable": false
                  }
                }
              }
            }
          }
        }
      },
      "file": {
        "kind": "file",
        "contents": "f",
        "executable": true
      },
      "link": {
        "kind": "symlink",
        "target": "file"
      }
    }
  }
  "#);
	};
	test_bundle_js(build, assertions).await;
}

async fn test_bundle<F, Fut>(artifact: impl Into<temp::Artifact> + Send + 'static, assertions: F)
where
	F: FnOnce(String) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();

		// Write the artifact to a temp.
		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let path = temp.path();

		// Check in.
		let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
		assert_success!(output);

		// Get the object.
		let id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();
		let output = server
			.tg()
			.arg("artifact")
			.arg("bundle")
			.arg(id.clone())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Get the object.
		let id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();
		let object_output = server
			.tg()
			.arg("object")
			.arg("get")
			.arg(id.clone())
			.arg("--format")
			.arg("tgvn")
			.arg("--pretty")
			.arg("true")
			.arg("--recursive")
			.output()
			.await
			.unwrap();
		assert_success!(object_output);
		let object_output = std::str::from_utf8(&object_output.stdout)
			.unwrap()
			.to_owned();

		assertions(object_output).await;
	})
	.await;
}

async fn test_bundle_js<F, Fut>(artifact: impl Into<temp::Artifact> + Send + 'static, assertions: F)
where
	F: FnOnce(temp::Artifact) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
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
		let output = server
			.tg()
			.arg("checkout")
			.arg(id)
			.arg(path)
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let artifact = temp::Artifact::with_path(temp.path()).await.unwrap();

		assertions(artifact).await;
	})
	.await;
}
