use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use tangram_cli_test::{Server, assert_success};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test bundling a file with no dependencies.
#[tokio::test]
async fn file_no_dependencies() {
	let artifact = temp::file!("hello!").into();
	let output = test(artifact).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"
	tg.file({
	  "contents": tg.blob("hello!"),
	})
	"#);
}

/// Test bundling a file with no dependencies.
#[tokio::test]
async fn file_no_dependencies_js() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let file = await tg.file("hello");
				return tg.bundle(file);
			};
		"#),
	}
	.into();
	let output = test_js(artifact).await;
	assert_json_snapshot!(output, @r#"
	{
	  "kind": "file",
	  "contents": "hello"
	}
	"#);
}

// /// Test bundling a directory that contains no files with dependencies
#[tokio::test]
async fn directory_no_dependencies() {
	let artifact = temp::directory! {
		"file" => temp::file!("hello"),
		"link" => temp::symlink!("link")
	}
	.into();
	let output = test(artifact).await;
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"
	tg.directory({
	  "file": tg.file({
	    "contents": tg.blob("hello"),
	  }),
	  "link": tg.symlink({
	    "path": "link",
	  }),
	})
	"#);
}

/// Test bundling a directory that contains files with dependencies.
#[tokio::test]
async fn directory_containing_file_with_file_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
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
			};
		"#),
	}
	.into();
	let output = test_js(artifact).await;
	assert_json_snapshot!(output, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    ".tangram": {
	      "kind": "directory",
	      "entries": {
	        "artifacts": {
	          "kind": "directory",
	          "entries": {
	            "fil_01veqdpfrvtngmf1vqfq1r602m0hwvey6a3xhg42xpknpybvfk3020": {
	              "kind": "file",
	              "contents": "dependency"
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
}

/// Test bundling dependencies that contain target symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_target_symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
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
			};
		"#),
	}
	.into();
	let output = test_js(artifact).await;
	assert_json_snapshot!(output, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    ".tangram": {
	      "kind": "directory",
	      "entries": {
	        "artifacts": {
	          "kind": "directory",
	          "entries": {
	            "dir_01em9s61ngmg0sbxebh8z0ajbe45rmcbbd3p2ss6yc8cpamsy5a3bg": {
	              "kind": "directory",
	              "entries": {
	                "dep": {
	                  "kind": "file",
	                  "contents": "dependency"
	                },
	                "link": {
	                  "kind": "symlink",
	                  "path": "dep"
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
	      "path": "file"
	    }
	  }
	}
	"#);
}

/// Test bundling dependencies that contain artifact/path symlinks.
#[tokio::test]
async fn directory_containing_file_with_directory_dependency_artifact_path_symlink() {
	let output = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let inner_dir = tg.directory({
					"a": tg.file("a"),
				}); 
				let dependency = tg.directory({
					"dep": tg.file("dependency"),
					"link": tg.symlink({
						artifact: inner_dir,
						path: "a"
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
			};
		"#),
	}
	.into();
	let output = test_js(output).await;
	assert_json_snapshot!(output, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    ".tangram": {
	      "kind": "directory",
	      "entries": {
	        "artifacts": {
	          "kind": "directory",
	          "entries": {
	            "dir_01m96hv5dzrzyjr66ktvxxr3shkj2vhn5h612zqz5vbabq83fpa6mg": {
	              "kind": "directory",
	              "entries": {
	                "dep": {
	                  "kind": "file",
	                  "contents": "dependency"
	                },
	                "link": {
	                  "kind": "symlink",
	                  "path": "../../../.tangram/artifacts/dir_01myaw4t7h2nw6mmr7jgm0acznf0bg070h4amd79x6n7n7m3xp7eh0/a"
	                }
	              }
	            },
	            "dir_01myaw4t7h2nw6mmr7jgm0acznf0bg070h4amd79x6n7n7m3xp7eh0": {
	              "kind": "directory",
	              "entries": {
	                "a": {
	                  "kind": "file",
	                  "contents": "a"
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
	      "path": "file"
	    }
	  }
	}
	"#);
}

async fn test(artifact: temp::Artifact) -> std::process::Output {
	let server = Server::new(TG).await.unwrap();

	// Write the artifact to a temp.
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();
	let path = temp.path();

	// Check in.
	let output = server.tg().arg("checkin").arg(path).output().await.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Get the object.
	let output = server
		.tg()
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
	server
		.tg()
		.arg("object")
		.arg("get")
		.arg(id.clone())
		.arg("--depth=inf")
		.arg("--format=tgon")
		.arg("--pretty=true")
		.output()
		.await
		.unwrap()
}

async fn test_js(artifact: temp::Artifact) -> temp::Artifact {
	let server = Server::new(TG).await.unwrap();

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

	temp::Artifact::with_path(temp.path()).await.unwrap()
}
