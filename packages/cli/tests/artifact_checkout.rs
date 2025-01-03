use indoc::indoc;
use insta::assert_json_snapshot;
use std::future::Future;
use tangram_cli::{assert_output_success, test::test};
use tangram_client::{self as tg};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
		export default tg.target(() => {
			return tg.directory({
				"hello.txt": "Hello, World!",
			})
		})
	"#),
	};
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
		Ok(())
	};
	let checkout_dependencies = None;
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.file("Hello, World!") 
			})
		"#),
	};
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
	"kind": "file",
	"contents": "Hello, World!",
	"executable": false
  }
  "#);
		Ok(())
	};
	let checkout_dependencies = None;
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

#[tokio::test]
async fn executable_file() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.file({
					contents: "Hello, World!",
					executable: true,
				})
			})
		"#),
	};
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
	"kind": "file",
	"contents": "Hello, World!",
	"executable": true
  }
  "#);
		Ok(())
	};
	let checkout_dependencies = None;
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

#[tokio::test]
async fn file_with_dependency() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
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
	let checkout_dependencies = Some(false);
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link": tg.symlink("hello.txt")
				})
			})
		"#),
	};
	let checkout_dependencies = None;
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a directory with a symlink.
#[tokio::test]
async fn symlink_shared_target() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"link1": tg.symlink("hello.txt"),
					"link2": tg.symlink("hello.txt")
				})
			})
		"#),
	};
	let checkout_dependencies = None;
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a very deep directory.
#[ignore]
#[tokio::test]
async fn deeply_nested_directory() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				let artifact = tg.file("hello");
				for (let i = 0; i < 10; i++) {
					let entries = { "a": artifact };
					artifact = tg.directory(entries);
				}
				return artifact;
			})
		"#),
	};
	let checkout_dependencies = None;
	let assertions = |_artifact: temp::Artifact| async move { Ok(()) };
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
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
	let checkout_dependencies = None;
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				return tg.directory({
					"foo": tg.symlink({artifact: tg.file("bar")})
				})
			})
		"#),
	};
	let checkout_dependencies = None;
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_directory() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
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
	let checkout_dependencies = None;
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
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a file that is a member of a graph.
#[tokio::test]
async fn graph_file() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
				let graph = tg.graph({
					nodes: [{
						kind: "file", 
						contents: "Hello, World!", 
						executable: false,
					}],
				});
				return tg.file({ graph: graph, node: 0 });  
			});
		"#),
	};
	let checkout_dependencies = None;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#""#);
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
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
	let checkout_dependencies = None;
	let assertions = |artifact: temp::Artifact| async move {
		assert_json_snapshot!(artifact, @r#"
  {
	"kind": "symlink",
	"target": "/bin/sh"
  }
  "#);
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

/// Test checking out a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() -> tg::Result<()> {
	let build = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => {
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
	let checkout_dependencies = None;
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
		"contents": "{\n  \"nodes\": [\n    {\n      \"kind\": \"directory\",\n      \"entries\": {\n        \"link\": 1\n      },\n      \"id\": \"dir_01jgpeycbs5s4yjr89jqf3kkvy1a0rmrk7j2fmedscvh495h5b3740\"\n    },\n    {\n      \"kind\": \"symlink\",\n      \"Artifact\": {\n        \"id\": \"sym_01qzd2gdre0bz3ck3q00yw16g36eg6sq9jrvzv5w3m5191ynjm3nq0\",\n        \"artifact\": 0,\n        \"subpath\": \"link\"\n      }\n    }\n  ]\n}",
		"executable": false
	  }
	}
  }
  "#);
		Ok(())
	};
	test_artifact_checkout(build, checkout_dependencies, assertions).await
}

async fn test_artifact_checkout<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	checkout_dependencies: Option<bool>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(temp::Artifact) -> Fut + Send + 'static,
	Fut: Future<Output = tg::Result<()>> + Send,
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
			.arg("--quiet")
			.arg(artifact_temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);

		let id = std::str::from_utf8(&output.stdout).unwrap().trim();

		let temp = Temp::new();
		let path = temp.path().to_owned();

		// Check out the artifact.
		let mut command = server.tg();
		command.arg("checkout").arg(id).arg(path);
		if let Some(checkout_dependencies) = checkout_dependencies {
			command
				.arg("--dependencies")
				.arg(checkout_dependencies.to_string());
		}
		let output = command.output().await.unwrap();
		assert_output_success!(output);

		let artifact = temp::Artifact::with_path(temp.path()).await.unwrap();

		assertions(artifact).await.unwrap();
	})
	.await;
	Ok(())
}
