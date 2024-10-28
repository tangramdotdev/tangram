use crate::{Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_temp::{self as temp, artifact, symlink, Temp};

#[tokio::test]
async fn cyclic_dependencies() -> tg::Result<()> {
	test(
		artifact!({
			"directory": {
				"foo": {
					"tangram.ts": r#"import * as bar from "../bar""#,
				},
				"bar": {
					"tangram.ts": r#"import * as foo from "../foo""#,
				},
			},
		}),
		"directory/foo",
		|_, _, output| async move {
			eprintln!("{output}");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		artifact!({
			"directory": {
				"hello.txt": "Hello, world!",
				"link": symlink!("hello.txt"),
				"subdirectory": {
					"sublink": symlink!("../link"),
				}
			}
		}),
		"directory",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
		tg.directory({
			"graph": tg.graph({
				"nodes": [
					{
						"kind": "symlink",
						"artifact": 3,
						"subpath": "hello.txt",
					},
					{
						"kind": "symlink",
						"artifact": 3,
						"subpath": "link",
					},
					{
						"kind": "directory",
						"entries": {
							"sublink": 1,
						},
					},
					{
						"kind": "directory",
						"entries": {
							"hello.txt": tg.file({
								"contents": tg.leaf("Hello, world!"),
							}),
							"link": 0,
							"subdirectory": 2,
						},
					},
				],
			}),
			"node": 3,
		})
		"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn readme() -> tg::Result<()> {
	test(
		artifact!({
			"directory": {
				"README.md": "Hello, World!!",
			}
		}),
		"directory",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"README.md": tg.file({
   		"contents": tg.leaf("Hello, World!!"),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn default_module() -> tg::Result<()> {
	test(
		artifact!({
			"directory": {
				"tangram.ts": "export default tg.target(() => {})",
			}
		}),
		"directory",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("export default tg.target(() => {})"),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(artifact: temp::Artifact, path: &str, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Artifact, String) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact
			.to_path(directory.as_ref())
			.await
			.map_err(
				|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
			)
			.inspect_err(|error| {
				eprintln!("path: {}", directory.path().display());
				eprintln!("{error}");
			})?;
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: directory.as_ref().join(path),
		};
		let artifact = tg::Artifact::check_in(&server, arg).await?;
		let object = tg::Object::from(artifact.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(artifact.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		(assertions)(server.clone(), artifact, output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
