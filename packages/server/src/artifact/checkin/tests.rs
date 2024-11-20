use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_snapshot;
use std::{panic::AssertUnwindSafe, path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn file_through_symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"a" => temp::directory! {
				"tangram.ts" => r#"import "../b/c/d"#,
			},
			"b" => temp::directory! {
				"c" => temp::symlink!("e"),
				"e" => temp::directory! {
					"d" => "hello, world!"
				}
			}
		},
		"a",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import "../b/c/d"),
   		"dependencies": {
   			"../b/c/d": {
   				"item": tg.file({
   					"contents": tg.leaf("hello, world!"),
   				}),
   				"path": "../b/e/d",
   			},
   		},
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn external_symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"a" => temp::directory! {
				"tangram.ts" => r#"import "../b/c"#,
			},
			"b" => temp::directory! {
				"c" => temp::symlink!("e"),
				"e" => temp::directory! {
					"d" => "hello, world!"
				}
			}
		},
		"a",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import "../b/c"),
   		"dependencies": {
   			"../b/c": {
   				"item": tg.symlink({
   					"subpath": "e",
   				}),
   				"path": "../b/c",
   			},
   		},
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn simple_path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar";"#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			},
		},
		"foo",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import * as bar from "../bar";"),
   		"dependencies": {
   			"../bar": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   				}),
   				"path": "../bar",
   				"subpath": "tangram.ts",
   			},
   		},
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_with_ndested_dependencies() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import * as bar from "./bar";
					import * as baz from "./baz";
				"#,
				"bar" => temp::directory! {
					"tangram.ts" => r#"
						import * as baz from "../baz";
					"#,
				},
				"baz" => temp::directory! {
					"tangram.ts" => "",
				}
			},
		},
		"foo",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"bar": tg.directory({
   		"tangram.ts": tg.file({
   			"contents": tg.leaf("
   						import * as baz from "../baz";
   					"),
   			"dependencies": {
   				"../baz": {
   					"item": tg.directory({
   						"tangram.ts": tg.file({
   							"contents": tg.leaf(""),
   						}),
   					}),
   					"path": "baz",
   					"subpath": "tangram.ts",
   				},
   			},
   		}),
   	}),
   	"baz": tg.directory({
   		"tangram.ts": tg.file({
   			"contents": tg.leaf(""),
   		}),
   	}),
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("
   					import * as bar from "./bar";
   					import * as baz from "./baz";
   				"),
   		"dependencies": {
   			"./bar": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf("
   						import * as baz from "../baz";
   					"),
   						"dependencies": {
   							"../baz": {
   								"item": tg.directory({
   									"tangram.ts": tg.file({
   										"contents": tg.leaf(""),
   									}),
   								}),
   								"path": "baz",
   								"subpath": "tangram.ts",
   							},
   						},
   					}),
   				}),
   				"path": "bar",
   				"subpath": "tangram.ts",
   			},
   			"./baz": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   				}),
   				"path": "baz",
   				"subpath": "tangram.ts",
   			},
   		},
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_with_cyclic_modules() -> tg::Result<()> {
	test(
		temp::directory! {
			"package" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "./foo.tg.ts";"#,
				"foo.tg.ts" => r#"import * as root from "./tangram.ts";"#,
			}
		},
		"package",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"foo.tg.ts": 2,
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as foo from "./foo.tg.ts";"),
   				"dependencies": {
   					"./foo.tg.ts": {
   						"item": 0,
   						"path": "",
   						"subpath": "foo.tg.ts",
   					},
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as root from "./tangram.ts";"),
   				"dependencies": {
   					"./tangram.ts": {
   						"item": 0,
   						"path": "",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory_with_nested_packages() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => "",
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			}
		},
		"",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"bar": tg.directory({
   		"tangram.ts": tg.file({
   			"contents": tg.leaf(""),
   		}),
   	}),
   	"foo": tg.directory({
   		"tangram.ts": tg.file({
   			"contents": tg.leaf(""),
   		}),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"link" => temp::symlink!("."),
			}
		},
		"directory",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"link": tg.symlink({
   		"subpath": ".",
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn cyclic_dependencies() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"foo" => temp::directory! {
					"tangram.ts" => r#"import * as bar from "../bar""#,
				},
				"bar" => temp::directory! {
					"tangram.ts" => r#"import * as foo from "../foo""#,
				},
			},
		},
		"directory/foo",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as bar from "../bar""),
   				"dependencies": {
   					"../bar": {
   						"item": 2,
   						"path": "../bar",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   			{
   				"kind": "directory",
   				"entries": {
   					"tangram.ts": 3,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as foo from "../foo""),
   				"dependencies": {
   					"../foo": {
   						"item": 0,
   						"path": "",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"hello.txt" => "Hello, world!",
				"link" => temp::symlink!("hello.txt"),
				"subdirectory" => temp::directory! {
					"sublink" => temp::symlink!("../link"),
				}
			}
		},
		"directory",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"hello.txt": tg.file({
   		"contents": tg.leaf("Hello, world!"),
   	}),
   	"link": tg.symlink({
   		"subpath": "hello.txt",
   	}),
   	"subdirectory": tg.directory({
   		"sublink": tg.symlink({
   			"subpath": "../link",
   		}),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"README.md" => "Hello, World!",
			}
		},
		"directory",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"README.md": tg.file({
   		"contents": tg.leaf("Hello, World!"),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"tangram.ts" => "export default tg.target(() => {})",
			}
		},
		"directory",
		false,
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

#[tokio::test]
async fn import_from_parent() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"baz" => temp::directory! {
					"mod.tg.ts" => r#"import * as baz from "..";"#
				},
				"foo" => temp::directory!{},
				"tangram.ts" => r#"import patches from "./foo" with { type: "directory" };"#,
			}
		},
		"directory",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"baz": 1,
   					"foo": tg.directory({}),
   					"tangram.ts": tg.file({
   						"contents": tg.leaf("import patches from "./foo" with { type: "directory" };"),
   						"dependencies": {
   							"./foo": {
   								"item": tg.directory({}),
   								"path": "foo",
   							},
   						},
   					}),
   				},
   			},
   			{
   				"kind": "directory",
   				"entries": {
   					"mod.tg.ts": 2,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as baz from "..";"),
   				"dependencies": {
   					"..": {
   						"item": 0,
   						"path": "",
   						"subpath": "tangram.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory_destructive() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"a" => temp::directory! {
					"b" => temp::directory! {
						"c" => temp::symlink!("../../a/d/e")
					},
					"d" => temp::directory! {
						"e" => temp::symlink!("../../a/f/g"),
					},
					"f" => temp::directory! {
						"g" => ""
					}
				},
			},
		},
		"directory",
		true,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"a": tg.directory({
   		"b": tg.directory({
   			"c": tg.symlink({
   				"subpath": "../../a/d/e",
   			}),
   		}),
   		"d": tg.directory({
   			"e": tg.symlink({
   				"subpath": "../../a/f/g",
   			}),
   		}),
   		"f": tg.directory({
   			"g": tg.file({
   				"contents": tg.leaf(""),
   			}),
   		}),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn destructive_package() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" => r#"import * as a from "./a.tg.ts"#,
			"a.tg.ts" => "",
		},
		"",
		true,
		|_server, _artifact, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"graph": tg.graph({
   		"nodes": [
   			{
   				"kind": "directory",
   				"entries": {
   					"a.tg.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import * as a from "./a.tg.ts"),
   				"dependencies": {
   					"./a.tg.ts": {
   						"item": 0,
   						"path": "",
   						"subpath": "a.tg.ts",
   					},
   				},
   			},
   		],
   	}),
   	"node": 0,
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn external_symlink_roundtrip() -> tg::Result<()> {
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;
	let result = AssertUnwindSafe(async {
		let directory = tg::directory! {
			"file" => "contents",
		}
		.into();
		let symlink = tg::symlink!(Some(directory), Some(PathBuf::from("file")));

		// Check out the artifact.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(temp.path().to_owned()),
		};
		let path = tg::Artifact::from(symlink.clone())
			.check_out(&server, arg)
			.await?;

		// Check the artifact back in.
		let arg = tg::artifact::checkin::Arg {
			deterministic: true,
			destructive: false,
			ignore: true,
			locked: true,
			path,
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_symlink()
			.map_err(|_| tg::error!("expected a symlink"))?;
		assert_eq!(artifact.id(&server).await?, symlink.id(&server).await?);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn ignore() -> tg::Result<()> {
	test(
		temp::directory! {
			".DS_Store" => temp::file!(""),
			".git" => temp::directory! {
				"config" => temp::file!(""),
			},
			".tangram" => temp::directory! {
				"config" => temp::file!(""),
			},
			"tangram.lock" => temp::file!(r#"{"nodes":[]}"#),
			"tangram.ts" => temp::file!(""),
		},
		"",
		false,
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf(""),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(
	artifact: temp::Artifact,
	path: &str,
	destructive: bool,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Artifact, String) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
		let arg = tg::artifact::checkin::Arg {
			destructive,
			deterministic: false,
			ignore: true,
			locked: false,
			path: directory.as_ref().join(path),
		};
		let stream = server.check_in_artifact(arg).await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = tg::Artifact::with_id(output.artifact);
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
	cleanup(temp, server).await;
	result.unwrap()
}
