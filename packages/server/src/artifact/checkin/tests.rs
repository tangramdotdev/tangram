use crate::{Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_temp::{self as temp, artifact, symlink, Temp};

#[tokio::test]
async fn simple_path_dependency() -> tg::Result<()> {
	test(
		artifact!({
			"foo": {
				"tangram.ts": r#"import * as bar from "../bar";"#,
			},
			"bar": {
				"tangram.ts": "",
			},
		}),
		"foo",
		|_, _, output| async move {
			eprintln!("{output}");
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
async fn nested_packages() -> tg::Result<()> {
	test(
		artifact!({
			"foo": {
				"tangram.ts": r#"
					import * as bar from "./bar";
					import * as baz from "./baz";
				"#,
				"bar": {
					"tangram.ts": r#"
						import * as baz from "../baz";
					"#,
				},
				"baz": {
					"tangram.ts": "",
				}
			},
		}),
		"foo",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
		tg.directory({
			"graph": tg.graph({
				"nodes": [
					{
						"kind": "file",
						"contents": tg.leaf("
								import * as baz from "../baz";
							"),
						"dependencies": {
							"../baz": {
								"item": 3,
								"subpath": "./baz",
							},
						},
					},
					{
						"kind": "directory",
						"entries": {
							"tangram.ts": 0,
						},
					},
					{
						"kind": "file",
						"contents": tg.leaf("
							import * as bar from "./bar";
							import * as baz from "./baz";
						"),
						"dependencies": {
							"./bar": {
								"item": 3,
								"subpath": "./bar",
							},
							"./baz": {
								"item": 3,
								"subpath": "./baz",
							},
						},
					},
					{
						"kind": "directory",
						"entries": {
							"bar": 1,
							"baz": tg.directory({
								"tangram.ts": tg.file({
									"contents": tg.leaf(""),
								}),
							}),
							"tangram.ts": 2,
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
async fn package_with_submodules() -> tg::Result<()> {
	test(
		artifact!({
			"package": {
				"tangram.ts": r#"import * as foo from "./foo.tg.ts";"#,
				"foo.tg.ts": r#"import * as root from "./tangram.ts";"#,
			}
		}),
		"package",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
				tg.directory({
					"graph": tg.graph({
						"nodes": [
							{
								"kind": "file",
								"contents": tg.leaf("import * as root from "./tangram.ts";"),
								"dependencies": {
									"./tangram.ts": {
										"item": 2,
										"subpath": "./tangram.ts",
									},
								},
							},
							{
								"kind": "file",
								"contents": tg.leaf("import * as foo from "./foo.tg.ts";"),
								"dependencies": {
									"./foo.tg.ts": {
										"item": 2,
										"subpath": "./foo.tg.ts",
									},
								},
							},
							{
								"kind": "directory",
								"entries": {
									"foo.tg.ts": 0,
									"tangram.ts": 1,
								},
							},
						],
					}),
					"node": 2,
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
		artifact!({
			"directory": {
				"link": symlink!("."),
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
							"artifact": 1,
						},
						{
							"kind": "directory",
							"entries": {
								"link": 0,
							},
						},
					],
				}),
				"node": 1,
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
			assert_snapshot!(output, @r#"
			tg.directory({
				"graph": tg.graph({
					"nodes": [
						{
							"kind": "file",
							"contents": tg.leaf("import * as foo from "../foo""),
							"dependencies": {
								"../foo": {
									"item": 3,
								},
							},
						},
						{
							"kind": "directory",
							"entries": {
								"tangram.ts": 0,
							},
						},
						{
							"kind": "file",
							"contents": tg.leaf("import * as bar from "../bar""),
							"dependencies": {
								"../bar": {
									"item": 1,
								},
							},
						},
						{
							"kind": "directory",
							"entries": {
								"tangram.ts": 2,
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
async fn file() -> tg::Result<()> {
	test(
		artifact!({
			"directory": {
				"README.md": "Hello, World!",
			}
		}),
		"directory",
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
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
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
	temp.remove()
		.await
		.map_err(|source| tg::error!(!source, "failed to remove temp"))?;
	result.unwrap()
}
