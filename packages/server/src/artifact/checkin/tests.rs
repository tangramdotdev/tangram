use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
<<<<<<< HEAD
use insta::assert_snapshot;
use std::{panic::AssertUnwindSafe, pin::pin};
=======
use insta::{assert_json_snapshot, assert_snapshot};
use std::{panic::AssertUnwindSafe, path::PathBuf, pin::pin};
>>>>>>> 2bcebc93 (chore: add lockfiles to checkin snapshots)
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

/*
	// called by create_input_graph_inner
	fn get_dependencies(...) -> BTreeMap<tg::Reference, tg::Referent<Either<PathBuf, tg::object::Id>> {
		// get a list of un-resolved dependencies

		// get a list of resolved-dependencies (lockfile | xattr)

		// diff the two, if they match, use the resolved dependencies, else use unresolved dependencies

		// first, try and find the file in the lockfile, if it exists.
		// if it does, read read the dependencies
		// if it is a module, parse it
		// check if the dependencies are the same
	}

*/
#[tokio::test]
async fn lockfile_out_of_date() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" => r#"import "./b.tg.ts"#,
			"./b.tg.ts" => "",
			"tangram.lock" => r#"{
				"nodes": [
					{
						"kind": "directory",
						"entries": {
							"a.tg.ts": 1,
							"tangram.ts": 2
						}
					},
					{
						"kind": "file"
					},
					{
						"kind": "file",
						"dependencies": {
							"./a.tg.ts": {
								"item": 0,
								"subpath": "./a.tg.ts"
							}
						}
					}
				]
			}"#,
		},
		"",
		false,
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "b.tg.ts": 1,
           "tangram.ts": 2
         }
       },
       {
         "kind": "file"
       },
       {
         "kind": "file",
         "contents": "lef_01kvv10qev9ymf87zx83rb03jef2x5y2m919j20bs4wqpp09r0tm8g",
         "dependencies": {
           "./b.tg.ts": {
             "item": 0,
             "path": "",
             "subpath": "b.tg.ts"
           }
         }
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#""#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");

			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
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
async fn artifact_symlink() -> tg::Result<()> {
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import "../b/c"),
   		"dependencies": {
   			"../b/c": {
   				"item": tg.symlink({
   					"target": "e",
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         }
       },
       {
         "kind": "file",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         }
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
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
async fn package_with_nested_dependencies() -> tg::Result<()> {
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "bar": 1,
           "baz": 3,
           "tangram.ts": 5
         }
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 2
         }
       },
       {
         "kind": "file",
         "dependencies": {
           "../baz": {
             "item": 3,
             "path": "baz",
             "subpath": "tangram.ts"
           }
         }
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 4
         }
       },
       {
         "kind": "file"
       },
       {
         "kind": "file",
         "dependencies": {
           "./bar": {
             "item": 1,
             "path": "bar",
             "subpath": "tangram.ts"
           },
           "./baz": {
             "item": 3,
             "path": "baz",
             "subpath": "tangram.ts"
           }
         }
       }
     ]
   }
   "#);
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "foo.tg.ts": 1,
           "tangram.ts": 2
         }
       },
       {
         "kind": "file",
         "contents": "lef_01wz1kgzch869nmx5q4pq7ka0vjszxqa4nj39bgjgm2hpxwem2jdxg",
         "dependencies": {
           "./tangram.ts": {
             "item": 0,
             "path": "",
             "subpath": "tangram.ts"
           }
         }
       },
       {
         "kind": "file",
         "contents": "lef_01a2nf5j3bh75f7g1nntakjjtv6h3k0h7aykjstpyzamks4sebyz2g",
         "dependencies": {
           "./foo.tg.ts": {
             "item": 0,
             "path": "",
             "subpath": "foo.tg.ts"
           }
         }
       }
     ]
   }
   "#);
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
		|_, lockfile, _, output| async move {
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "bar": 1,
           "foo": 3
         }
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 2
         }
       },
       {
         "kind": "file"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 4
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
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
		|_, _, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"link": tg.symlink({
   		"target": ".",
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         }
       },
       {
         "kind": "file",
         "contents": "lef_01pqttaksgrf3n76tqrrhb6c96tyafzhrex2jgy54ht8419s6wpg2g",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         }
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         }
       },
       {
         "kind": "file",
         "contents": "lef_01fnhktwqxcgtzkra7arsx7d50rgmaycmnqxhrt58s0yb9xkg5ydjg",
         "dependencies": {
           "../foo": {
             "item": 0,
             "path": "",
             "subpath": "tangram.ts"
           }
         }
       }
     ]
   }
   "#);
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
		|_, _, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"hello.txt": tg.file({
   		"contents": tg.leaf("Hello, world!"),
   	}),
   	"link": tg.symlink({
   		"target": "hello.txt",
   	}),
   	"subdirectory": tg.directory({
   		"sublink": tg.symlink({
   			"target": "../link",
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
		|_, _, _, output| async move {
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
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
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "baz": 1,
           "tangram.ts": 3
         }
       },
       {
         "kind": "directory",
         "entries": {
           "mod.tg.ts": 2
         }
       },
       {
         "kind": "file",
         "contents": "lef_01z7p470yk3nybnm1f00y9m49p13rd52fqs1v9rwmmh9nd3s4stztg",
         "dependencies": {
           "..": {
             "item": 0,
             "path": "",
             "subpath": "tangram.ts"
           }
         }
       },
       {
         "kind": "file"
       }
     ]
   }
   "#);
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
		|_, _lockfile, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"a": tg.directory({
   		"b": tg.directory({
   			"c": tg.symlink({
   				"target": "../../a/d/e",
   			}),
   		}),
   		"d": tg.directory({
   			"e": tg.symlink({
   				"target": "../../a/f/g",
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
		|_server, _lockfile, _artifact, output| async move {
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
		|_, _, _, output| async move {
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
	artifact: impl Into<temp::Artifact>,
	path: &str,
	destructive: bool,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, Option<tg::Lockfile>, tg::Artifact, String) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact = artifact.into();
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;

		let path = directory.as_ref().join(path);
		let arg = tg::artifact::checkin::Arg {
			destructive,
			deterministic: false,
			ignore: true,
			locked: false,
			path: path.clone(),
		};
		let stream = server.check_in_artifact(arg).await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		// Get the lockfile if it exists.
		let lockfile = tokio::fs::read(path.join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.map(|bytes| serde_json::from_slice(&bytes))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))?;

		// Get the artifact.
		let artifact = tg::Artifact::with_id(output.artifact);

		// Print the output.
		let object = tg::Object::from(artifact.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(artifact.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		(assertions)(server.clone(), lockfile, artifact, output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
