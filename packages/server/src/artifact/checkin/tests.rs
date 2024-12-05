use crate::{util::fs::cleanup, Config, Server};
use futures::{Future, FutureExt as _};
use insta::{assert_json_snapshot, assert_snapshot};
use std::{panic::AssertUnwindSafe, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};
use tg::handle::Ext;

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
		|_, _, metadata, _, output| async move {
			assert_json_snapshot!(metadata, @r#"
   {
     "complete": true,
     "count": 6,
     "depth": 3,
     "weight": 442
   }
   "#);
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
		|_, _, metadata, _, output| async move {
			assert_json_snapshot!(metadata, @r#"
   {
     "complete": true,
     "count": 3,
     "depth": 3,
     "weight": 172
   }
   "#);
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
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"link" => temp::symlink!("."),
			}
		},
		"directory",
		false,
		|_, _, metadata, _, output| async move {
			assert_json_snapshot!(metadata, @r#"
   {
     "complete": true,
     "count": 2,
     "depth": 2,
     "weight": 95
   }
   "#);
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
		|_, _, metadata, lockfile, output| async move {
			assert_json_snapshot!(&metadata, @r#"
   {
     "complete": true,
     "count": 4,
     "depth": 4,
     "weight": 440
   }
   "#);

			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "b.tg.ts": 1,
           "tangram.ts": 2
         },
         "id": "dir_0160h2b0874ht1tghsj4kx52azk05h57athnmrrfrmt0d44mvp33dg"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
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
         },
         "id": "fil_01vp5107n1fnx1sey6n000g3343kjhm6fzhmqvanvd5wfqhk2bn69g"
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
   					"b.tg.ts": tg.file({
   						"contents": tg.leaf(""),
   					}),
   					"tangram.ts": 1,
   				},
   			},
   			{
   				"kind": "file",
   				"contents": tg.leaf("import \"./b.tg.ts"),
   				"dependencies": {
   					"./b.tg.ts": {
   						"item": 0,
   						"path": "",
   						"subpath": "b.tg.ts",
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
		|_, _, metadata, lockfile, output| async move {
			assert_json_snapshot!(metadata, @r#"
   {
     "complete": true,
     "count": 5,
     "depth": 4,
     "weight": 378
   }
   "#);

			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01herzzj4fs0kkcx2gexc8g19z65ye1nf8bcf7seb79x7xwp0twb10"
       },
       {
         "kind": "file",
         "id": "fil_015kyznxcqzkjhghra06294bdqgbsc6s5jxhm9kkf7fzyabxmdhyp0"
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import \"../b/c/d"),
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
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01xbgdpny7b5ps7cnrmdnjb5hs5wdxg27kbdvpp0qmgcrv0r8n0st0"
       },
       {
         "kind": "file",
         "id": "fil_01t5dgeercxdbxwggkw0hzpzxmvqe8tqsmvbxbrtqz56zyh7sm1nc0"
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import \"../b/c"),
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
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01za2dxxhecas2y3r5rgas60mh7bjx2bwnyxc80xkcg9n4q91vj7n0"
       },
       {
         "kind": "file",
         "dependencies": {
           "../bar": {
             "item": 2,
             "path": "../bar",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_01pwga7r12479ej6c632wks0n1rs3wke5r7pswxmtcxebv0c2k7d0g"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import * as bar from \"../bar\";"),
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
		|_, _, _, lockfile, output| async move {
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
         },
         "id": "dir_01dzr73p15nepynspxv401bjk1k6qntf09whpr3bfhyp4anfmcgmmg"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 2
         },
         "id": "dir_0178dk0vn8m0ajqv0j37wjt24vmnqs21d7pn95hvncxnzqqbm922bg"
       },
       {
         "kind": "file",
         "dependencies": {
           "../baz": {
             "item": 3,
             "path": "baz",
             "subpath": "tangram.ts"
           }
         },
         "id": "fil_01w3vaptn2y3evejngg9r59ct6zf5mk723sh6dhsdd4sn4220ffw0g"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 4
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
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
         },
         "id": "fil_015ya4qczcz84x720tkc0fx7kmm474kqv87ykptenwcf7qj573tv8g"
       }
     ]
   }
   "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"bar": tg.directory({
   		"tangram.ts": tg.file({
   			"contents": tg.leaf("\n\t\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t\t"),
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
   		"contents": tg.leaf("\n\t\t\t\t\timport * as bar from \"./bar\";\n\t\t\t\t\timport * as baz from \"./baz\";\n\t\t\t\t"),
   		"dependencies": {
   			"./bar": {
   				"item": tg.directory({
   					"tangram.ts": tg.file({
   						"contents": tg.leaf("\n\t\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t\t"),
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
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "foo.tg.ts": 1,
           "tangram.ts": 2
         },
         "id": "dir_010prey26p516r3ms8z1sft1s4n3xpm5jbpt5h3qqmemm61kg97gh0"
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
         },
         "id": "fil_01ktrenvk1711bckv8hdpe0ze6ty00yxexptrts9b26vh994frtbwg"
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
         },
         "id": "fil_01brq5a8r3prynqs1say65vbcv47z0e4qtk5gadg2q11hazjfnqyqg"
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
   				"contents": tg.leaf("import * as foo from \"./foo.tg.ts\";"),
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
   				"contents": tg.leaf("import * as root from \"./tangram.ts\";"),
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
		|_, _, _, lockfile, output| async move {
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "bar": 1,
           "foo": 3
         },
         "id": "dir_018v5bakd2wjnpf2r4t0q81cz193k4m43jt5kpgret344p0txda2ng"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 2
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 4
         },
         "id": "dir_01xgt4hh9nhgacbsa204wnne5346kmfd2eh3ddqqde8gyr6ma2jcj0"
       },
       {
         "kind": "file",
         "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
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
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_014q009v7jzv5686j2f0y4j17m53rsev0gy814qkchmrrnsycf32xg"
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
         },
         "id": "fil_019ah53qck1p9xxe0jxvb0vxwdnt4680bfj0xtj9etawagthsrh1e0"
       },
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 3
         },
         "id": "dir_01w261pda787rg6f89jxqzevkbt30bbbe1zwnsnjav6mxdx7gcs27g"
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
         },
         "id": "fil_0157mddgf6en44fc11ercy1b022135je68p0h3x3hka1syke5abvz0"
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
   				"contents": tg.leaf("import * as bar from \"../bar\""),
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
   				"contents": tg.leaf("import * as foo from \"../foo\""),
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
async fn package() -> tg::Result<()> {
	test(
		temp::directory! {
			"directory" => temp::directory! {
				"tangram.ts" => "export default tg.target(() => {})",
			}
		},
		"directory",
		false,
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "tangram.ts": 1
         },
         "id": "dir_01hw68xvst4xee12q5hcjm1v1n2payedrka0ehvhy1zc6msmga5pv0"
       },
       {
         "kind": "file",
         "id": "fil_017nhpghzswya07fxkrvjeqrehffdp11em51yag7msbzzt65gteqrg"
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
		|_, _, _, lockfile, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(lockfile, @r#"
   {
     "nodes": [
       {
         "kind": "directory",
         "entries": {
           "baz": 1,
           "tangram.ts": 3
         },
         "id": "dir_01f9q70fj1f9fj75n94bcdwsrgjbtxetqv9dmjnhw3m1grrztzphr0"
       },
       {
         "kind": "directory",
         "entries": {
           "mod.tg.ts": 2
         },
         "id": "dir_01dzvxenaqf25mpnt681c2p51hensma2g1echwwbn621jrg62bwvb0"
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
         },
         "id": "fil_010vre9ea0zrmdj1fmve4dj1bhw0xpq8af7wk1jq1ejt1gmxn0nwa0"
       },
       {
         "kind": "file",
         "id": "fil_01rr7hzyjv7nws35wxdnh2txgscff7nqwtfqrwa5g4ghkaktc7hhs0"
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
   						"contents": tg.leaf("import patches from \"./foo\" with { type: \"directory\" };"),
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
   				"contents": tg.leaf("import * as baz from \"..\";"),
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
		|_, _, _, _lockfile, output| async move {
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
		|_server, _artifact, _metadata, _lockfile, output| async move {
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
   				"contents": tg.leaf("import * as a from \"./a.tg.ts"),
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
		|_, _, _, _, output| async move {
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

#[tokio::test]
async fn tagged_object() -> tg::Result<()> {
	let file = tg::file!("Hello, world!");
	let tag = "hello-world".parse::<tg::Tag>().unwrap();
	let artifact = temp::directory! {
		"tangram.ts" => r#"import hello from "hello-world""#,
	};
	test_with_tags(
		artifact,
		[(tag, file)],
		|_, lockfile, _, output| async move {
			let lockfile = lockfile.expect("expected a lockfile");
			assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01nnp9jbwak11sksnwv2jtcdjyv252wg1ksm3zv4pbrc8dvdbkkrw0"
      },
      {
        "kind": "file",
        "dependencies": {
          "hello-world": {
            "item": "fil_01yxtf8s9sxc1dcv6vs0zjxhra1xp11j97h485cjhmtwa4mrrzbrag",
            "tag": "hello-world"
          }
        },
        "id": "fil_01713frgeanw8fswtkpa36t55p90zhgmsm8hht2nsfxhzxs21dq5p0"
      }
    ]
  }
  "#);
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("import hello from \"hello-world\""),
   		"dependencies": {
   			"hello-world": {
   				"item": tg.file({
   					"contents": tg.leaf("Hello, world!"),
   				}),
   				"tag": "hello-world",
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
async fn diamond_dependency() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		publish(
			&server,
			"a/1.0.0",
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
				// a/tangram.ts
				export default tg.target(() => "a/1.0.0");
			"#),
			},
		)
		.await?;
		publish(
			&server,
			"a/1.1.0",
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
				// a/tangram.ts
				export default tg.target(() => "a/1.1.0");
			"#),
			},
		)
		.await?;
		publish(
			&server,
			"b",
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
				// b/tangram.ts
				import a from "a/^1";
				export default tg.target(() => "b");
			"#),
			},
		)
		.await?;
		publish(
			&server,
			"c",
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
				// c/tangram.ts
				import a from "a^1.0";
				export default tg.target(() => "c");
			"#),
			},
		)
		.await?;
		let (_artifact, _metadata, lockfile, output) = checkin(
			&server,
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
				import b from "b";
				import c from "c";
			"#),
			},
		)
		.await
		.inspect_err(|error| {
			let trace = error.trace(&server.config.advanced.error_trace_options);
			eprintln!("{trace}");
		})?;

		assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01h8qq59a6xpn9chea3nkv0jnraspyq5khthsczzrqc8n2zcde4wg0"
      },
      {
        "kind": "file",
        "dependencies": {
          "b": {
            "item": "dir_0131j20yz5zp2d85eenrm4x1wwebmjj83dbym69eqyet3yd3g33j20",
            "tag": "b"
          },
          "c": {
            "item": "dir_0146bk6v3szf75s5yjv0canqcn6f6paqq3phcene475jgfkqwdty6g",
            "tag": "c"
          }
        },
        "id": "fil_01pe736efywyyczf8sr0ppe3ntes5q5m3g35cr9wzgaj8gyrzkgh3g"
      }
    ]
  }
  "#);
		assert_snapshot!(output, @r#"
  tg.directory({
  	"tangram.ts": tg.file({
  		"contents": tg.leaf("import b from \"b\";\nimport c from \"c\";\n"),
  		"dependencies": {
  			"b": {
  				"item": tg.directory({
  					"tangram.ts": tg.file({
  						"contents": tg.leaf("// b/tangram.ts\nimport a from \"a/^1\";\nexport default tg.target(() => \"b\");\n"),
  						"dependencies": {
  							"a/^1": {
  								"item": tg.directory({
  									"tangram.ts": tg.file({
  										"contents": tg.leaf("// a/tangram.ts\nexport default tg.target(() => \"a/1.0.0\");\n"),
  									}),
  								}),
  								"tag": "a/1.0.0",
  							},
  						},
  					}),
  				}),
  				"tag": "b",
  			},
  			"c": {
  				"item": tg.directory({
  					"tangram.ts": tg.file({
  						"contents": tg.leaf("// c/tangram.ts\nimport a from \"a^1.0\";\nexport default tg.target(() => \"c\");\n"),
						"dependencies": {
  							"a/^1.0": {
  								"item": tg.directory({
  									"tangram.ts": tg.file({
  										"contents": tg.leaf("// a/tangram.ts\nexport default tg.target(() => \"a/1.0.0\");\n"),
  									}),
  								}),
  								"tag": "a/1.0.0",
  							},
  						},
  					}),
  				}),
  				"tag": "c",
  			},
  		},
  	}),
  })
  "#);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

async fn test<F, Fut>(
	artifact: impl Into<temp::Artifact>,
	path: &str,
	destructive: bool,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Artifact, tg::object::Metadata, Option<tg::Lockfile>, String) -> Fut,
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
			lockfile: true,
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

		// Get the metadata.
		let metadata = server
			.get_object_metadata(&output.artifact.clone().into())
			.await?;

		// Get the artifact.
		let artifact = tg::Artifact::with_id(output.artifact);

		// Get the object.
		let object = tg::Object::from(artifact.clone());

		// Print the output.
		object.load_recursive(&server).await?;
		let value = tg::Value::from(artifact.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		(assertions)(server.clone(), artifact, metadata, lockfile, output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

async fn test_with_tags<F, Fut>(
	artifact: impl Into<temp::Artifact>,
	tags: impl IntoIterator<Item = (impl Into<tg::Tag>, impl Into<tg::Object>)>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, Option<tg::Lockfile>, tg::Artifact, String) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact: temp::Artifact = artifact.into();
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;

	let result = AssertUnwindSafe(async {
		// Put tags.
		for (tag, object) in tags {
			let id = object.into().id(&server).await?;
			server
				.put_tag(
					&tag.into(),
					tg::tag::put::Arg {
						force: false,
						item: Either::Right(id),
						remote: None,
					},
				)
				.await?;
		}
		let temp = Temp::new();
		artifact.to_path(temp.path()).await.unwrap();

		// Checkin the artifact.
		let arg = tg::artifact::checkin::Arg {
			path: temp.path().to_owned(),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
		};
		let artifact = tg::Artifact::check_in(&server, arg).await?;
		let lockfile = tg::Lockfile::try_read(&temp.path().join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.flatten();

		// Print the output.
		let object = tg::Object::from(artifact.clone());
		object.load_recursive(&server).await?;
		let value = tg::Value::from(artifact.clone());
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);

		// Test the assertinos.
		(assertions)(server.clone(), lockfile, artifact, output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

async fn publish(
	server: &Server,
	tag: &str,
	artifact: impl Into<temp::Artifact>,
) -> tg::Result<()> {
	let (artifact, _, _, _) = checkin(server, artifact).await?;
	let tag = tag.parse().map_err(|_| tg::error!("failed to parse tag"))?;
	let arg = tg::tag::put::Arg {
		item: Either::Right(artifact.id(server).await?.into()),
		force: false,
		remote: None,
	};
	server.put_tag(&tag, arg).await?;
	Ok(())
}

async fn checkin(
	server: &Server,
	artifact: impl Into<temp::Artifact>,
) -> tg::Result<(
	tg::Artifact,
	tg::object::Metadata,
	Option<tg::Lockfile>,
	String,
)> {
	let temp = Temp::new();
	let artifact: temp::Artifact = artifact.into();
	artifact
		.to_path(temp.path())
		.await
		.map_err(|source| tg::error!(!source, "failed to create artifact"))?;
	let arg = tg::artifact::checkin::Arg {
		path: temp.path().to_owned(),
		destructive: false,
		deterministic: false,
		ignore: true,
		locked: false,
		lockfile: true,
	};
	let artifact = tg::Artifact::check_in(server, arg).await?;
	let lockfile = tg::Lockfile::try_read(&temp.path().join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.flatten();
	let object = tg::Object::from(artifact.clone());
	let metadata = server
		.get_object_metadata(&object.id(server).await?)
		.await?;
	object.load_recursive(server).await?;
	let value = tg::Value::from(artifact.clone());
	let options = tg::value::print::Options {
		recursive: true,
		style: tg::value::print::Style::Pretty { indentation: "\t" },
	};
	let output = value.print(options);
	Ok((artifact, metadata, lockfile, output))
}
