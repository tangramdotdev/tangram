use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::{future::Future, path::Path};
use tangram_cli::{
	assert_success,
	test::{test, Server},
};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"hello.txt" => "Hello, world!",
			"link" => temp::symlink!("hello.txt"),
			"subdirectory" => temp::directory! {
				"sublink" => temp::symlink!("../link"),
			}
		}
	};
	let path = "directory";
	let assertions = |object: String, metadata: String, _| async move {
		assert_snapshot!(object, @r#"
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
		assert_snapshot!(metadata, @r#"
  {
    "complete": true,
    "count": 6,
    "depth": 3,
    "weight": 442
  }
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn file() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"README.md" => "Hello, World!",
		}
	};
	let path = "directory";
	let assertions = |object: String, metadata: String, _| async move {
		assert_snapshot!(object, @r#"
  tg.directory({
    "README.md": tg.file({
      "contents": tg.leaf("Hello, World!"),
    }),
  })
  "#);
		assert_snapshot!(metadata, @r#"
  {
    "complete": true,
    "count": 3,
    "depth": 3,
    "weight": 172
  }
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn symlink() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"link" => temp::symlink!("."),
		}
	};
	let path = "directory";
	let assertions = |object: String, metadata: String, _| async move {
		assert_snapshot!(object, @r#"
  tg.directory({
    "link": tg.symlink({
      "target": ".",
    }),
  })
  "#);
		assert_snapshot!(metadata, @r#"
  {
    "complete": true,
    "count": 2,
    "depth": 2,
    "weight": 95
  }
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn file_through_symlink() {
	let directory = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c/d"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	};
	let path = "a";
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
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
		assert_snapshot!(metadata, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn artifact_symlink() {
	let directory = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	};
	let path = "a";
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
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
		assert_snapshot!(metadata, @r#"
  {
    "complete": true,
    "count": 4,
    "depth": 3,
    "weight": 300
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
        "id": "dir_01xbgdpny7b5ps7cnrmdnjb5hs5wdxg27kbdvpp0qmgcrv0r8n0st0"
      },
      {
        "kind": "file",
        "id": "fil_01t5dgeercxdbxwggkw0hzpzxmvqe8tqsmvbxbrtqz56zyh7sm1nc0"
      }
    ]
  }
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn lockfile_out_of_date() {
	let directory = temp::directory! {
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
		}"#
	};
	let path = "";
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(metadata, @r#"
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
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn simple_path_dependency() {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar";"#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		},
	};
	let path = "foo";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package_with_nested_dependencies() {
	let directory = temp::directory! {
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
	};
	let path = "foo";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
        "id": "dir_016c0hyd587p2w6mcgeadwm73rjvbnwrbfej4exgdjw9xb7jynn6xg"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 2
        },
        "id": "dir_017jbe3ap28vj9qxaa4tdcbrfbvbqm48sjw20d0184bj4b4h3r5p10"
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
        "id": "fil_01w9ep677eqfh59vsanqg0ydkrkq8n9yqbjm0tqsj3f3xgdfqny87g"
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
        "id": "fil_01ym6gkmf9npa0n7xpwk64mc6dap53qnxan6qm4vhdw1ev2g3nnamg"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "bar": tg.directory({
      "tangram.ts": tg.file({
        "contents": tg.leaf("\n\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t"),
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
      "contents": tg.leaf("\n\t\t\t\timport * as bar from \"./bar\";\n\t\t\t\timport * as baz from \"./baz\";\n\t\t\t"),
      "dependencies": {
        "./bar": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf("\n\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t"),
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"tangram.ts" => "export default tg.target(() => {})",
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf("export default tg.target(() => {})"),
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn directory_with_nested_packages() {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => "",
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	};
	let path = "";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
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
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_directory_from_current() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory! {
				"mod.tg.ts" => r#"import a from ".";"#
			},
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "a": 1
        },
        "id": "dir_013qp759rjhk3kbma89abr001b0sc65ts68p9dwt1anww7mn0y64mg"
      },
      {
        "kind": "directory",
        "entries": {
          "mod.tg.ts": 2
        },
        "id": "dir_018ctma2cbtavcj881dm0y2n7kz8r5nrqmdp5npq4j9ya5cmvq9wa0"
      },
      {
        "kind": "file",
        "contents": "lef_013zybf1ec34vd94gv58eqsje8jctv68qy169rz9sdqrcb9kpmargg",
        "dependencies": {
          ".": {
            "item": 0,
            "path": "",
            "subpath": "a"
          }
        },
        "id": "fil_01twcsm3s5h948nr90af1x05v2t6p5ct91th1wb82r90se6v0sdtbg"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "graph": tg.graph({
      "nodes": [
        {
          "kind": "directory",
          "entries": {
            "a": 1,
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
          "contents": tg.leaf("import a from \".\";"),
          "dependencies": {
            ".": {
              "item": 0,
              "path": "",
              "subpath": "a",
            },
          },
        },
      ],
    }),
    "node": 0,
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_package_from_current() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory! {
				"mod.tg.ts" => r#"import * as a from ".";"#,
				"tangram.ts" => ""
			},
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "a": 1
        },
        "id": "dir_01j82ndr1rmj49j2jzxqp9fktgavstdpztsrrbyb1jga7aqqj41ym0"
      },
      {
        "kind": "directory",
        "entries": {
          "mod.tg.ts": 2,
          "tangram.ts": 3
        },
        "id": "dir_013jcybpz1dg60gjgtszaak8jreskbmcqqpm38xyz958b2vpz3qc9g"
      },
      {
        "kind": "file",
        "contents": "lef_01f5c3vv1z4ejbnxc9nza26gecndwkt8n7jpbm5hw1gx9yega1y150",
        "dependencies": {
          ".": {
            "item": 1,
            "path": "a",
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01p3822cwafpv4ggyrq9s1kjhk16bn00z5m0d5yp1dgccyknpzb76g"
      },
      {
        "kind": "file",
        "id": "fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "a": tg.directory({
      "graph": tg.graph({
        "nodes": [
          {
            "kind": "directory",
            "entries": {
              "mod.tg.ts": 1,
              "tangram.ts": tg.file({
                "contents": tg.leaf(""),
              }),
            },
          },
          {
            "kind": "file",
            "contents": tg.leaf("import * as a from \".\";"),
            "dependencies": {
              ".": {
                "item": 0,
                "path": "a",
                "subpath": "tangram.ts",
              },
            },
          },
        ],
      }),
      "node": 0,
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_directory_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{},
			"tangram.ts" => r#"import a from "./a""#,
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_014acgrfbgqq3cmcasbm0rg11kjga3r503c6xehfvxrenbb9vez4jg"
      },
      {
        "kind": "file",
        "contents": "lef_01547n1jzmegft5pxdjhfxjmkttsdh085eec8v22key5vrtzqskwy0",
        "dependencies": {
          "./a": {
            "item": 0,
            "path": "",
            "subpath": "a"
          }
        },
        "id": "fil_01yf698d5k87pyhkeqwxaadcpfyasp5mc7tad3vn2wk6zbsv4t6qz0"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "graph": tg.graph({
      "nodes": [
        {
          "kind": "directory",
          "entries": {
            "a": tg.directory({}),
            "tangram.ts": 1,
          },
        },
        {
          "kind": "file",
          "contents": tg.leaf("import a from \"./a\""),
          "dependencies": {
            "./a": {
              "item": 0,
              "path": "",
              "subpath": "a",
            },
          },
        },
      ],
    }),
    "node": 0,
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_package_with_type_directory_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a" with { type: "directory" }"#,
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "a": 1,
          "tangram.ts": 3
        },
        "id": "dir_014gmvcx8sb5zs49yqncntptqfcex3bve7tcn0cya3263mnr5t72e0"
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
        "kind": "file",
        "contents": "lef_011rnw4cfjf09t68gc321s8bhb61x6ev27eraretthfyaazk1vgt10",
        "dependencies": {
          "./a": {
            "item": 0,
            "path": "",
            "subpath": "a"
          }
        },
        "id": "fil_01tyeqe83hx84c7npb1ayy1m9d4a84gs4tmvcfp8mtwn3na7728j4g"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "graph": tg.graph({
      "nodes": [
        {
          "kind": "directory",
          "entries": {
            "a": tg.directory({
              "tangram.ts": tg.file({
                "contents": tg.leaf(""),
              }),
            }),
            "tangram.ts": 1,
          },
        },
        {
          "kind": "file",
          "contents": tg.leaf("import a from \"./a\" with { type: \"directory\" }"),
          "dependencies": {
            "./a": {
              "item": 0,
              "path": "",
              "subpath": "a",
            },
          },
        },
      ],
    }),
    "node": 0,
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_package_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a"#,
		}
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "a": 1,
          "tangram.ts": 3
        },
        "id": "dir_011j1bpzfppq2r4vvtg0v4ed5we7s1jwnfdmrgr8m9tj1sk1e6n9b0"
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
        "kind": "file",
        "dependencies": {
          "./a": {
            "item": 1,
            "path": "a",
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_016g7fp581r3e8n81dg6na50bddcen9pk8prqgqct7df8pqzphyyag"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "a": tg.directory({
      "tangram.ts": tg.file({
        "contents": tg.leaf(""),
      }),
    }),
    "tangram.ts": tg.file({
      "contents": tg.leaf("import a from \"./a"),
      "dependencies": {
        "./a": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf(""),
            }),
          }),
          "path": "a",
          "subpath": "tangram.ts",
        },
      },
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package_with_cyclic_modules() {
	let directory = temp::directory! {
		"package" => temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts";"#,
			"foo.tg.ts" => r#"import * as root from "./tangram.ts";"#,
		}
	};
	let path = "package";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn cyclic_dependencies() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "../foo""#,
			},
		},
	};
	let path = "directory/foo";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn directory_destructive() {
	let directory = temp::directory! {
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
	};
	let path = "directory";
	let assertions = |object: String, _metadata: String, _lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
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
	};
	let destructive = true;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package_destructive() {
	let directory = temp::directory! {
		"tangram.ts" => r#"import * as a from "./a.tg.ts"#,
		"a.tg.ts" => "",
	};
	let path = "";
	let assertions = |object: String, _metadata: String, _lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
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
	};
	let destructive = true;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn default_ignore() {
	let directory = temp::directory! {
		".DS_Store" => temp::file!(""),
		".git" => temp::directory! {
			"config" => temp::file!(""),
		},
		".tangram" => temp::directory! {
			"config" => temp::file!(""),
		},
		"tangram.lock" => temp::file!(r#"{"nodes":[]}"#),
		"tangram.ts" => temp::file!(""),
	};
	let path = "";
	let assertions = |object: String, _metadata: String, _lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf(""),
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn missing_in_lockfile() {
	let directory = temp::directory! {
		"tangram.ts" => r#"import * as a from "./a"#,
		"tangram.lock" => temp::file!(indoc!(r#"
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
			}
		"#)),
		"a" => temp::directory! {
			"tangram.ts" => "",
		},
	};
	let path = "a";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
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
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf(""),
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn invalid_lockfile() {
	let directory = temp::directory! {
		"tangram.lock" => temp::file!(indoc!(r#"
			{
				"nodes": [
					{
						"kind": "file"
					}
				]
			}
		"#)),
		"a" => temp::directory! {
			"tangram.ts" => "",
		},
	};
	let path = "a";
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
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
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf(""),
    }),
  })
  "#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_object() {
	let tags = vec![("hello-world".into(), temp::file!("Hello, world!"))];
	let directory = temp::directory! {
		"tangram.ts" => r#"import hello from "hello-world""#,
	};
	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
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
            "item": 2,
            "tag": "hello-world"
          }
        },
        "id": "fil_01713frgeanw8fswtkpa36t55p90zhgmsm8hht2nsfxhzxs21dq5p0"
      },
      {
        "kind": "file",
        "contents": "lef_01xqjw1c8f5v29f739pmqp3s1fypt16mcww2dy1wad166wzsfpz66g",
        "id": "fil_01yxtf8s9sxc1dcv6vs0zjxhra1xp11j97h485cjhmtwa4mrrzbrag"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
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
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package() {
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default tg.target(() => "a");
			"#),
		},
	)];
	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
			export default tg.target(async () => {
				return await a();
			});
		"#)
	};
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01qr8dyk7j1dcm99fcv2yf8xq3ex4040b5j44z16nk9ecn4mmrss9g"
      },
      {
        "kind": "file",
        "dependencies": {
          "a": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "a"
          }
        },
        "id": "fil_01nj9vsy1dr2p3771v34pq49t2a55tyt0m0mt8bz7ryg1ascs09zeg"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01rt33awnmsr341vxm2tejp5txxn7zrrca8hqwtp2jxfakxv8ps75g"
      },
      {
        "kind": "file",
        "contents": "lef_01fktw8vh756v2jmnpp5dyy76nvj3e3d9eysxp24ebr2anqprrbwz0",
        "id": "fil_01kjqjy201v16b4xg7vs9qcyj69jm5bd6h2k3qrsr6cznmbhswaea0"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf("import a from \"a\";\nexport default tg.target(async () => {\n\treturn await a();\n});\n"),
      "dependencies": {
        "a": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf("export default tg.target(() => \"a\");\n"),
            }),
          }),
          "subpath": "tangram.ts",
          "tag": "a",
        },
      },
    }),
  })
  "#);
	};
	let destructive = false;
	let path = "";
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package_with_cyclic_dependency() {
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				import foo from "./foo.tg.ts";
			"#),
			"foo.tg.ts" => indoc::indoc!(r#"
				import * as a from "./tangram.ts";
			"#),
		},
	)];

	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
		"#),
	};

	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_018a9xvdxyjk4hbje5z1tvapt7b0nckwa4zdvmshnp2g0gnx8sbbpg"
      },
      {
        "kind": "file",
        "dependencies": {
          "a": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "a"
          }
        },
        "id": "fil_016y98m3qa8ks5gpy32dmr8837eb4jsrz9zeay0wfjjabb0twm03ng"
      },
      {
        "kind": "directory",
        "entries": {
          "foo.tg.ts": 3,
          "tangram.ts": 4
        },
        "id": "dir_01ddweh83zaxjcq0qabp7w0qcnx5a4cnapbdstxk9anvxey5b1vmrg"
      },
      {
        "kind": "file",
        "contents": "lef_01mn2bw5f7w51jk66tjtvh9114zrdnx7saptmrark0jeafebh9yn6g",
        "dependencies": {
          "./tangram.ts": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "a"
          }
        },
        "id": "fil_01pgtnj5xx2kan9g7th6shsj5p1xy8vejk55z12n64eetpws8pqd50"
      },
      {
        "kind": "file",
        "contents": "lef_015n08n4fm2ves89zvbfqxnej2byq4bacb1qszgy73f321haskq7z0",
        "dependencies": {
          "./foo.tg.ts": {
            "item": 2,
            "subpath": "foo.tg.ts",
            "tag": "a"
          }
        },
        "id": "fil_018mvpkw54dbcje47y0nkq4sra4y1axkhd2ka9n2xrd41w2tpqne70"
      }
    ]
  }
  "#);
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf("import a from \"a\";\n"),
      "dependencies": {
        "a": {
          "item": tg.directory({
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
                  "contents": tg.leaf("import foo from \"./foo.tg.ts\";\n"),
                  "dependencies": {
                    "./foo.tg.ts": {
                      "item": 0,
                      "subpath": "foo.tg.ts",
                      "tag": "a",
                    },
                  },
                },
                {
                  "kind": "file",
                  "contents": tg.leaf("import * as a from \"./tangram.ts\";\n"),
                  "dependencies": {
                    "./tangram.ts": {
                      "item": 0,
                      "subpath": "tangram.ts",
                      "tag": "a",
                    },
                  },
                },
              ],
            }),
            "node": 0,
          }),
          "subpath": "tangram.ts",
          "tag": "a",
        },
      },
    }),
  })
  "#);
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tag_dependency_cycles() {
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => "",
			},
		),
		(
			"b/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as a from "a/*";
					import * as foo from "./foo.tg.ts";
				"#),
				"foo.tg.ts" => indoc!(r#"
					import * as b from "./tangram.ts";
				"#),
			},
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as b from "b/*";
				"#),
			},
		),
	];

	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as b from "b/*";
			import * as a from "a/*";
		"#),
	};

	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01tzv7mnqxkq5t3sw5sg8jes7bjdazgrsnmp9zt93fqswazpz43r8g"
      },
      {
        "kind": "file",
        "dependencies": {
          "a/*": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "a/1.1.0"
          },
          "b/*": {
            "item": 4,
            "subpath": "tangram.ts",
            "tag": "b/1.0.0"
          }
        },
        "id": "fil_01s77r9zrw3fj54epdrw103dptdbqzke2878y272e35g5s7qs1yd6g"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01c10z175t2xft8kp58f71vs990yyt3wxbhcrvb9qe7zkb5qga8pwg"
      },
      {
        "kind": "file",
        "contents": "lef_015qtah5hxy64eyx8vqccxt04pwnm87j851w784yr4nq7rjrmgtts0",
        "dependencies": {
          "b/*": {
            "item": 4,
            "subpath": "tangram.ts",
            "tag": "b/1.0.0"
          }
        },
        "id": "fil_01v140jjq24vh3nk8n61rz28t9xyqy365prfrnadmz65m8hzckwaqg"
      },
      {
        "kind": "directory",
        "entries": {
          "foo.tg.ts": 5,
          "tangram.ts": 6
        },
        "id": "dir_01bjjfcggydyvgjaetwh5pazhnmsg1wgetxdahpewqjvpc4pszefk0"
      },
      {
        "kind": "file",
        "contents": "lef_016bmyhqk81jgbkns2m3w5yke2h9dadqt4r60mnkcpedev96hha9j0",
        "dependencies": {
          "./tangram.ts": {
            "item": 4,
            "subpath": "tangram.ts",
            "tag": "b/1.0.0"
          }
        },
        "id": "fil_0152j633eft4bbtswdmjrkqy2awwj75kck7k1cyx7hkb0bw30rmgbg"
      },
      {
        "kind": "file",
        "contents": "lef_01gj0fndzaa1p7tch03pvja4ne3nhv0rwewcvm9w4v2k1xwckacwy0",
        "dependencies": {
          "./foo.tg.ts": {
            "item": 4,
            "subpath": "foo.tg.ts",
            "tag": "b/1.0.0"
          },
          "a/*": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "a/1.1.0"
          }
        },
        "id": "fil_01mc1angae3s1me6na3zw6axjdjkke8m13a97s0tmnh7w7at0p9ss0"
      }
    ]
  }
  "#); // Keep existing snapshot
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf("import * as b from \"b/*\";\nimport * as a from \"a/*\";\n"),
      "dependencies": {
        "a/*": {
          "item": tg.directory({
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
                  "contents": tg.leaf("import * as b from \"b/*\";\n"),
                  "dependencies": {
                    "b/*": {
                      "item": 2,
                      "subpath": "tangram.ts",
                      "tag": "b/1.0.0",
                    },
                  },
                },
                {
                  "kind": "directory",
                  "entries": {
                    "foo.tg.ts": 4,
                    "tangram.ts": 3,
                  },
                },
                {
                  "kind": "file",
                  "contents": tg.leaf("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
                  "dependencies": {
                    "./foo.tg.ts": {
                      "item": 2,
                      "subpath": "foo.tg.ts",
                      "tag": "b/1.0.0",
                    },
                    "a/*": {
                      "item": 0,
                      "subpath": "tangram.ts",
                      "tag": "a/1.1.0",
                    },
                  },
                },
                {
                  "kind": "file",
                  "contents": tg.leaf("import * as b from \"./tangram.ts\";\n"),
                  "dependencies": {
                    "./tangram.ts": {
                      "item": 2,
                      "subpath": "tangram.ts",
                      "tag": "b/1.0.0",
                    },
                  },
                },
              ],
            }),
            "node": 0,
          }),
          "subpath": "tangram.ts",
          "tag": "a/1.1.0",
        },
        "b/*": {
          "item": tg.directory({
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
                  "contents": tg.leaf("import * as b from \"b/*\";\n"),
                  "dependencies": {
                    "b/*": {
                      "item": 2,
                      "subpath": "tangram.ts",
                      "tag": "b/1.0.0",
                    },
                  },
                },
                {
                  "kind": "directory",
                  "entries": {
                    "foo.tg.ts": 4,
                    "tangram.ts": 3,
                  },
                },
                {
                  "kind": "file",
                  "contents": tg.leaf("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
                  "dependencies": {
                    "./foo.tg.ts": {
                      "item": 2,
                      "subpath": "foo.tg.ts",
                      "tag": "b/1.0.0",
                    },
                    "a/*": {
                      "item": 0,
                      "subpath": "tangram.ts",
                      "tag": "a/1.1.0",
                    },
                  },
                },
                {
                  "kind": "file",
                  "contents": tg.leaf("import * as b from \"./tangram.ts\";\n"),
                  "dependencies": {
                    "./tangram.ts": {
                      "item": 2,
                      "subpath": "tangram.ts",
                      "tag": "b/1.0.0",
                    },
                  },
                },
              ],
            }),
            "node": 2,
          }),
          "subpath": "tangram.ts",
          "tag": "b/1.0.0",
        },
      },
    }),
  })
  "#); // Keep existing snapshot
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn diamond_dependency() {
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default tg.target(() => "a/1.0.0");
				"#),
			},
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default tg.target(() => "a/1.1.0");
				"#),
			},
		),
		(
			"b".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1";
					export default tg.target(() => "b");
				"#),
			},
		),
		(
			"c".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1.0";
					export default tg.target(() => "c");
				"#),
			},
		),
	];

	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import b from "b";
			import c from "c";
		"#),
	};

	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(&lockfile, @r#"
  {
    "nodes": [
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 1
        },
        "id": "dir_01nfcqmz3k2j62ht4xh019mnxcb977twjvegsjaseggx8hpw36qjq0"
      },
      {
        "kind": "file",
        "dependencies": {
          "b": {
            "item": 2,
            "subpath": "tangram.ts",
            "tag": "b"
          },
          "c": {
            "item": 6,
            "subpath": "tangram.ts",
            "tag": "c"
          }
        },
        "id": "fil_0187wrqf0jgz30d57n8h6wekf4s00qw95jdcz8rgffsnfp6ae4dpmg"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01faxd74z5acazcn9vzj7w462ya1kddzy2gnkgwh9m79sn9faw37dg"
      },
      {
        "kind": "file",
        "contents": "lef_01pb6cshy8jfsp41qj4s2h2nx171dt7p3j92ykbhrsqx0zj6nbgc70",
        "dependencies": {
          "a/^1": {
            "item": 4,
            "subpath": "tangram.ts",
            "tag": "a/1.1.0"
          }
        },
        "id": "fil_01rfczghzmc67r15mqyxd183aqb5x72c58hfzy1e1vctjw34dr28jg"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 5
        },
        "id": "dir_01asv8vefcr8svvstfvhatdpvnyq1f5v1mtc5g94b29x2ev2meda9g"
      },
      {
        "kind": "file",
        "contents": "lef_01x2np2j14ewfghscp44cmtwyfg9ed8pfmcffmwccrs02gsv3ygmp0",
        "id": "fil_016cwcgvz23avmszqk01t8kh9j7aaaw8y43s8q28v30zgpq276c43g"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 7
        },
        "id": "dir_01zxyr1tfax0cjmbcz1bmyg7e7fm4p1gh895qbe7nxfbkh1307gybg"
      },
      {
        "kind": "file",
        "contents": "lef_01nscf5kvpy1ebv0bemqh1562vz0etqrk30e9jyenpwyxfbmfcmxgg",
        "dependencies": {
          "a/^1.0": {
            "item": 4,
            "subpath": "tangram.ts",
            "tag": "a/1.1.0"
          }
        },
        "id": "fil_01g6m0gdvz6j4bhxyxwy4pxqyqajwj45czzgcy2e8hzbnarpmd741g"
      }
    ]
  }
  "#); // Keep existing snapshot
		assert_snapshot!(object, @r#"
  tg.directory({
    "tangram.ts": tg.file({
      "contents": tg.leaf("import b from \"b\";\nimport c from \"c\";\n"),
      "dependencies": {
        "b": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf("import a from \"a/^1\";\nexport default tg.target(() => \"b\");\n"),
              "dependencies": {
                "a/^1": {
                  "item": tg.directory({
                    "tangram.ts": tg.file({
                      "contents": tg.leaf("export default tg.target(() => \"a/1.1.0\");\n"),
                    }),
                  }),
                  "subpath": "tangram.ts",
                  "tag": "a/1.1.0",
                },
              },
            }),
          }),
          "subpath": "tangram.ts",
          "tag": "b",
        },
        "c": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf("import a from \"a/^1.0\";\nexport default tg.target(() => \"c\");\n"),
              "dependencies": {
                "a/^1.0": {
                  "item": tg.directory({
                    "tangram.ts": tg.file({
                      "contents": tg.leaf("export default tg.target(() => \"a/1.1.0\");\n"),
                    }),
                  }),
                  "subpath": "tangram.ts",
                  "tag": "a/1.1.0",
                },
              },
            }),
          }),
          "subpath": "tangram.ts",
          "tag": "c",
        },
      },
    }),
  })
  "#);
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package_reproducible_checkin() {
	test(TG, move |context| async move {
		let mut context = context.lock().await;

		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

		// Tag the objects on the remote server.
		let tag = "foo";
		let artifact: temp::Artifact = temp::file!("foo").into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let output = remote_server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Create a local server.
		let local_server1 = context.spawn_server().await.unwrap();
		let output = local_server1
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(remote_server.url().to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Create a second local server.
		let local_server2 = context.spawn_server().await.unwrap();
		let output = local_server2
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(remote_server.url().to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Create an artifact.
		let artifact: temp::Artifact = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				import * as foo from "foo";
			"#),
		}
		.into();
		let path = "";
		let destructive = false;
		let tags = Vec::<(String, temp::Artifact)>::new();

		// Confirm the two outputs are the same.
		let (object_output1, _metadata_output1, _lockfile1) = test_artifact_checkin_inner(
			artifact.clone(),
			path,
			destructive,
			tags.clone(),
			&local_server1,
		)
		.await;
		let (object_output2, _metadata_output2, _lockfile2) =
			test_artifact_checkin_inner(artifact.clone(), path, destructive, tags, &local_server2)
				.await;
		assert_eq!(object_output1, object_output2);
	})
	.await;
}

#[tokio::test]
async fn tag_dependencies_after_clean() {
	test(TG, move |context| async move {
		let mut context = context.lock().await;

		// Create the first server.
		let server1 = context.spawn_server().await.unwrap();

		// Create the second server.
		let server2 = context.spawn_server().await.unwrap();
		let output = server2
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(server1.url().to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Publish the referent to server 1.
		let referent = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default tg.target(() => "foo")
			"#)
		};
		let artifact: temp::Artifact = referent.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let tag = "foo";
		let output = server1
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Checkin the referrer to server 2.
		let referrer = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				import foo from "foo";
				export default tg.target(() => foo())
			"#)
		};
		let path = "";
		let destructive = false;
		let tags = Vec::<(String, temp::Artifact)>::new();
		let (output1, _, _) =
			test_artifact_checkin_inner(referrer.clone(), path, destructive, tags, &server2).await;

		// // Clean up server 2.
		// server2.stop_gracefully().await;

		// Create the second server again.
		let server2 = context.spawn_server().await.unwrap();
		let output = server2
			.tg()
			.arg("remote")
			.arg("put")
			.arg("default")
			.arg(server1.url().to_string())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Checkin the artifact to server 2 again, this time the lockfile has been written to disk.
		let path = "";
		let destructive = false;
		let tags = Vec::<(String, temp::Artifact)>::new();
		let (output2, _, _) =
			test_artifact_checkin_inner(referrer, path, destructive, tags, &server2).await;

		// Confirm the outputs are the same.
		assert_eq!(output1, output2);
	})
	.await;
}

async fn test_artifact_checkin<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	destructive: bool,
	tags: Vec<(String, impl Into<temp::Artifact> + Send + 'static)>,
	assertions: F,
) where
	F: FnOnce(String, String, Option<tg::Lockfile>) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();
		let (object, metadata, lockfile) =
			test_artifact_checkin_inner(artifact, path, destructive, tags, &server).await;
		assertions(object, metadata, lockfile).await;
	})
	.await;
}

async fn test_artifact_checkin_inner(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: impl AsRef<Path>,
	destructive: bool,
	tags: Vec<(String, impl Into<temp::Artifact> + Send + 'static)>,
	server: &Server,
) -> (String, String, Option<tg::Lockfile>) {
	// Tag the objects.
	for (tag, artifact) in tags {
		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();

		// Tag the dependency
		let output = server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Write the artifact to a temp.
	let artifact: temp::Artifact = artifact.into();
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();

	let path = temp.path().join(path);

	// Check in.
	let mut command = server.tg();
	command.arg("checkin");
	command.arg(path.clone());
	if destructive {
		command.arg("--destructive");
	}
	let output = command.output().await.unwrap();
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

	// Get the metadata.
	let metadata_output = server
		.tg()
		.arg("object")
		.arg("metadata")
		.arg(id)
		.arg("--pretty")
		.arg("true")
		.output()
		.await
		.unwrap();
	assert_success!(metadata_output);

	// Get the lockfile if it exists.
	let lockfile = tokio::fs::read(path.join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))
		.unwrap();

	if destructive {
		// Confirm there is no file or directory at this path.
		assert!(!path.exists());
	}

	(
		std::str::from_utf8(&object_output.stdout).unwrap().into(),
		std::str::from_utf8(&metadata_output.stdout).unwrap().into(),
		lockfile,
	)
}
