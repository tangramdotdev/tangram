use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::{collections::BTreeMap, future::Future, path::Path};
use tangram_cli::{
	assert_output_success,
	test::{test, Server},
};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() -> tg::Result<()> {
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
		Ok::<_, tg::Error>(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
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
		Ok::<_, tg::Error>(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
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
		Ok::<_, tg::Error>(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn file_through_symlink() -> tg::Result<()> {
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
		assert_json_snapshot!(lockfile, @r#""#);
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn artifact_symlink() -> tg::Result<()> {
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
                "count": 5,
                "depth": 4,
                "weight": 378
            }
            "#);
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(lockfile, @r#""#);
		Ok::<_, tg::Error>(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn lockfile_out_of_date() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn simple_path_dependency() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn package_with_nested_dependencies() -> tg::Result<()> {
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
		assert_snapshot!(object, @r#"
tg.directory({
    "bar": tg.directory({
        "tangram.ts": tg.file({
            "contents": tg.leaf("\n                    import * as baz from \"../baz\";\n                "),
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
        "contents": tg.leaf("\n                import * as bar from \"./bar\";\n                import * as baz from \"./baz\";\n            "),
        "dependencies": {
            "./bar": {
                "item": tg.directory({
                    "tangram.ts": tg.file({
                        "contents": tg.leaf("\n                    import * as baz from \"../baz\";\n                "),
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn package() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn directory_with_nested_packages() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

// THIS IS WRONG? WHAT
#[tokio::test]
async fn import_directory_from_current() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn import_package_from_current() -> tg::Result<()> {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory! {
				"mod.tg.ts" => r#"import * as a from ".";"#,
				"tangram.ts" => r#""#
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn import_directory_from_parent() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn import_package_with_type_directory_from_parent() -> tg::Result<()> {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => r#""#,
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn import_package_from_parent() -> tg::Result<()> {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => r#""#,
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn package_with_cyclic_modules() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn cyclic_dependencies() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn directory_destructive() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = true;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn package_destructive() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = true;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn default_ignore() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn missing_in_lockfile() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn invalid_lockfile() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn tagged_object() -> tg::Result<()> {
	let tags = vec![("hello-world".into(), temp::file!("Hello, world!"))];
	let directory = temp::directory! {
		"tangram.ts" => r#"import hello from "hello-world""#,
	};
	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		let lockfile = lockfile.expect("expected a lockfile");
		assert_json_snapshot!(&lockfile, @r#""#);
		assert_snapshot!(object, @r#""#);
		Ok(())
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn tagged_package() -> tg::Result<()> {
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
		assert_json_snapshot!(&lockfile, @r#""#);
		assert_snapshot!(object, @r#""#);
		Ok(())
	};
	let destructive = false;
	let path = "";
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn tagged_package_with_cyclic_dependency() -> tg::Result<()> {
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
		Ok(())
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn tag_dependency_cycles() -> tg::Result<()> {
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
		assert_json_snapshot!(&lockfile, @r#"..."#); // Keep existing snapshot
		assert_snapshot!(object, @r#"..."#); // Keep existing snapshot
		Ok(())
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn diamond_dependency() -> tg::Result<()> {
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
                    // a/tangram.ts
                    export default tg.target(() => "a/1.0.0");
                "#),
			},
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
                    // a/tangram.ts
                    export default tg.target(() => "a/1.1.0");
                "#),
			},
		),
		(
			"b".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
                    // b/tangram.ts
                    import a from "a/^1";
                    export default tg.target(() => "b");
                "#),
			},
		),
		(
			"c".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
                    // c/tangram.ts
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
		assert_json_snapshot!(&lockfile, @r#"..."#); // Keep existing snapshot
		assert_snapshot!(object, @r#"..."#); // Keep existing snapshot
		Ok(())
	};
	let destructive = false;
	test_artifact_checkin(directory, path, destructive, tags, assertions).await
}

#[tokio::test]
async fn tagged_package_reproducible_checkin() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;

		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

		// Tag the objects on the remote server.
		let tag = "foo";
		let artifact: temp::Artifact = temp::file!("foo").into();
		let temp = Temp::new();
		artifact.to_path(&temp.as_ref()).await.unwrap();
		let output = remote_server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);

		// Create a local server.
		let config = tangram_cli::Config {
			remotes: Some(Some(BTreeMap::from([(
				"default".to_owned(),
				Some(tangram_cli::config::Remote {
					url: remote_server.url().clone(),
				}),
			)]))),
			..Default::default()
		};
		let local_server1 = context.spawn_server_with_config(config).await.unwrap();

		// Create a second local server.
		let config = tangram_cli::Config {
			remotes: Some(Some(BTreeMap::from([(
				"default".to_owned(),
				Some(tangram_cli::config::Remote {
					url: remote_server.url().clone(),
				}),
			)]))),
			..Default::default()
		};
		let local_server2 = context.spawn_server_with_config(config).await.unwrap();

		// Create an artifact.
		let artifact: temp::Artifact = temp::directory! {
			"tangram.ts" => indoc!(r#"
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
	Ok(())
}

#[tokio::test]
async fn tag_dependencies_after_clean() -> tg::Result<()> {
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		// Create the first server.
		let temp1 = Temp::new();
		let config = tangram_cli::Config {
			..Default::default()
		};
		let server1 = context
			.spawn_server_with_temp_and_config(temp1, config)
			.await
			.unwrap();

		// Create the second server.
		let temp2 = Temp::new();
		let config = tangram_cli::Config {
			remotes: Some(Some(BTreeMap::from([(
				"default".to_owned(),
				Some(tangram_cli::config::Remote {
					url: server1.url().clone(),
				}),
			)]))),
			..Default::default()
		};
		let server2 = context
			.spawn_server_with_temp_and_config(temp2, config)
			.await
			.unwrap();

		// Publish the referent to server 1.
		let referent = temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default tg.target(() => "foo")
			"#)
		};
		let artifact: temp::Artifact = referent.into();
		let temp = Temp::new();
		artifact.to_path(&temp.as_ref()).await.unwrap();
		let tag = "foo";
		let output = server1
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);

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

		// Clean up server 2.

		// Create the second server again.
		let temp2 = Temp::new();
		let config = tangram_cli::Config {
			remotes: Some(Some(BTreeMap::from([(
				"default".to_owned(),
				Some(tangram_cli::config::Remote {
					url: server1.url().clone(),
				}),
			)]))),
			..Default::default()
		};
		let server2 = context
			.spawn_server_with_temp_and_config(temp2, config)
			.await
			.unwrap();

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
	Ok(())
}

async fn test_artifact_checkin<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	destructive: bool,
	tags: Vec<(String, impl Into<temp::Artifact> + Send + 'static)>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(String, String, Option<tg::Lockfile>) -> Fut + Send + 'static,
	Fut: Future<Output = tg::Result<()>> + Send,
{
	test(TG, move |context| async move {
		let mut context = context.lock().await;
		let server = context.spawn_server().await.unwrap();
		let (object, metadata, lockfile) =
			test_artifact_checkin_inner(artifact, path, destructive, tags, &server).await;
		assertions(object, metadata, lockfile).await.unwrap();
	})
	.await;
	Ok(())
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
		artifact.to_path(&temp.as_ref()).await.unwrap();

		// Tag the dependency
		let output = server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
	}

	// Write the artifact to a temp.
	let artifact: temp::Artifact = artifact.into();
	let temp = Temp::new();
	artifact.to_path(&temp.as_ref()).await.unwrap();

	let path = temp.path().join(path);

	// Check in.
	let mut cmd = server.tg();
	cmd.arg("checkin").arg(path.clone());
	if destructive {
		cmd.arg("--destructive");
	}
	let output = cmd.output().await.unwrap();
	assert_output_success!(output);

	// Get the object.
	let id = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();
	let object_output = server
		.tg()
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
	assert_output_success!(object_output);

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
	assert_output_success!(metadata_output);

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
