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
async fn directory_with_duplicate_entries() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a.txt" => "Hello, world!",
	  "b.txt" => "Hello, world!",
		}
	};
	let path = "directory";
	let assertions = |object: String, metadata: String, _| async move {
		assert_snapshot!(object, @r#"
  tg.directory({
    "a.txt": tg.file({
      "contents": tg.leaf("Hello, world!"),
    }),
    "b.txt": tg.file({
      "contents": tg.leaf("Hello, world!"),
    }),
  })
  "#);
		assert_snapshot!(metadata, @r#"
  {
    "complete": true,
    "count": 3,
    "depth": 3,
    "weight": 237
  }
  "#)
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
    "weight": 360
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
        "id": "dir_01sfxc12kdhkczx7hpf7w9qmvzdx2kskhp1jqr4rky56bh3g3bgsxg"
      },
      {
        "kind": "file",
        "id": "fil_014bbykn7emmm5d5ade8najw6hrmnwc9at3en7wvdaahx3q5vh05ng"
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
    "weight": 284
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
        "id": "dir_01ehyscz6b09rfgrgaa0kprmy5wyb9np0c0h97ktd9nw135tpshywg"
      },
      {
        "kind": "file",
        "id": "fil_017n95dt2m8rkr70nmh2e19cgm2hjgxvgxeb1zhc0qh6r8zv66qs30"
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
    "count": 5,
    "depth": 4,
    "weight": 447
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
        "id": "dir_01zhsfa0g7bvxqr8n1r5c3p6kq6wtj1t08w063mm2atts7c69z72x0"
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
            "subpath": "b.tg.ts"
          }
        },
        "id": "fil_01defcetb2r1e5vpb9j4xvsxyx1c20tvbr34f27rasqhygsjkx267g"
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
        "id": "dir_01psn7dzpkyhg2ggakcs4z5t7f70dankg4jys8d8nb457r7rg2r8gg"
      },
      {
        "kind": "file",
        "dependencies": {
          "../bar": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_0164x40zgnt4ewam0t3j8rq0amv4bzcyvbqnrjd78g03pttveac7s0"
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
        "id": "dir_0155r2egv2659mhcav4v10cyayasbmwx3wceyatjnm5acaytax1wd0"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 2
        },
        "id": "dir_01p51hdbwcw97c29cqbwtjasnrm0z9s4jt7hhr3ncvyx2xye0kk41g"
      },
      {
        "kind": "file",
        "dependencies": {
          "../baz": {
            "item": 3,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01ech8fmtth7hmw2dprwanfax0wmra003r59taqfx65zmsqxt3qet0"
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
            "subpath": "tangram.ts"
          },
          "./baz": {
            "item": 3,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_019zqh56dswrf2d02czkxq6ne7axpkzn5n1mn3bynbp05n59c41nr0"
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
                  "subpath": "tangram.ts",
                },
              },
            }),
          }),
          "subpath": "tangram.ts",
        },
        "./baz": {
          "item": tg.directory({
            "tangram.ts": tg.file({
              "contents": tg.leaf(""),
            }),
          }),
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
        "id": "dir_01c8xhsrz9pc3yd0nwtwv18xspk5c5xm86ff1rdxnqksh6t4tv6v20"
      },
      {
        "kind": "directory",
        "entries": {
          "mod.tg.ts": 2
        },
        "id": "dir_011z159ekep5rv6evxq85n733naswpa37np1hpfkqc8xaqp1g7hbxg"
      },
      {
        "kind": "file",
        "contents": "lef_013zybf1ec34vd94gv58eqsje8jctv68qy169rz9sdqrcb9kpmargg",
        "dependencies": {
          ".": {
            "item": 0,
            "subpath": "a"
          }
        },
        "id": "fil_01psm3mjtpk3nxf54dfzhbagr6ns2c7brge4nz44p3sdx4z5178rng"
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
        "id": "dir_01r2s046gvyrhjhdy6zmfdyn9wwjf15wv4669q6km5mg5kcz8nax30"
      },
      {
        "kind": "directory",
        "entries": {
          "mod.tg.ts": 2,
          "tangram.ts": 3
        },
        "id": "dir_01qcsa7yt6f796185awafrdpx8vrcan1eygtpbnxjbep9wzjcw30cg"
      },
      {
        "kind": "file",
        "contents": "lef_01f5c3vv1z4ejbnxc9nza26gecndwkt8n7jpbm5hw1gx9yega1y150",
        "dependencies": {
          ".": {
            "item": 1,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_0198p772d5g6wpps3v01688qcmd4k8jq7z9sex2gt46xk1azvnmj9g"
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
        "id": "dir_01ztyjmrmb6fz29g7xfwpgy0jh2fs9vvsaa9dgyp77r4dww3zx3xe0"
      },
      {
        "kind": "file",
        "contents": "lef_01547n1jzmegft5pxdjhfxjmkttsdh085eec8v22key5vrtzqskwy0",
        "dependencies": {
          "./a": {
            "item": 0,
            "subpath": "a"
          }
        },
        "id": "fil_01ghq54ws34wg7ae7v4vrds7h5ywcn16avwggze5k110pvbz4by470"
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
        "id": "dir_0183amfbapjebd89tc4d7ke2pgatrnfk87889gn88fwcwjqa5eqwrg"
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
            "subpath": "a"
          }
        },
        "id": "fil_01ph5k4075ep1nkpdkhs2pjn2kf8mn9tp6cb826cfwtng6n9hph6yg"
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
        "id": "dir_01ma8vacbss7qbgwwg2pj38g221vhseb7e84xbb9kn5pj6m7j7k5z0"
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
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_016rt7bd69x17ghh6skjcsxx7yqn2v8dyd3dqp197t9h03fmcxmha0"
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
        "id": "dir_01hn0fxkkyf3hgthdb9179cy1pdm0t0kvr34f9f3k8tkk2ws01wj60"
      },
      {
        "kind": "file",
        "contents": "lef_01wz1kgzch869nmx5q4pq7ka0vjszxqa4nj39bgjgm2hpxwem2jdxg",
        "dependencies": {
          "./tangram.ts": {
            "item": 0,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_017n9eky25y855zdndx9a8sggspg12z7sredj7vehq3pjm2t8gsttg"
      },
      {
        "kind": "file",
        "contents": "lef_01a2nf5j3bh75f7g1nntakjjtv6h3k0h7aykjstpyzamks4sebyz2g",
        "dependencies": {
          "./foo.tg.ts": {
            "item": 0,
            "subpath": "foo.tg.ts"
          }
        },
        "id": "fil_01jk5px81jfd00vw4kazpzhhm1q660egdtr1xbx9c07s2qb2jgy1jg"
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
        "id": "dir_018x18xx2b9r8mptxqh1hk59fwv1cjxz1wrx0389cj4m9fj2tzw1p0"
      },
      {
        "kind": "file",
        "contents": "lef_01pqttaksgrf3n76tqrrhb6c96tyafzhrex2jgy54ht8419s6wpg2g",
        "dependencies": {
          "../bar": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01qz8cvmh02zvmw8m6sd54nng7td125gskaws5fetjp74fzqkat190"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01cysy22404xh7ee1179kp980vtqks3azbhs73j442rjry91nwb3d0"
      },
      {
        "kind": "file",
        "contents": "lef_01fnhktwqxcgtzkra7arsx7d50rgmaycmnqxhrt58s0yb9xkg5ydjg",
        "dependencies": {
          "../foo": {
            "item": 0,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01b6vsf5tdd8ckqjqa541wnavrvkepqnwy11phpkgxtemct51j8whg"
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
        "id": "dir_0103qcdk4tgcahkn262qeazkjrqx4pv4488r25e3w72pvf0a5mw1t0"
      },
      {
        "kind": "file",
        "dependencies": {
          "hello-world": {
            "item": "fil_01yxtf8s9sxc1dcv6vs0zjxhra1xp11j97h485cjhmtwa4mrrzbrag"
          }
        },
        "id": "fil_012frh90xgpyb6k8m8dv2e7w106zjfd7t8b5ewk8bdqc6764szp2rg"
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
        "id": "dir_013vnxmkz1dapnw2hyac4tn9r64tzsa8gmyttnrr9jm347sp4stkvg"
      },
      {
        "kind": "file",
        "dependencies": {
          "a": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_0153rytwzqkn19xp6z9ra5nmn3cwjenhfn18gztnt8vemtn74fq0tg"
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
        "id": "dir_016av7j3e2aafe9wht7qqgjb8vz73cc47yrkrnnp2bpfzwkwtrwtdg"
      },
      {
        "kind": "file",
        "dependencies": {
          "a": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01byhrjjy63jr2mdbzt2jrj6bfgnyf6v1bcfzxcaqwny6tq3hd0790"
      },
      {
        "kind": "directory",
        "entries": {
          "foo.tg.ts": 3,
          "tangram.ts": 4
        },
        "id": "dir_01yan0xrn062gmr28vd4msp8cj5zgkn031v6essz4z6pxs0n7rrpk0"
      },
      {
        "kind": "file",
        "contents": "lef_01mn2bw5f7w51jk66tjtvh9114zrdnx7saptmrark0jeafebh9yn6g",
        "dependencies": {
          "./tangram.ts": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01d5p4xxk27g0q1s47t0n8ypn0j5rhgrdt04s4djqey6esjrcmccr0"
      },
      {
        "kind": "file",
        "contents": "lef_015n08n4fm2ves89zvbfqxnej2byq4bacb1qszgy73f321haskq7z0",
        "dependencies": {
          "./foo.tg.ts": {
            "item": 2,
            "subpath": "foo.tg.ts"
          }
        },
        "id": "fil_01f466tpp902t4e9gfb1hn5z0ktgmdvefds7v9hdkcx0wnt711phqg"
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
                    },
                  },
                },
              ],
            }),
            "node": 0,
          }),
          "subpath": "tangram.ts",
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
        "id": "dir_01nztcqdxcka49vket83atb0mhrz9psjhwqmvrqbzy4mzwyngqt4y0"
      },
      {
        "kind": "file",
        "dependencies": {
          "a/*": {
            "item": 2,
            "subpath": "tangram.ts"
          },
          "b/*": {
            "item": 4,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01vt30yxcge5agq8rk78vs5kystffnsca8cqzrs6rxg6gyx77t3180"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01sqj2r5c0559hw3wpxr5cx4ehyyedxeg06s3dybsayytte4hypsy0"
      },
      {
        "kind": "file",
        "contents": "lef_015qtah5hxy64eyx8vqccxt04pwnm87j851w784yr4nq7rjrmgtts0",
        "dependencies": {
          "b/*": {
            "item": 4,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_011ny4hdpj5qztf8kxscz6jrhc2s86wvgp9zadxn58hc5y5a43ytf0"
      },
      {
        "kind": "directory",
        "entries": {
          "foo.tg.ts": 5,
          "tangram.ts": 6
        },
        "id": "dir_01xh02ecmm5y1bdd6cdf832gq3pgv01p9dzq6fryy0y851y8cxkh40"
      },
      {
        "kind": "file",
        "contents": "lef_016bmyhqk81jgbkns2m3w5yke2h9dadqt4r60mnkcpedev96hha9j0",
        "dependencies": {
          "./tangram.ts": {
            "item": 4,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01tmwyfdzczwa675jkv70tw2sa5sppe8kp6hcz775a1zszvzvh4b30"
      },
      {
        "kind": "file",
        "contents": "lef_01gj0fndzaa1p7tch03pvja4ne3nhv0rwewcvm9w4v2k1xwckacwy0",
        "dependencies": {
          "./foo.tg.ts": {
            "item": 4,
            "subpath": "foo.tg.ts"
          },
          "a/*": {
            "item": 2,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_011g5r7dk2z0cebm0b10p8p47mm7jk0an9h1jedv3wg4ma937qg0dg"
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
                    },
                    "a/*": {
                      "item": 0,
                      "subpath": "tangram.ts",
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
                    },
                  },
                },
              ],
            }),
            "node": 0,
          }),
          "subpath": "tangram.ts",
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
                    },
                    "a/*": {
                      "item": 0,
                      "subpath": "tangram.ts",
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
                    },
                  },
                },
              ],
            }),
            "node": 2,
          }),
          "subpath": "tangram.ts",
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
        "id": "dir_01xzz36tpddatx0wxj8g8p4hpcsv59cp65b1fmzt83pddq2pc8wh2g"
      },
      {
        "kind": "file",
        "dependencies": {
          "b": {
            "item": 2,
            "subpath": "tangram.ts"
          },
          "c": {
            "item": 6,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01770014mc5bvqfzm7p5hzhgbfjqedzj41qmeaynfyjwz4q8dkratg"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 3
        },
        "id": "dir_01zexwa4d73rs0epg079txfyb9nhtatcbptrrd9r8mn0na8dg3bpwg"
      },
      {
        "kind": "file",
        "dependencies": {
          "a/^1": {
            "item": 4,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_01y20v44namqtqq5w4bq33qv6vsxg3nnz12sy8z01tc54t6v3jjrv0"
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
        "id": "fil_016cwcgvz23avmszqk01t8kh9j7aaaw8y43s8q28v30zgpq276c43g"
      },
      {
        "kind": "directory",
        "entries": {
          "tangram.ts": 7
        },
        "id": "dir_019tq93a76yfhge32wb4mma4zwkfy5b50sjn94pb10rhg1h7xm7wmg"
      },
      {
        "kind": "file",
        "dependencies": {
          "a/^1.0": {
            "item": 4,
            "subpath": "tangram.ts"
          }
        },
        "id": "fil_018tgmkgbajfrkpfq1ybx7jy669vf0bh9381gb555txy0y1mjdk0r0"
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
                },
              },
            }),
          }),
          "subpath": "tangram.ts",
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
                },
              },
            }),
          }),
          "subpath": "tangram.ts",
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
