use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::path::Path;
use tangram_cli::{
	assert_success,
	test::{Server, test},
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
		    "contents": tg.blob("Hello, world!"),
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
		  "count": 6,
		  "depth": 3,
		  "weight": 443
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		    "contents": tg.blob("Hello, world!"),
		  }),
		  "b.txt": tg.file({
		    "contents": tg.blob("Hello, world!"),
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 3,
		  "depth": 3,
		  "weight": 238
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		    "contents": tg.blob("Hello, World!"),
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 3,
		  "depth": 3,
		  "weight": 173
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		  "count": 2,
		  "depth": 2,
		  "weight": 95
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		    "contents": tg.blob("import \"../b/c/d"),
		    "dependencies": {
		      "../b/c/d": {
		        "item": tg.file({
		          "contents": tg.blob("hello, world!"),
		        }),
		      },
		    },
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 5,
		  "depth": 4,
		  "weight": 362
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
		      "id": "dir_0197w0ssyd8mm004avr9zv9p88n6255vfn2299hngz2875jvfzezcg"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01bq1z31cqje98dfcfj7vh758mj2d88vkqvxqgb4v7044dpwrztbwg"
		    }
		  ]
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		    "contents": tg.blob("import \"../b/c"),
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
		  "count": 4,
		  "depth": 3,
		  "weight": 285
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
		      "id": "dir_011a08s8wznfwhhytpsrjzzsvsjjrhe88rgey51x20bhwdazmrnvcg"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01ngmk7ypy3f53qk22sbanwacx9jp38q89q9fpgytpx3mxhbw6eyag"
		    }
		  ]
		}
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		  "count": 5,
		  "depth": 4,
		  "weight": 449
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
		      "id": "dir_01nsds8z80v4wjx797p7q61s2me6b52p3jyz3nt357m1aksz28zj4g"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01mw8x6y88phss0shrra3tyxc38zwszcdah69tc0wkw6fkg89jt49g",
		      "dependencies": {
		        "./b.tg.ts": {
		          "item": 0,
		          "subpath": "b.tg.ts"
		        }
		      },
		      "id": "fil_014pgr51yfg92zvcncdx9g1wh3brjgz2ksg5d5hvpb9wezyn7sgmwg"
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
		            "contents": tg.blob(""),
		          }),
		          "tangram.ts": 1,
		        },
		      },
		      {
		        "kind": "file",
		        "contents": tg.blob("import \"./b.tg.ts"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01qgbqaxsymejxmbpz7k37kvd68c7t1j2qkk3k79mbwj3epy9sjvhg"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "../bar": {
		          "item": 2,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01tmbdc5dgtbs0srawj90msja6j7qwtdyngnce6ebcxgfcs732f0wg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import * as bar from \"../bar\";"),
		    "dependencies": {
		      "../bar": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01kz3kxx0vx8ctg82c116rzyvme0zpaxdpgx10f96seeyspm9agym0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 2
		      },
		      "id": "dir_01mm87nayqg9dxv3szvmpkxqddsjjn745ge7wm6zgheehvj2sb9g60"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "../baz": {
		          "item": 3,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01h03gq3b5xaxp0sp61ggnjt1xm49k7kj9mpzv8tq531eg6k80cweg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 4
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
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
		      "id": "fil_01t6ar0n883y7h8caxqhr7rchkz35gcwptjxhhb91sg91gtpftk1xg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "bar": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob("\n\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t"),
		      "dependencies": {
		        "../baz": {
		          "item": tg.directory({
		            "tangram.ts": tg.file({
		              "contents": tg.blob(""),
		            }),
		          }),
		          "subpath": "tangram.ts",
		        },
		      },
		    }),
		  }),
		  "baz": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("\n\t\t\t\timport * as bar from \"./bar\";\n\t\t\t\timport * as baz from \"./baz\";\n\t\t\t"),
		    "dependencies": {
		      "./bar": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("\n\t\t\t\t\timport * as baz from \"../baz\";\n\t\t\t\t"),
		            "dependencies": {
		              "../baz": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob(""),
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
		            "contents": tg.blob(""),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"tangram.ts" => "export default tg.command(() => {})",
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
		      "id": "dir_01h8y52j5q8hcsd8qv4tt350jk34ajamj8pw6py6tkkhefae8wsfn0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01s87qfc3564bvf3zpyy5mgh4a1v7hcywwehq69kvthk3qqm7m9xr0"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("export default tg.command(() => {})"),
		  }),
		})
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01wt47fx7taeywhr5dwsjv6xxkxjb8p0wrfv01a9zkcedjepmq3pj0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 2
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 4
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "bar": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		  "foo": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		})
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01qc70hm2jn29hedvkwtcfdsefmbk6x9qq9aac52166ycyck660cd0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "mod.tg.ts": 2
		      },
		      "id": "dir_01bgkfmyyp4bx9xx6zfx7kb5na8jqkrds5qnd5fzfpyawdy93z0c9g"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_016zn3k3yxdsk5esgbedyzt441k8t2agz8a8m47c9d7br0kfa8acw0",
		      "dependencies": {
		        ".": {
		          "item": 0,
		          "subpath": "a"
		        }
		      },
		      "id": "fil_01qx5tar2byw72q1aqqvjrq5hs5q7er2q7t6v374zeedg2mq4arjxg"
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
		        "contents": tg.blob("import a from \".\";"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01qzxsvn86t9dwahfmxy52yk8bs167g4nzc8pcbqsmrbjxgbm8hcng"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "mod.tg.ts": 2,
		        "tangram.ts": 3
		      },
		      "id": "dir_01avgmn0evgzxfw2hwe7tg40k2gt3e5g7nh28cgegmzrr84jyvdrk0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01aq2gskg75gmmbjtjfbr7meynk5h439y6se0rvtqyjpa12we0abe0",
		      "dependencies": {
		        ".": {
		          "item": 1,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_015qh12pqh0x1f4tms0arq03qkw1nd9qgaskyws0ge77w82wjnyw90"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
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
		              "contents": tg.blob(""),
		            }),
		          },
		        },
		        {
		          "kind": "file",
		          "contents": tg.blob("import * as a from \".\";"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01hgj317y6x614ay2xyfrz7cz2eqdct4d8nr6qcr3c7c3mt073gnp0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01v0nnfyzd37t9vstptzn5zs2hchwf6w74dkfrn1yqejcz08rfxzz0",
		      "dependencies": {
		        "./a": {
		          "item": 0,
		          "subpath": "a"
		        }
		      },
		      "id": "fil_01wxs8zdq42ce9zxp1a37kfatzzgnaqw140c6zecgrrm2ja13t57b0"
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
		        "contents": tg.blob("import a from \"./a\""),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01bj526178y9vkbtb3v503kydk8nbv8ggektrwvmdzk727qt20q1f0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 2
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01mrk87qyd19txs3kh9xpy2rfw6fykawqy1hjg4ktfk9mg2j9w892g",
		      "dependencies": {
		        "./a": {
		          "item": 0,
		          "subpath": "a"
		        }
		      },
		      "id": "fil_01dfnzdj9heg75yqmhhsac6yjtbc9zx77f85m3kf2znvek2vqgkyqg"
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
		              "contents": tg.blob(""),
		            }),
		          }),
		          "tangram.ts": 1,
		        },
		      },
		      {
		        "kind": "file",
		        "contents": tg.blob("import a from \"./a\" with { type: \"directory\" }"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_012acxke3za771m8fqt927fcqe4swfk914mvnr5rjagax68f7e6rag"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 2
		      },
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "./a": {
		          "item": 1,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_017cm2mdr6ehceqcr0h7afb0y10yzbkmjh9pat1nrk2w9j80r8a64g"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"./a"),
		    "dependencies": {
		      "./a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01tqc1kjwrmayj2nva3p9gckxw5fhxc2z2fabg15a9nh1200v39en0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_010pwqd32ehjhaj9eswh61x95cgqby7x5w0fybj56a34cmbehs3mhg",
		      "dependencies": {
		        "./tangram.ts": {
		          "item": 0,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_012ck8szxb8edsgv9zqfc71wabe52x5xm60d2c2h1axnka04pscmk0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ng396txzk7v9ehpbjm36zkh7be839spckcv6z9d160p2frkya8h0",
		      "dependencies": {
		        "./foo.tg.ts": {
		          "item": 0,
		          "subpath": "foo.tg.ts"
		        }
		      },
		      "id": "fil_01yerx4dv4f33met4g6kr74ccmqc2dnmxwgbwkxmbc8gv5dx2fr5c0"
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
		        "contents": tg.blob("import * as foo from \"./foo.tg.ts\";"),
		        "dependencies": {
		          "./foo.tg.ts": {
		            "item": 0,
		            "subpath": "foo.tg.ts",
		          },
		        },
		      },
		      {
		        "kind": "file",
		        "contents": tg.blob("import * as root from \"./tangram.ts\";"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01cpa8tkxn4pg1n9wx5ey5mkss1gbkd6xn4amrdkmb6vx5x5ee8j0g"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_013jt3hdkvnhdr5apgc6chh92gzgrta5c7tvzj9ve83fyx15k0r820",
		      "dependencies": {
		        "../bar": {
		          "item": 2,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01s9dwzpkna94zn99xhx6z2pfv9fmy0z0k8d859hr5bapmj9nhmfeg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01hphc57438as9n79m4t4gddhb37mz8c9a9tff8t13qmrrhmxjymbg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01amvkrw427fngj2zwq9cvy6s78zkg1dkqa84ypfdkp8n7hb7zyyd0",
		      "dependencies": {
		        "../foo": {
		          "item": 0,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_012e8ed9yp5pyrv5pvea6jjdj00ng8ysw764vh4b8jdn622nnc0ekg"
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
		        "contents": tg.blob("import * as bar from \"../bar\""),
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
		        "contents": tg.blob("import * as foo from \"../foo\""),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		        "contents": tg.blob(""),
		      }),
		    }),
		  }),
		})
		"#);
	};
	let destructive = true;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		    "contents": tg.blob(""),
		  }),
		})
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		})
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01sftd1y3rkt16rne7bm02m6ybxbectev4t71x302vweh2zb4jsfz0"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01qgz0w7ntzvac2mbfpz5355d58vn920fb5hsm3qpdmc5206z5xazg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		})
		"#);
	};
	let destructive = false;
	let tags = Vec::<(String, temp::Artifact)>::new();
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01wsw1m54syvnt2avy89830ntvzqx9wz58279pvv7eg13s1vw22sc0"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "hello-world": {
		          "item": "fil_01b64fk2r3af0mp8wek1630m1k57bq8fqp0yvqjq7701b3tngbfyxg"
		        }
		      },
		      "id": "fil_0148zf44vq48d8h8r4zpcdyy9wbt204hr5b2rer237z3qc6968727g"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import hello from \"hello-world\""),
		    "dependencies": {
		      "hello-world": {
		        "item": tg.file({
		          "contents": tg.blob("Hello, world!"),
		        }),
		      },
		    },
		  }),
		})
		"#);
	};
	let destructive = false;
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package() {
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default tg.command(() => "a");
			"#),
		},
	)];
	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
			export default tg.command(async () => {
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
		      "id": "dir_01nmxpd9pn574eb44944s45cxpsatwt70701xn1sbtyejd8jgyd4rg"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "a": {
		          "item": 2,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01dg1e8v987kdrjxr40r1natxf2qsvsggn5yqjgp4a4sy4a1s113rg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01cgwvgsd81m77snb9t4fe88x1a1r00ch1v7embvar5qxm72c0h9ng"
		    },
		    {
		      "kind": "file",
		      "id": "fil_01eypdhmb5v4wcjxyk14naaq9bvw09rp84s1amn5q30jv6b324a0fg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"a\";\nexport default tg.command(async () => {\n\treturn await a();\n});\n"),
		    "dependencies": {
		      "a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("export default tg.command(() => \"a\");\n"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01nz4bm8spqvg09gzsgeyt46b166p4csdwe3hbyrcdr0wb3a8gwbcg"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "a": {
		          "item": 2,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01ejrwby3fgbx9k76rtbwsz167n74206gq38tb2j3yy3x40eawyf1g"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 3,
		        "tangram.ts": 4
		      },
		      "id": "dir_015dn19rejjy4r5dww8whnv75kfnj38s6ybbxh615wvwfb3bfzfkp0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ycqx996y57ta8qpg72zsn6g446x37htxw7v3se7xmaa5nwrwx2t0",
		      "dependencies": {
		        "./tangram.ts": {
		          "item": 2,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01a0k08fsy5qwty2d9xfgkpar0zyfxx19rp64xwehfw40st7s0m2s0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_018dz0kathbh9mktnftc933pd35gy651y1ppqe3tg2q0an0dt35zr0",
		      "dependencies": {
		        "./foo.tg.ts": {
		          "item": 2,
		          "subpath": "foo.tg.ts"
		        }
		      },
		      "id": "fil_01g8ap6qxg0n1n524hzymzyrfhm1mg6vxhr5w6xvwr7ctx155tabkg"
		    }
		  ]
		}
		"#);
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"a\";\n"),
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
		                "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
		                "dependencies": {
		                  "./foo.tg.ts": {
		                    "item": 0,
		                    "subpath": "foo.tg.ts",
		                  },
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as a from \"./tangram.ts\";\n"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
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
		      "id": "dir_01m4temjzxy0yzktybwhdstn8rqqzs91e2ew8cdga8m158ry9ng690"
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
		      "id": "fil_01mh33deh74chns7bb5wk31ma1pt4expdj7s2tdbx471ztsnwxtg50"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01v78snf298jjg097akacyrqrndbz6y352m4ezn8rmdj9xqnn6cq0g"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_0158re2012fvbq8s0zxgsdmkmg7k05y79mnbeha500h9k973hk06k0",
		      "dependencies": {
		        "b/*": {
		          "item": 4,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_016vv3ja5e1fzvt0t2bbmsm7v038fn810kyz4d0f6zmrs02zjyzv10"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 5,
		        "tangram.ts": 6
		      },
		      "id": "dir_019xvb81v5nhw4fdvngdakk2k6fmqdhfgvjrsfcb9jgfnc2sqqxhcg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01mv4a5380n5nacg4cvgh1r1f0vcrk489j6rfsj031gxp9b8t9gxq0",
		      "dependencies": {
		        "./tangram.ts": {
		          "item": 4,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01pxh9a2q3aq8tzypmx61n0zhqpp65ec92d6shre3qfqe2yqydv7s0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ajr136dx93ph4zrx9eqbq5gz05gh530ew34qzh0dgh4jbvvx6m30",
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
		      "id": "fil_010qhs4kjtjxxwrdy5w5y7rmefpmz8c3pv41rfw22w82v2ej2ryfn0"
		    }
		  ]
		}
		"#); // Keep existing snapshot
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import * as b from \"b/*\";\nimport * as a from \"a/*\";\n"),
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
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
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
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
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
		                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
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
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
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
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
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
		                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn diamond_dependency() {
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default tg.command(() => "a/1.0.0");
				"#),
			},
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default tg.command(() => "a/1.1.0");
				"#),
			},
		),
		(
			"b".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1";
					export default tg.command(() => "b");
				"#),
			},
		),
		(
			"c".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1.0";
					export default tg.command(() => "c");
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
		      "id": "dir_01n5ttvstnfxpp59kfjsvtm0bhbjp5epc3gv5bghqwgtxna2fhtqgg"
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
		      "id": "fil_0123b879gp12kv8xbpjqdvrzdz6s55422vchyxcn31bcghhmzvn2fg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_017t4rek8zyvydm88p55h5xvk6wt0hrqns2hx69asfb4d6sdcqtw80"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "a/^1": {
		          "item": 4,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_014rw9rrghzanqpkr9h1mzhmnqg0rdp1kkde6m27ehkv57hsgxxka0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 5
		      },
		      "id": "dir_01phy4jtxvda54b3y3akqe7e8tdffvtj0brtcf5kby295p2zq5wbag"
		    },
		    {
		      "kind": "file",
		      "id": "fil_015aa81e0d8agetgp8955q1x1kwnsyp2rcsj7rcrpx51cx5dh9ctrg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 7
		      },
		      "id": "dir_01q7xpp3fddemw4krb227nart0c77w7v61gxfsemwra7p8sp83myy0"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "a/^1.0": {
		          "item": 4,
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01y20pberbjrxefsxkyhm319qzxpgtksg7xaqrp0f0j3z38rdb5c2g"
		    }
		  ]
		}
		"#); // Keep existing snapshot
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import b from \"b\";\nimport c from \"c\";\n"),
		    "dependencies": {
		      "b": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("import a from \"a/^1\";\nexport default tg.command(() => \"b\");\n"),
		            "dependencies": {
		              "a/^1": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("export default tg.command(() => \"a/1.1.0\");\n"),
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
		            "contents": tg.blob("import a from \"a/^1.0\";\nexport default tg.command(() => \"c\");\n"),
		            "dependencies": {
		              "a/^1.0": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("export default tg.command(() => \"a/1.1.0\");\n"),
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
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package_reproducible_checkin() {
	test(TG, async move |context| {
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
		let (object_output1, _metadata_output1, _lockfile1) = test_checkin_inner(
			artifact.clone(),
			path,
			destructive,
			tags.clone(),
			&local_server1,
		)
		.await;
		let (object_output2, _metadata_output2, _lockfile2) =
			test_checkin_inner(artifact.clone(), path, destructive, tags, &local_server2).await;
		assert_eq!(object_output1, object_output2);
	})
	.await;
}

#[tokio::test]
async fn tag_dependencies_after_clean() {
	test(TG, async move |context| {
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
				export default tg.command(() => "foo")
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
				export default tg.command(() => foo())
			"#)
		};
		let path = "";
		let destructive = false;
		let tags = Vec::<(String, temp::Artifact)>::new();
		let (output1, _, _) =
			test_checkin_inner(referrer.clone(), path, destructive, tags, &server2).await;

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
		let (output2, _, _) = test_checkin_inner(referrer, path, destructive, tags, &server2).await;

		// Confirm the outputs are the same.
		assert_eq!(output1, output2);
	})
	.await;
}

async fn test_checkin<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	destructive: bool,
	tags: Vec<(String, impl Into<temp::Artifact> + Send + 'static)>,
	assertions: F,
) where
	F: FnOnce(String, String, Option<tg::Lockfile>) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();
		let (object, metadata, lockfile) =
			test_checkin_inner(artifact, path, destructive, tags, &server).await;
		assertions(object, metadata, lockfile).await;
	})
	.await;
}

async fn test_checkin_inner(
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

	// Index
	{
		let mut command = server.tg();
		command.arg("index");
		let index_output = command.output().await.unwrap();
		assert_success!(index_output);
	}

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

	(
		std::str::from_utf8(&object_output.stdout).unwrap().into(),
		std::str::from_utf8(&metadata_output.stdout).unwrap().into(),
		lockfile,
	)
}
