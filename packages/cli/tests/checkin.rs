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
async fn single_file() {
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
		        "path": "../b/e/d",
		      },
		    },
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 5,
		  "depth": 4,
		  "weight": 380
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
				      "id": "dir_010005phrqxhzc2tjaqre4jab3nfjets3n0xq427pv8ks8q45p3n2g"
				    },
				    {
				      "kind": "file",
				      "id": "fil_01bekxaqs9w8vsycr287w5g53d6ts390x5ghrjrp7ce0e3xh563f8g"
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
		        "path": "../b/c",
		      },
		    },
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
				{
				  "count": 4,
				  "depth": 3,
				  "weight": 301
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
		      "id": "dir_01hg130caj00ny9p7j7bsqg2evf8qkzykg3g250vmwcwxvwjnqvj20"
		    },
		    {
		      "kind": "file",
		      "id": "fil_0174w6skgjpk15cfhgymzzc6qawde3m3c0ken1424sp7xgdbs3mhtg"
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
		  "count": null,
		  "depth": null,
		  "weight": null
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
		      "id": "dir_01r3x6dfwxa5b6m8d42xj1h805671qq0nsxy60r5yakv0b4xq763h0"
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
		      "id": "fil_01e9ew2j9q6jnjdh3wnv4saj91ahc8p4j2tr57z6e93dfptt5mkjs0"
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
		      "id": "dir_0135k9hrn6ex25j5v6g0hab63w7ywjrrr867jncm62rdqqhb2sgc4g"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 2
		      },
		      "id": "dir_01g5nfn3z3pv889xzradap3k9hcjak1pe41e8rpjh0d6hwe6zweje0"
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
		      "id": "fil_011zas3hzxy8zhgjd6em89084nwsav6996fafptayb7mwc0hspnqhg"
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
		          "path": "bar",
		          "subpath": "tangram.ts"
		        },
		        "./baz": {
		          "item": 3,
		          "path": "baz",
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01sw07a9xgp8tds89629r8dw8aqw6z6skgc65jd45hj21q2n1ae9s0"
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
		          "path": "baz",
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
		            "contents": tg.blob(""),
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
		      "id": "dir_01tefnm7k9mybxjpv7xcdtrpjnzzvg3vd175ajpvfyrhakvhfme2r0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "mod.tg.ts": 2,
		        "tangram.ts": 3
		      },
		      "id": "dir_01t7yv16xc2xtxz4gbv7428b9tyeayb8b3fypjdcxa6c3kghm69e5g"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01aq2gskg75gmmbjtjfbr7meynk5h439y6se0rvtqyjpa12we0abe0",
		      "dependencies": {
		        ".": {
		          "item": 1,
		          "path": "a",
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01dqx0ka1z6s4aenvggnx9zjx2mrsd2d4b55cjvnsvy1gzrk03rqs0"
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
		      "id": "dir_01sskpnxd9bqwxt7h9qme56epbgyq4gfrwsvmydy9tzqfy705njrf0"
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
		          "path": "a",
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_014v8qvm7e51ghtkmc1z3ev5j94fjnxam2ya6qx7q45xpsnjd1devg"
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
		    "contents": tg.blob("import a from \"./a\" with { type: \"directory\" }"),
		    "dependencies": {
		      "./a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
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
		      "id": "dir_01z7vpew7gpj9tnzde3faw6yny1zdnx5fmbjaqdvjdp423dmydygy0"
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
		          "path": "a",
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01rp5cf460yqdwy91rkm31pz25awp6wn30j41mwpqfqdgmwvtbpnx0"
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
		      "id": "dir_01z0xx74bhkp20h5h838y04ep1y86w5wq8vpmczq9hbjkcrj38mr6g"
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
		      "id": "fil_01gps075nn9g9kgzneh4haatcyx039g60xrzjrprptf63xzyg8zfp0"
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
		      "id": "fil_018a6y6fz0npdydzfwp86g3n6xbmnchyb2jr9g640nctf1cbqpfxfg"
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
		          "foo.tg.ts": 1,
		          "tangram.ts": 2,
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
		      "id": "dir_01aeyem407yerjtkvxb76h3cm3jnn1asn7bh0z7a5h90twggkf8vc0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_013jt3hdkvnhdr5apgc6chh92gzgrta5c7tvzj9ve83fyx15k0r820",
		      "dependencies": {
		        "../bar": {
		          "item": 2,
		          "path": "../bar",
		          "subpath": "tangram.ts"
		        }
		      },
		      "id": "fil_01d6dav4c760dt9rgsbw2t11x07dktnbvhkgymgjzm5g5yfz57qapg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01pbbtvgc7ngyvxtp78fed44qt3znbzf6pgedaa17t2wvek2rwwvr0"
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
		      "id": "fil_01x6ajp8q92dq8nzrmmcxms3q7pjv3d12z12wg1shp1myv1gvw0m10"
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
		      "id": "dir_013cddxyakamjszdm8efsnw2p65rfxrmy3pcrtc3yc2fzpdp5tj1fg"
		    },
		    {
		      "kind": "file",
		      "dependencies": {
		        "hello-world": {
		          "item": 2,
		          "tag": "hello-world"
		        }
		      },
		      "id": "fil_01vm0ea1b00hek3haz99vpkdy93zj4a0rdcmaa0vnnk54dfpk3w9bg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ad8gw7fez4t8d2bqjsd1f2e6te1tqmfenfhkzcz2smex3w6pchm0",
		      "id": "fil_01b64fk2r3af0mp8wek1630m1k57bq8fqp0yvqjq7701b3tngbfyxg"
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
		        "tag": "hello-world",
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
async fn tagged_package1() {
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
		      "id": "dir_01xf6kw5vfpt67fp5vy9a3kedh3np51gzjmr9ekjx06c7m885q105g"
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
		      "id": "fil_01qts0qxbd8dz7y43h0ca0h2sb9menenytsztynzfwfbegxzspa2m0"
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
		      "contents": "blb_01n5629skz1z37hhemwskperd1qt3xgm6ss590q9m8j5ycr0cdr59g",
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
		        "tag": "a",
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
		      "id": "dir_01e4anfv9eq4s6bdg63b578nbver4dzd7mvtg3j64rwpzsgf81k03g"
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
		      "id": "fil_01syp2f8f29xqq09gsjg32r9qnpn5f678r2fb9xvkyxctkh2eve83g"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 3,
		        "tangram.ts": 4
		      },
		      "id": "dir_01b226x1hfwp2bx0nz3yp9mx1ws56757k1hetswgpednsb0jgd5y30"
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
		      "id": "fil_01n2mdc7sttzaq8965pwntpsyzacqtax9dw397c0b82q6dtja5h5p0"
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
		      "id": "fil_01wrq9m40z7v5wsf5hc5a5jrtvwr2eaghzzkex4y9q757tczxw7c20"
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
		                  "foo.tg.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
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
		                      ],
		                    }),
		                    "node": 1,
		                  }),
		                  "tangram.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
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
		                      ],
		                    }),
		                    "node": 2,
		                  }),
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
		        "tag": "a",
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
		      "id": "dir_01rw61x6v704f2yck3jrskv1fkf79ea0cxhv7d8zz4ev2ta19qs4tg"
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
		      "id": "fil_01mgk5bndjshargeg7h6bb0nm2z8yg6jddw0s1910xnmb7hr8ddvbg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01qremn7w8etcxsnn7k1vtxd683q4w0vtfrqqxywcyx7xt2cp5rtwg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_0158re2012fvbq8s0zxgsdmkmg7k05y79mnbeha500h9k973hk06k0",
		      "dependencies": {
		        "b/*": {
		          "item": 4,
		          "subpath": "tangram.ts",
		          "tag": "b/1.0.0"
		        }
		      },
		      "id": "fil_01sa3dcc5y35emtjdj92h46qtc5kh81ytmxqcr285a2f6mc1683e20"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 5,
		        "tangram.ts": 6
		      },
		      "id": "dir_01k61vd0t8f5q72aph52dt1bmbzjx50mskp8wt1f4gxw8ymwgj4270"
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
		      "id": "fil_01r1n4kb6k1kewbxwcasrrtqpddexyvdmcks6qxe62fpq7wzcb7bgg"
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
		          "subpath": "tangram.ts",
		          "tag": "a/1.0.0"
		        }
		      },
		      "id": "fil_01fvq7vvybenar9m5j4dpz8jtrqt21ehvvc0cpvbhqc0mdhpnk4skg"
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
		                  "foo.tg.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                          "dependencies": {
		                            "./tangram.ts": {
		                              "item": 0,
		                              "subpath": "tangram.ts",
		                            },
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                          "dependencies": {
		                            "./foo.tg.ts": {
		                              "item": 0,
		                              "subpath": "foo.tg.ts",
		                            },
		                            "a/*": {
		                              "item": tg.directory({
		                                "tangram.ts": tg.file({
		                                  "contents": tg.blob(""),
		                                }),
		                              }),
		                              "subpath": "tangram.ts",
		                              "tag": "a/1.0.0",
		                            },
		                          },
		                        },
		                      ],
		                    }),
		                    "node": 1,
		                  }),
		                  "tangram.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                          "dependencies": {
		                            "./tangram.ts": {
		                              "item": 0,
		                              "subpath": "tangram.ts",
		                            },
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                          "dependencies": {
		                            "./foo.tg.ts": {
		                              "item": 0,
		                              "subpath": "foo.tg.ts",
		                            },
		                            "a/*": {
		                              "item": tg.directory({
		                                "tangram.ts": tg.file({
		                                  "contents": tg.blob(""),
		                                }),
		                              }),
		                              "subpath": "tangram.ts",
		                              "tag": "a/1.0.0",
		                            },
		                          },
		                        },
		                      ],
		                    }),
		                    "node": 2,
		                  }),
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                "dependencies": {
		                  "./foo.tg.ts": {
		                    "item": 0,
		                    "subpath": "foo.tg.ts",
		                  },
		                  "a/*": {
		                    "item": 2,
		                    "subpath": "tangram.ts",
		                    "tag": "a/1.0.0",
		                  },
		                },
		              },
		              {
		                "kind": "directory",
		                "entries": {
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("import * as b from \"b/*\";\n"),
		                    "dependencies": {
		                      "b/*": {
		                        "item": tg.directory({
		                          "graph": tg.graph({
		                            "nodes": [
		                              {
		                                "kind": "directory",
		                                "entries": {
		                                  "foo.tg.ts": tg.file({
		                                    "graph": tg.graph({
		                                      "nodes": [
		                                        {
		                                          "kind": "directory",
		                                          "entries": {
		                                            "foo.tg.ts": 1,
		                                            "tangram.ts": 2,
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                                          "dependencies": {
		                                            "./tangram.ts": {
		                                              "item": 0,
		                                              "subpath": "tangram.ts",
		                                            },
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                          "dependencies": {
		                                            "./foo.tg.ts": {
		                                              "item": 0,
		                                              "subpath": "foo.tg.ts",
		                                            },
		                                            "a/*": {
		                                              "item": tg.directory({
		                                                "tangram.ts": tg.file({
		                                                  "contents": tg.blob(""),
		                                                }),
		                                              }),
		                                              "subpath": "tangram.ts",
		                                              "tag": "a/1.0.0",
		                                            },
		                                          },
		                                        },
		                                      ],
		                                    }),
		                                    "node": 1,
		                                  }),
		                                  "tangram.ts": tg.file({
		                                    "graph": tg.graph({
		                                      "nodes": [
		                                        {
		                                          "kind": "directory",
		                                          "entries": {
		                                            "foo.tg.ts": 1,
		                                            "tangram.ts": 2,
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                                          "dependencies": {
		                                            "./tangram.ts": {
		                                              "item": 0,
		                                              "subpath": "tangram.ts",
		                                            },
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                          "dependencies": {
		                                            "./foo.tg.ts": {
		                                              "item": 0,
		                                              "subpath": "foo.tg.ts",
		                                            },
		                                            "a/*": {
		                                              "item": tg.directory({
		                                                "tangram.ts": tg.file({
		                                                  "contents": tg.blob(""),
		                                                }),
		                                              }),
		                                              "subpath": "tangram.ts",
		                                              "tag": "a/1.0.0",
		                                            },
		                                          },
		                                        },
		                                      ],
		                                    }),
		                                    "node": 2,
		                                  }),
		                                },
		                              },
		                              {
		                                "kind": "file",
		                                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                "dependencies": {
		                                  "./foo.tg.ts": {
		                                    "item": 0,
		                                    "subpath": "foo.tg.ts",
		                                  },
		                                  "a/*": {
		                                    "item": tg.directory({
		                                      "tangram.ts": tg.file({
		                                        "contents": tg.blob(""),
		                                      }),
		                                    }),
		                                    "subpath": "tangram.ts",
		                                    "tag": "a/1.0.0",
		                                  },
		                                },
		                              },
		                              {
		                                "kind": "file",
		                                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
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
		                        "tag": "b/1.0.0",
		                      },
		                    },
		                  }),
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
		                "dependencies": {
		                  "b/*": {
		                    "item": 0,
		                    "subpath": "tangram.ts",
		                    "tag": "b/1.0.0",
		                  },
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                "dependencies": {
		                  "./tangram.ts": {
		                    "item": 0,
		                    "subpath": "tangram.ts",
		                  },
		                },
		              },
		            ],
		          }),
		          "node": 2,
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
		                  "foo.tg.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                          "dependencies": {
		                            "./tangram.ts": {
		                              "item": 0,
		                              "subpath": "tangram.ts",
		                            },
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                          "dependencies": {
		                            "./foo.tg.ts": {
		                              "item": 0,
		                              "subpath": "foo.tg.ts",
		                            },
		                            "a/*": {
		                              "item": tg.directory({
		                                "tangram.ts": tg.file({
		                                  "contents": tg.blob(""),
		                                }),
		                              }),
		                              "subpath": "tangram.ts",
		                              "tag": "a/1.0.0",
		                            },
		                          },
		                        },
		                      ],
		                    }),
		                    "node": 1,
		                  }),
		                  "tangram.ts": tg.file({
		                    "graph": tg.graph({
		                      "nodes": [
		                        {
		                          "kind": "directory",
		                          "entries": {
		                            "foo.tg.ts": 1,
		                            "tangram.ts": 2,
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                          "dependencies": {
		                            "./tangram.ts": {
		                              "item": 0,
		                              "subpath": "tangram.ts",
		                            },
		                          },
		                        },
		                        {
		                          "kind": "file",
		                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                          "dependencies": {
		                            "./foo.tg.ts": {
		                              "item": 0,
		                              "subpath": "foo.tg.ts",
		                            },
		                            "a/*": {
		                              "item": tg.directory({
		                                "tangram.ts": tg.file({
		                                  "contents": tg.blob(""),
		                                }),
		                              }),
		                              "subpath": "tangram.ts",
		                              "tag": "a/1.0.0",
		                            },
		                          },
		                        },
		                      ],
		                    }),
		                    "node": 2,
		                  }),
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                "dependencies": {
		                  "./foo.tg.ts": {
		                    "item": 0,
		                    "subpath": "foo.tg.ts",
		                  },
		                  "a/*": {
		                    "item": 2,
		                    "subpath": "tangram.ts",
		                    "tag": "a/1.0.0",
		                  },
		                },
		              },
		              {
		                "kind": "directory",
		                "entries": {
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("import * as b from \"b/*\";\n"),
		                    "dependencies": {
		                      "b/*": {
		                        "item": tg.directory({
		                          "graph": tg.graph({
		                            "nodes": [
		                              {
		                                "kind": "directory",
		                                "entries": {
		                                  "foo.tg.ts": tg.file({
		                                    "graph": tg.graph({
		                                      "nodes": [
		                                        {
		                                          "kind": "directory",
		                                          "entries": {
		                                            "foo.tg.ts": 1,
		                                            "tangram.ts": 2,
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                                          "dependencies": {
		                                            "./tangram.ts": {
		                                              "item": 0,
		                                              "subpath": "tangram.ts",
		                                            },
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                          "dependencies": {
		                                            "./foo.tg.ts": {
		                                              "item": 0,
		                                              "subpath": "foo.tg.ts",
		                                            },
		                                            "a/*": {
		                                              "item": tg.directory({
		                                                "tangram.ts": tg.file({
		                                                  "contents": tg.blob(""),
		                                                }),
		                                              }),
		                                              "subpath": "tangram.ts",
		                                              "tag": "a/1.0.0",
		                                            },
		                                          },
		                                        },
		                                      ],
		                                    }),
		                                    "node": 1,
		                                  }),
		                                  "tangram.ts": tg.file({
		                                    "graph": tg.graph({
		                                      "nodes": [
		                                        {
		                                          "kind": "directory",
		                                          "entries": {
		                                            "foo.tg.ts": 1,
		                                            "tangram.ts": 2,
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                                          "dependencies": {
		                                            "./tangram.ts": {
		                                              "item": 0,
		                                              "subpath": "tangram.ts",
		                                            },
		                                          },
		                                        },
		                                        {
		                                          "kind": "file",
		                                          "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                          "dependencies": {
		                                            "./foo.tg.ts": {
		                                              "item": 0,
		                                              "subpath": "foo.tg.ts",
		                                            },
		                                            "a/*": {
		                                              "item": tg.directory({
		                                                "tangram.ts": tg.file({
		                                                  "contents": tg.blob(""),
		                                                }),
		                                              }),
		                                              "subpath": "tangram.ts",
		                                              "tag": "a/1.0.0",
		                                            },
		                                          },
		                                        },
		                                      ],
		                                    }),
		                                    "node": 2,
		                                  }),
		                                },
		                              },
		                              {
		                                "kind": "file",
		                                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                                "dependencies": {
		                                  "./foo.tg.ts": {
		                                    "item": 0,
		                                    "subpath": "foo.tg.ts",
		                                  },
		                                  "a/*": {
		                                    "item": tg.directory({
		                                      "tangram.ts": tg.file({
		                                        "contents": tg.blob(""),
		                                      }),
		                                    }),
		                                    "subpath": "tangram.ts",
		                                    "tag": "a/1.0.0",
		                                  },
		                                },
		                              },
		                              {
		                                "kind": "file",
		                                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
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
		                        "tag": "b/1.0.0",
		                      },
		                    },
		                  }),
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
		                "dependencies": {
		                  "b/*": {
		                    "item": 0,
		                    "subpath": "tangram.ts",
		                    "tag": "b/1.0.0",
		                  },
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
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
		        "tag": "b/1.0.0",
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
		      "id": "dir_01hsbmh86tc28yvgf06s5ak7h6gz73kjy81fgy9b3z4dwhs7yphnx0"
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
		      "id": "fil_01zatcb7pffd5j4c47grp717j0cw34pfxmgahgm1zvzajetr6r5jxg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 3
		      },
		      "id": "dir_01hqc0m35kxkshb2xmznk5v2mprxd2h2sm2sj4hq2kya459ktgabwg"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01byykskrvvmh90z5g0me4ds0e6fnx0dqcygfvqs8pk6q8d2yyzebg",
		      "dependencies": {
		        "a/^1": {
		          "item": 4,
		          "subpath": "tangram.ts",
		          "tag": "a/1.1.0"
		        }
		      },
		      "id": "fil_01w54pe8nh03dsgbtd9n7vrrvx176naar2fzm3jssajbpy28zmxr50"
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
		      "contents": "blb_01yds64vb62ehy37dvxfqna3z6ts7r4kd9r9m96nwcfgerps7yenpg",
		      "id": "fil_015aa81e0d8agetgp8955q1x1kwnsyp2rcsj7rcrpx51cx5dh9ctrg"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 7
		      },
		      "id": "dir_01v8mwngmadf7x4mpzr2b0ne35pjtjfnhd4r380ve4vnsw3q5sgxn0"
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01p5h9vmz4x3nqnnef2pz5nvzx4ff81nhj29n1jccc7kbgw2037p30",
		      "dependencies": {
		        "a/^1.0": {
		          "item": 4,
		          "subpath": "tangram.ts",
		          "tag": "a/1.1.0"
		        }
		      },
		      "id": "fil_01x6pkv47na28txy2fy894dk4c07r515s2k697tj77zv6mdnfgk7z0"
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
		            "contents": tg.blob("import a from \"a/^1.0\";\nexport default tg.command(() => \"c\");\n"),
		            "dependencies": {
		              "a/^1.0": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("export default tg.command(() => \"a/1.1.0\");\n"),
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

		// Clean up server 2.
		server2.stop_gracefully().await;

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
		command.arg("--ignore=false");
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
