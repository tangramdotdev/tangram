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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn directory_with_duplicate_entries() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a.txt" => "Hello, World!",
			"b.txt" => "Hello, World!",
		}
	};
	let path = "directory";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a.txt": tg.file({
		    "contents": tg.blob("Hello, World!"),
		  }),
		  "b.txt": tg.file({
		    "contents": tg.blob("Hello, World!"),
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
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn file_through_symlink() {
	let directory = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c/d";"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	};
	let path = "a";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import \"../b/c/d\";"),
		    "dependencies": {
		      "../b/c/d": {
		        "item": tg.file({
		          "contents": tg.blob("hello, world!"),
		        }),
		        "path": "../b/c/d",
		      },
		    },
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 5,
		  "depth": 4,
		  "weight": 382
		}
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn artifact_symlink() {
	let directory = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c";"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	};
	let path = "a";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import \"../b/c\";"),
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
		  "weight": 303
		}
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn lockfile_out_of_date() {
	let directory = temp::directory! {
		"tangram.ts" => r#"import "./b.tg.ts";"#,
		"./b.tg.ts" => "",
		"tangram.lock" => indoc!(r#"{
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
							"path": "./a.tg.ts"
						}
					}
				}
			]
		}"#),
	};
	let path = "";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "b.tg.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import \"./b.tg.ts\";"),
		    "dependencies": {
		      "./b.tg.ts": {
		        "item": tg.file({
		          "contents": tg.blob(""),
		        }),
		        "path": "b.tg.ts",
		      },
		    },
		  }),
		})
		"#);
		assert_snapshot!(metadata, @r#"
		{
		  "count": 7,
		  "depth": 4,
		  "weight": 515
		}
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		      },
		    },
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package_with_nested_dependencies() {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"bar" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as baz from "../baz";
				"#),
			},
			"baz" => temp::directory! {
				"tangram.ts" => "",
			},
			"tangram.ts" => indoc!(r#"
				import * as bar from "./bar";
				import * as baz from "./baz";
			"#),
		},
	};
	let path = "foo";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "bar": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob("import * as baz from \"../baz\";\n"),
		      "dependencies": {
		        "../baz": {
		          "item": tg.directory({
		            "tangram.ts": tg.file({
		              "contents": tg.blob(""),
		            }),
		          }),
		          "path": "../baz",
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
		    "contents": tg.blob("import * as bar from \"./bar\";\nimport * as baz from \"./baz\";\n"),
		    "dependencies": {
		      "./bar": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("import * as baz from \"../baz\";\n"),
		            "dependencies": {
		              "../baz": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob(""),
		                  }),
		                }),
		                "path": "../baz",
		              },
		            },
		          }),
		        }),
		        "path": "bar",
		      },
		      "./baz": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
		          }),
		        }),
		        "path": "baz",
		      },
		    },
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn package() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"tangram.ts" => "export default () => {};",
		}
	};
	let path = "directory";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("export default () => {};"),
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a": tg.directory({
		    "graph": tg.graph({
		      "nodes": [
		        {
		          "kind": "directory",
		          "entries": {
		            "mod.tg.ts": 1,
		          },
		        },
		        {
		          "kind": "file",
		          "contents": tg.blob("import a from \".\";"),
		          "dependencies": {
		            ".": {
		              "item": 0,
		              "path": ".",
		            },
		          },
		        },
		      ],
		    }),
		    "node": 0,
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		              "path": ".",
		            },
		          },
		        },
		      ],
		    }),
		    "node": 0,
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_directory_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory! {},
			"tangram.ts" => r#"import a from "./a";"#,
		}
	};
	let path = "directory";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a": tg.directory({}),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"./a\";"),
		    "dependencies": {
		      "./a": {
		        "item": tg.directory({}),
		        "path": "a",
		      },
		    },
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_package_with_type_directory_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a" with { type: "directory" };"#,
		}
	};
	let path = "directory";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"./a\" with { type: \"directory\" };"),
		    "dependencies": {
		      "./a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
		          }),
		        }),
		        "path": "a",
		      },
		    },
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn import_package_from_parent() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a";"#,
		}
	};
	let path = "directory";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "a": tg.directory({
		    "tangram.ts": tg.file({
		      "contents": tg.blob(""),
		    }),
		  }),
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"./a\";"),
		    "dependencies": {
		      "./a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob(""),
		          }),
		        }),
		        "path": "a",
		      },
		    },
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "foo.tg.ts": tg.file({
		    "graph": tg.graph({
		      "nodes": [
		        {
		          "kind": "file",
		          "contents": tg.blob("import * as root from \"./tangram.ts\";"),
		          "dependencies": {
		            "./tangram.ts": {
		              "item": 1,
		              "path": "tangram.ts",
		            },
		          },
		        },
		        {
		          "kind": "file",
		          "contents": tg.blob("import * as foo from \"./foo.tg.ts\";"),
		          "dependencies": {
		            "./foo.tg.ts": {
		              "item": 0,
		              "path": "foo.tg.ts",
		            },
		          },
		        },
		      ],
		    }),
		    "node": 0,
		  }),
		  "tangram.ts": tg.file({
		    "graph": tg.graph({
		      "nodes": [
		        {
		          "kind": "file",
		          "contents": tg.blob("import * as root from \"./tangram.ts\";"),
		          "dependencies": {
		            "./tangram.ts": {
		              "item": 1,
		              "path": "tangram.ts",
		            },
		          },
		        },
		        {
		          "kind": "file",
		          "contents": tg.blob("import * as foo from \"./foo.tg.ts\";"),
		          "dependencies": {
		            "./foo.tg.ts": {
		              "item": 0,
		              "path": "foo.tg.ts",
		            },
		          },
		        },
		      ],
		    }),
		    "node": 1,
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn cyclic_dependencies() {
	let directory = temp::directory! {
		"directory" => temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar";"#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "../foo";"#,
			},
		},
	};
	let path = "directory/foo";
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		        "contents": tg.blob("import * as bar from \"../bar\";"),
		        "dependencies": {
		          "../bar": {
		            "item": 2,
		            "path": "../bar",
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
		        "contents": tg.blob("import * as foo from \"../foo\";"),
		        "dependencies": {
		          "../foo": {
		            "item": 0,
		            "path": "../foo",
		          },
		        },
		      },
		    ],
		  }),
		  "node": 0,
		})
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = true;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
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
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn missing_in_lockfile() {
	let directory = temp::directory! {
		"tangram.ts" => r#"import * as a from "./a";"#,
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
								"path": "./a.tg.ts"
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
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
	let destructive = false;
	let tags = vec![];
	let assertions = |object: String, _metadata: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob(""),
		  }),
		})
		"#);
		assert!(lockfile.is_none());
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_object() {
	let tags = vec![("hello".into(), temp::file!("Hello, world!").into())];
	let directory = temp::directory! {
		"tangram.ts" => r#"import hello from "hello";"#,
	};
	let path = "";
	let destructive = false;
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import hello from \"hello\";"),
		    "dependencies": {
		      "hello": {
		        "item": tg.file({
		          "contents": tg.blob("Hello, world!"),
		        }),
		        "tag": "hello",
		      },
		    },
		  }),
		})
		"#);
		assert_json_snapshot!(lockfile.unwrap(), @r#"
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
		        "hello": {
		          "item": 2,
		          "tag": "hello"
		        }
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ad8gw7fez4t8d2bqjsd1f2e6te1tqmfenfhkzcz2smex3w6pchm0"
		    }
		  ]
		}
		"#);
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn simple_tagged_package() {
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default () => "a";
			"#),
		}
		.into(),
	)];
	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
			export default tg.command(async () => {
				return await a();
			});
		"#)
	};
	let destructive = false;
	let path = "";
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"a\";\nexport default tg.command(async () => {\n\treturn await a();\n});\n"),
		    "dependencies": {
		      "a": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("export default () => \"a\";\n"),
		          }),
		        }),
		        "tag": "a",
		      },
		    },
		  }),
		})
		"#);
		assert_json_snapshot!(lockfile.unwrap(), @r#"
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
		        "a": {
		          "item": 2,
		          "tag": "a"
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
		      "contents": "blb_01038pab1jh9r3ztm2811kzr14ff3223xhcp9dgczg1gd1afmje6ng"
		    }
		  ]
		}
		"#);
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tagged_package_with_cyclic_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
		"#),
	};
	let path = "";
	let destructive = false;
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				import foo from "./foo.tg.ts";
			"#),
			"foo.tg.ts" => indoc::indoc!(r#"
				import * as a from "./tangram.ts";
			"#),
		}
		.into(),
	)];
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import a from \"a\";\n"),
		    "dependencies": {
		      "a": {
		        "item": tg.directory({
		          "foo.tg.ts": tg.file({
		            "graph": tg.graph({
		              "nodes": [
		                {
		                  "kind": "file",
		                  "contents": tg.blob("import * as a from \"./tangram.ts\";\n"),
		                  "dependencies": {
		                    "./tangram.ts": {
		                      "item": 1,
		                      "path": "tangram.ts",
		                    },
		                  },
		                },
		                {
		                  "kind": "file",
		                  "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
		                  "dependencies": {
		                    "./foo.tg.ts": {
		                      "item": 0,
		                      "path": "foo.tg.ts",
		                    },
		                  },
		                },
		              ],
		            }),
		            "node": 0,
		          }),
		          "tangram.ts": tg.file({
		            "graph": tg.graph({
		              "nodes": [
		                {
		                  "kind": "file",
		                  "contents": tg.blob("import * as a from \"./tangram.ts\";\n"),
		                  "dependencies": {
		                    "./tangram.ts": {
		                      "item": 1,
		                      "path": "tangram.ts",
		                    },
		                  },
		                },
		                {
		                  "kind": "file",
		                  "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
		                  "dependencies": {
		                    "./foo.tg.ts": {
		                      "item": 0,
		                      "path": "foo.tg.ts",
		                    },
		                  },
		                },
		              ],
		            }),
		            "node": 1,
		          }),
		        }),
		        "tag": "a",
		      },
		    },
		  }),
		})
		"#);
		assert_json_snapshot!(lockfile.unwrap(), @r#"
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
		        "a": {
		          "item": 2,
		          "tag": "a"
		        }
		      }
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 3,
		        "tangram.ts": 4
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ycqx996y57ta8qpg72zsn6g446x37htxw7v3se7xmaa5nwrwx2t0",
		      "dependencies": {
		        "./tangram.ts": {
		          "item": 4,
		          "path": "tangram.ts"
		        }
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_018dz0kathbh9mktnftc933pd35gy651y1ppqe3tg2q0an0dt35zr0",
		      "dependencies": {
		        "./foo.tg.ts": {
		          "item": 3,
		          "path": "foo.tg.ts"
		        }
		      }
		    }
		  ]
		}
		"#);
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tag_dependency_cycles() {
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => "",
			}
			.into(),
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
			}
			.into(),
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as b from "b/*";
				"#),
			}
			.into(),
		),
	];

	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as b from "b/*";
			import * as a from "a/*";
		"#),
	};

	let path = "";
	let destructive = false;
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
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
		                  "foo.tg.ts": 1,
		                  "tangram.ts": 2,
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
		                "dependencies": {
		                  "./tangram.ts": {
		                    "item": 2,
		                    "path": "tangram.ts",
		                  },
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                "dependencies": {
		                  "./foo.tg.ts": {
		                    "item": 1,
		                    "path": "foo.tg.ts",
		                  },
		                  "a/*": {
		                    "item": 3,
		                    "tag": "a/1.1.0",
		                  },
		                },
		              },
		              {
		                "kind": "directory",
		                "entries": {
		                  "tangram.ts": 4,
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
		                "dependencies": {
		                  "b/*": {
		                    "item": 0,
		                    "tag": "b/1.0.0",
		                  },
		                },
		              },
		            ],
		          }),
		          "node": 3,
		        }),
		        "tag": "a/1.1.0",
		      },
		      "b/*": {
		        "item": tg.directory({
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
		                    "item": 2,
		                    "path": "tangram.ts",
		                  },
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
		                "dependencies": {
		                  "./foo.tg.ts": {
		                    "item": 1,
		                    "path": "foo.tg.ts",
		                  },
		                  "a/*": {
		                    "item": 3,
		                    "tag": "a/1.1.0",
		                  },
		                },
		              },
		              {
		                "kind": "directory",
		                "entries": {
		                  "tangram.ts": 4,
		                },
		              },
		              {
		                "kind": "file",
		                "contents": tg.blob("import * as b from \"b/*\";\n"),
		                "dependencies": {
		                  "b/*": {
		                    "item": 0,
		                    "tag": "b/1.0.0",
		                  },
		                },
		              },
		            ],
		          }),
		          "node": 0,
		        }),
		        "tag": "b/1.0.0",
		      },
		    },
		  }),
		})
		"#);
		assert_json_snapshot!(lockfile.unwrap(), @r#"
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
		        "a/*": {
		          "item": 2,
		          "tag": "a/1.1.0"
		        },
		        "b/*": {
		          "item": 4,
		          "tag": "b/1.0.0"
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
		      "contents": "blb_0158re2012fvbq8s0zxgsdmkmg7k05y79mnbeha500h9k973hk06k0",
		      "dependencies": {
		        "b/*": {
		          "item": 4,
		          "tag": "b/1.0.0"
		        }
		      }
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "foo.tg.ts": 5,
		        "tangram.ts": 6
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01mv4a5380n5nacg4cvgh1r1f0vcrk489j6rfsj031gxp9b8t9gxq0",
		      "dependencies": {
		        "./tangram.ts": {
		          "item": 6,
		          "path": "tangram.ts"
		        }
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01ajr136dx93ph4zrx9eqbq5gz05gh530ew34qzh0dgh4jbvvx6m30",
		      "dependencies": {
		        "./foo.tg.ts": {
		          "item": 5,
		          "path": "foo.tg.ts"
		        },
		        "a/*": {
		          "item": 2,
		          "tag": "a/1.1.0"
		        }
		      }
		    }
		  ]
		}
		"#);
	};
	test_checkin(directory, path, destructive, tags, assertions).await;
}

#[tokio::test]
async fn tag_diamond_dependency() {
	let directory = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import b from "b";
			import c from "c";
		"#),
	};
	let path = "";
	let destructive = false;
	let tags = vec![
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default () => "a/1.0.0";
				"#),
			}
			.into(),
		),
		(
			"a/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					export default () => "a/1.1.0";
				"#),
			}
			.into(),
		),
		(
			"b".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1";
					export default () => "b";
				"#),
			}
			.into(),
		),
		(
			"c".into(),
			temp::directory! {
				"tangram.ts" => indoc::indoc!(r#"
					import a from "a/^1.0";
					export default () => "c";
				"#),
			}
			.into(),
		),
	];
	let assertions = |object: String, _: String, lockfile: Option<tg::Lockfile>| async move {
		assert_snapshot!(object, @r#"
		tg.directory({
		  "tangram.ts": tg.file({
		    "contents": tg.blob("import b from \"b\";\nimport c from \"c\";\n"),
		    "dependencies": {
		      "b": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("import a from \"a/^1\";\nexport default () => \"b\";\n"),
		            "dependencies": {
		              "a/^1": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("export default () => \"a/1.1.0\";\n"),
		                  }),
		                }),
		                "tag": "a/1.1.0",
		              },
		            },
		          }),
		        }),
		        "tag": "b",
		      },
		      "c": {
		        "item": tg.directory({
		          "tangram.ts": tg.file({
		            "contents": tg.blob("import a from \"a/^1.0\";\nexport default () => \"c\";\n"),
		            "dependencies": {
		              "a/^1.0": {
		                "item": tg.directory({
		                  "tangram.ts": tg.file({
		                    "contents": tg.blob("export default () => \"a/1.1.0\";\n"),
		                  }),
		                }),
		                "tag": "a/1.1.0",
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
		assert_json_snapshot!(lockfile.unwrap(), @r#"
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
		        "b": {
		          "item": 2,
		          "tag": "b"
		        },
		        "c": {
		          "item": 6,
		          "tag": "c"
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
		      "contents": "blb_01dra984m32dp47yvrynmkcxzcxmgmptmgnyhb8xk6th2wk3wgkae0",
		      "dependencies": {
		        "a/^1": {
		          "item": 4,
		          "tag": "a/1.1.0"
		        }
		      }
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 5
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01m5trn2pegd6rtt2phdq9ggagexa9w1w57e4ckkc6gj7rfxnnbbh0"
		    },
		    {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": 7
		      }
		    },
		    {
		      "kind": "file",
		      "contents": "blb_01xv04mkcazz11b2wsjy0kp279772f9wb2gbsn2maer3qxhbdvvppg",
		      "dependencies": {
		        "a/^1.0": {
		          "item": 4,
		          "tag": "a/1.1.0"
		        }
		      }
		    }
		  ]
		}
		"#);
	};
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
		let tags = vec![];

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
				export default () => "foo";
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
				export default () => foo();
			"#)
		};
		let path = "";
		let destructive = false;
		let tags = vec![];
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
		let tags = vec![];
		let (output2, _, _) = test_checkin_inner(referrer, path, destructive, tags, &server2).await;

		// Confirm the outputs are the same.
		assert_eq!(output1, output2);
	})
	.await;
}

#[tokio::test]
async fn update_tagged_package() {
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		// Tag old version.
		let old: temp::Artifact = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default () => "a/1.0.0";
			"#),
		}
		.into();
		let temp = Temp::new();
		old.to_path(temp.path()).await.unwrap();
		let output = server
			.tg()
			.arg("tag")
			.arg("a/1.0.0")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Create new artifact and check it in.
		let local: temp::Artifact = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				import a from "a/^1";
				export default () => tg.run(a);
			"#),
		}
		.into();
		let local_temp = Temp::new();
		local.to_path(local_temp.path()).await.unwrap();

		// Create initial checkin.
		let output = server
			.tg()
			.arg("checkin")
			.arg(local_temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Check the original lockfile.
		let lockfile = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.map(|bytes| serde_json::from_slice::<tg::Lockfile>(&bytes))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))
			.unwrap();
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
		        "a/^1": {
		          "item": 2,
		          "tag": "a/1.0.0"
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
		      "contents": "blb_01qc4k8f53qz0mh9e1wwmymcqj99bebd2r9t65g8ry4a2bx1hcr2v0"
		    }
		  ]
		}
		"#);

		// Publish an update.
		let new: temp::Artifact = temp::directory! {
			"tangram.ts" => indoc::indoc!(r#"
				export default () => "a/1.1.0";
			"#),
		}
		.into();
		let temp = Temp::new();
		new.to_path(temp.path()).await.unwrap();
		let output = server
			.tg()
			.arg("tag")
			.arg("a/1.1.0")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Checkin again.
		let output = server
			.tg()
			.arg("checkin")
			.arg(local_temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Verify the lockfile hasn't changed.
		let lockfile = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.map(|bytes| serde_json::from_slice::<tg::Lockfile>(&bytes))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))
			.unwrap();
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
		        "a/^1": {
		          "item": 2,
		          "tag": "a/1.0.0"
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
		      "contents": "blb_01qc4k8f53qz0mh9e1wwmymcqj99bebd2r9t65g8ry4a2bx1hcr2v0"
		    }
		  ]
		}
		"#);

		// Update.
		let output = server
			.tg()
			.arg("update")
			.arg(local_temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		// Verify the lockfile is different.
		let lockfile = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
			.await
			.ok()
			.map(|bytes| serde_json::from_slice::<tg::Lockfile>(&bytes))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))
			.unwrap();
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
		        "a/^1": {
		          "item": 2,
		          "tag": "a/1.1.0"
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
		      "contents": "blb_01m5trn2pegd6rtt2phdq9ggagexa9w1w57e4ckkc6gj7rfxnnbbh0"
		    }
		  ]
		}
		"#);
	})
	.await;
}

async fn test_checkin<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	destructive: bool,
	tags: impl IntoIterator<Item = (String, temp::Artifact)> + Send + 'static,
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
	tags: impl IntoIterator<Item = (String, temp::Artifact)> + Send + 'static,
	server: &Server,
) -> (String, String, Option<tg::Lockfile>) {
	// Tag the objects.
	for (tag, artifact) in tags {
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

	// Index.
	let mut index_command = server.tg();
	index_command.arg("index");
	let index_output = index_command.output().await.unwrap();
	assert_success!(index_output);

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
		.output()
		.await
		.unwrap();
	assert_success!(object_output);
	let object = std::str::from_utf8(&object_output.stdout).unwrap().into();

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
	let metadata = std::str::from_utf8(&metadata_output.stdout).unwrap().into();

	// Get the lockfile.
	let lockfile = tokio::fs::read(path.join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize lockfile"))
		.unwrap();

	(object, metadata, lockfile)
}
