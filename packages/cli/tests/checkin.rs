use {
	indoc::indoc,
	insta::{assert_json_snapshot, assert_snapshot},
	std::path::Path,
	tangram_cli_test::{Server, assert_failure, assert_success},
	tangram_client as tg,
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let artifact = temp::directory! {
		"hello.txt" => "Hello, world!",
		"link" => temp::symlink!("hello.txt"),
		"subdirectory" => temp::directory! {
			"sublink" => temp::symlink!("../link"),
		}
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "hello.txt": tg.file({
	    "contents": tg.blob("Hello, world!"),
	  }),
	  "link": tg.symlink({
	    "path": "hello.txt",
	  }),
	  "subdirectory": tg.directory({
	    "sublink": tg.symlink({
	      "path": "../link",
	    }),
	  }),
	})
	"#);
	assert_snapshot!(metadata, @r#"
	{
	  "count": 6,
	  "depth": 3,
	  "weight": 298
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn file() {
	let artifact = temp::directory! {
		"README.md" => "Hello, World!",
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
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
	  "weight": 115
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn symlink() {
	let artifact = temp::directory! {
		"link" => temp::symlink!("."),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "link": tg.symlink({
	    "path": ".",
	  }),
	})
	"#);
	assert_snapshot!(metadata, @r#"
	{
	  "count": 2,
	  "depth": 2,
	  "weight": 61
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn directory_with_duplicate_entries() {
	let artifact = temp::directory! {
		"a.txt" => "Hello, World!",
		"b.txt" => "Hello, World!",
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
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
	  "weight": 156
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn file_through_symlink() {
	let artifact = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c/d";"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	}
	.into();
	let path = Path::new("a");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import \"../b/c/d\";"),
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
	  "weight": 247
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn file_with_symlink_no_kind() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "./foo.tg.ts";
		"#),
		"foo.tg.ts" => temp::symlink!("bar.tg.ts"),
		"bar.tg.ts" => indoc!(r#"
			export default bar = "bar";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "bar.tg.ts": tg.file({
	    "contents": tg.blob("export default bar = \"bar\";\n"),
	  }),
	  "foo.tg.ts": tg.symlink({
	    "path": "bar.tg.ts",
	  }),
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
	    "dependencies": {
	      "./foo.tg.ts": {
	        "item": tg.file({
	          "contents": tg.blob("export default bar = \"bar\";\n"),
	        }),
	        "path": "bar.tg.ts",
	      },
	    },
	  }),
	})
	"#);
	assert_snapshot!(metadata, @r#"
	{
	  "count": 8,
	  "depth": 4,
	  "weight": 467
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn file_with_symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "./foo.tg.ts" with { kind: "symlink" };
		"#),
		"foo.tg.ts" => temp::symlink!("bar.tg.ts"),
		"bar.tg.ts" => indoc!(r#"
			export default bar = "bar";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "bar.tg.ts": tg.file({
	    "contents": tg.blob("export default bar = \"bar\";\n"),
	  }),
	  "foo.tg.ts": tg.symlink({
	    "path": "bar.tg.ts",
	  }),
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import foo from \"./foo.tg.ts\" with { kind: \"symlink\" };\n"),
	    "dependencies": {
	      "./foo.tg.ts": {
	        "item": tg.symlink({
	          "path": "bar.tg.ts",
	        }),
	        "path": "foo.tg.ts",
	      },
	    },
	  }),
	})
	"#);
	assert_snapshot!(metadata, @r#"
	{
	  "count": 7,
	  "depth": 3,
	  "weight": 436
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn artifact_symlink() {
	let artifact = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => r#"import "../b/c";"#,
		},
		"b" => temp::directory! {
			"c" => temp::symlink!("e"),
			"e" => temp::directory! {
				"d" => "hello, world!"
			}
		}
	}
	.into();
	let path = Path::new("a");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import \"../b/c\";"),
	    "dependencies": {
	      "../b/c": {
	        "item": tg.directory({
	          "d": tg.file({
	            "contents": tg.blob("hello, world!"),
	          }),
	        }),
	        "path": "../b/e",
	      },
	    },
	  }),
	})
	"#);
	assert_snapshot!(metadata, @r#"
	{
	  "count": 6,
	  "depth": 5,
	  "weight": 290
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn lock_out_of_date() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"import "./b.tg.ts";"#,
		"./b.tg.ts" => "",
		"tangram.lock" => indoc!(r#"{
			"nodes": [
				{
					"kind": "directory",
					"entries": {
						"a.tg.ts": { "node": 1 },
						"tangram.ts": { "node": 2 }
					}
				},
				{
					"kind": "file"
				},
				{
					"kind": "file",
					"dependencies": {
						"./a.tg.ts": {
							"item": { "node": 0 },
							"path": "./a.tg.ts"
						}
					}
				}
			]
		}"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, metadata, lock) = test(artifact, path, destructive, tags).await;
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
	  "weight": 327
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn simple_path_dependency() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar";"#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		},
	}
	.into();
	let path = Path::new("foo");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn package_with_nested_dependencies() {
	let artifact = temp::directory! {
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
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn package() {
	let artifact = temp::directory! {
		"tangram.ts" => "export default () => {};",
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("export default () => {};"),
	  }),
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn directory_with_nested_packages() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => "",
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn import_directory_from_current() {
	let artifact = temp::directory! {
		"a" => temp::directory! {
			"mod.tg.ts" => r#"import a from ".";"#
		},
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "a": tg.directory({
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "directory",
	          "entries": {
	            "mod.tg.ts": {
	              "node": 1,
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import a from \".\";"),
	          "dependencies": {
	            ".": {
	              "item": {
	                "node": 0,
	              },
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn import_package_from_current() {
	let artifact = temp::directory! {
		"a" => temp::directory! {
			"mod.tg.ts" => r#"import * as a from ".";"#,
			"tangram.ts" => ""
		},
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "a": tg.directory({
	    "graph": tg.graph({
	      "nodes": [
	        {
	          "kind": "directory",
	          "entries": {
	            "mod.tg.ts": {
	              "node": 1,
	            },
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
	              "item": {
	                "node": 0,
	              },
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn import_directory_from_parent() {
	let artifact = temp::directory! {
		"a" => temp::directory! {},
		"tangram.ts" => r#"import a from "./a";"#,
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "a": tg.directory(),
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import a from \"./a\";"),
	    "dependencies": {
	      "./a": {
	        "item": tg.directory(),
	        "path": "a",
	      },
	    },
	  }),
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn import_package_with_type_directory_from_parent() {
	let artifact = temp::directory! {
		"a" => temp::directory!{
			"tangram.ts" => "",
		},
		"tangram.ts" => r#"import a from "./a" with { type: "directory" };"#,
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn import_package_from_parent() {
	let artifact = temp::directory! {
		"a" => temp::directory!{
			"tangram.ts" => "",
		},
		"tangram.ts" => r#"import a from "./a";"#,
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn package_with_cyclic_modules() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"import * as foo from "./foo.tg.ts";"#,
		"foo.tg.ts" => r#"import * as root from "./tangram.ts";"#,
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	              "item": {
	                "node": 1,
	              },
	              "path": "tangram.ts",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"./foo.tg.ts\";"),
	          "dependencies": {
	            "./foo.tg.ts": {
	              "item": {
	                "node": 0,
	              },
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
	              "item": {
	                "node": 1,
	              },
	              "path": "tangram.ts",
	            },
	          },
	        },
	        {
	          "kind": "file",
	          "contents": tg.blob("import * as foo from \"./foo.tg.ts\";"),
	          "dependencies": {
	            "./foo.tg.ts": {
	              "item": {
	                "node": 0,
	              },
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn cyclic_dependencies() {
	let artifact = temp::directory! {
		"directory" => temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar";"#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"import * as foo from "../foo";"#,
			},
		},
	}
	.into();
	let path = Path::new("directory/foo");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "graph": tg.graph({
	    "nodes": [
	      {
	        "kind": "directory",
	        "entries": {
	          "tangram.ts": {
	            "node": 1,
	          },
	        },
	      },
	      {
	        "kind": "file",
	        "contents": tg.blob("import * as bar from \"../bar\";"),
	        "dependencies": {
	          "../bar": {
	            "item": {
	              "node": 2,
	            },
	            "path": "../bar",
	          },
	        },
	      },
	      {
	        "kind": "directory",
	        "entries": {
	          "tangram.ts": {
	            "node": 3,
	          },
	        },
	      },
	      {
	        "kind": "file",
	        "contents": tg.blob("import * as foo from \"../foo\";"),
	        "dependencies": {
	          "../foo": {
	            "item": {
	              "node": 0,
	            },
	            "path": "../foo",
	          },
	        },
	      },
	    ],
	  }),
	  "node": 0,
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn directory_destructive() {
	let artifact = temp::directory! {
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
	}
	.into();
	let path = Path::new("directory");
	let destructive = true;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "a": tg.directory({
	    "b": tg.directory({
	      "c": tg.symlink({
	        "path": "../../a/d/e",
	      }),
	    }),
	    "d": tg.directory({
	      "e": tg.symlink({
	        "path": "../../a/f/g",
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
	assert!(lock.is_none());
}

#[tokio::test]
async fn default_ignore() {
	let artifact = temp::directory! {
		".DS_Store" => temp::file!(""),
		".git" => temp::directory! {
			"config" => temp::file!(""),
		},
		".tangram" => temp::directory! {
			"config" => temp::file!(""),
		},
		"tangram.lock" => temp::file!(r#"{"nodes":[]}"#),
		"tangram.ts" => temp::file!(""),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	  }),
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn missing_in_lockfile() {
	let artifact = temp::directory! {
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
	}
	.into();
	let path = Path::new("a");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	  }),
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn invalid_lockfile() {
	let artifact = temp::directory! {
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
	}
	.into();
	let path = Path::new("a");
	let destructive = false;
	let tags = vec![];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	  }),
	})
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn tagged_object() {
	let tags = vec![("hello".into(), temp::file!("Hello, world!").into())];
	let artifact = temp::directory! {
		"tangram.ts" => r#"import hello from "hello";"#,
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert_json_snapshot!(lock.unwrap(), @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "hello": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "fil_01sp9ta6qgjk4msgsw7fxck19fxxeqec551wbsc8w9gjvm59vs9w00",
	            "tag": "hello"
	          }
	        }
	      }
	    },
	    {
	      "kind": "file"
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn simple_tagged_package() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import a from "a";
			export default tg.command(async () => {
				return await a();
			});
		"#)
	}
	.into();
	let destructive = false;
	let path = Path::new("");
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "a";
			"#),
		}
		.into(),
	)];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	assert_json_snapshot!(lock.unwrap(), @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_01va3y2d84s8st8xbsnka859w7rf90yc9kvsbb5zskhq5nn8q0vnsg",
	            "tag": "a"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn tagged_package_with_cyclic_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import a from "a";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![(
		"a".into(),
		temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "./foo.tg.ts";
			"#),
			"foo.tg.ts" => indoc!(r#"
				import * as a from "./tangram.ts";
			"#),
		}
		.into(),
	)];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	                      "item": {
	                        "node": 1,
	                      },
	                      "path": "tangram.ts",
	                    },
	                  },
	                },
	                {
	                  "kind": "file",
	                  "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
	                  "dependencies": {
	                    "./foo.tg.ts": {
	                      "item": {
	                        "node": 0,
	                      },
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
	                      "item": {
	                        "node": 1,
	                      },
	                      "path": "tangram.ts",
	                    },
	                  },
	                },
	                {
	                  "kind": "file",
	                  "contents": tg.blob("import foo from \"./foo.tg.ts\";\n"),
	                  "dependencies": {
	                    "./foo.tg.ts": {
	                      "item": {
	                        "node": 0,
	                      },
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
	assert_json_snapshot!(lock.unwrap(), @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_010c0ty66164s1qxzc6c8f6wr364pns1gfwkczzr9za6bnd1ftev7g",
	            "tag": "a"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn tag_dependency_not_exist() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as a from "a/^1.2";
		"#),
	}
	.into();
	let tags = vec![];
	let path = Path::new("");
	let destructive = false;
	let (stdout, stderr) = test_failure(artifact, path, destructive, tags).await;
	eprintln!("stdout: {stdout:?}");
	eprintln!("stderr: {stderr:?}");
	assert_snapshot!(stderr, @r"
	error an error occurred
	-> <path>/tangram.ts requires 'a/^1.2' but no matching tags were found
	");
	assert_snapshot!(stdout, @r#""#);
}

#[tokio::test]
async fn tag_dependency_no_solution() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as a from "a/*";
			import * as b from "b/*";
		"#),
	}
	.into();

	let tags = vec![
		(
			"c/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r""),
			}
			.into(),
		),
		(
			"c/2.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r""),
			}
			.into(),
		),
		(
			"a/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as c from "c/^1"
				"#),
			}
			.into(),
		),
		(
			"b/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as c from "c/^2"
				"#),
			}
			.into(),
		),
	];
	let path = Path::new("");
	let destructive = false;
	let (stdout, stderr) = test_failure(artifact, path, destructive, tags).await;
	assert_snapshot!(stderr, @r"
	error an error occurred
	-> failed to find a matching tag for 'c'
	-> b/1.0.0:tangram.ts requires 'c/^2'
	-> a/1.0.0:tangram.ts requires 'c/^1'
	");
	assert_snapshot!(stdout, @r#""#);
}

#[tokio::test]
async fn tag_dependency_cycles() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as a from "a/*";
			import * as b from "b/*";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
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
				"foo.tg.ts" => indoc!(r#"
					import * as b from "./tangram.ts";
				"#),
				"tangram.ts" => indoc!(r#"
					import * as a from "a/*";
					import * as foo from "./foo.tg.ts";
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
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import * as a from \"a/*\";\nimport * as b from \"b/*\";\n"),
	    "dependencies": {
	      "a/%2A": {
	        "item": tg.directory({
	          "graph": tg.graph({
	            "nodes": [
	              {
	                "kind": "directory",
	                "entries": {
	                  "tangram.ts": {
	                    "node": 1,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"b/*\";\n"),
	                "dependencies": {
	                  "b/%2A": {
	                    "item": {
	                      "node": 2,
	                    },
	                    "tag": "b/1.0.0",
	                  },
	                },
	              },
	              {
	                "kind": "directory",
	                "entries": {
	                  "foo.tg.ts": {
	                    "node": 3,
	                  },
	                  "tangram.ts": {
	                    "node": 4,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
	                "dependencies": {
	                  "./tangram.ts": {
	                    "item": {
	                      "node": 4,
	                    },
	                    "path": "tangram.ts",
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
	                "dependencies": {
	                  "./foo.tg.ts": {
	                    "item": {
	                      "node": 3,
	                    },
	                    "path": "foo.tg.ts",
	                  },
	                  "a/%2A": {
	                    "item": {
	                      "node": 0,
	                    },
	                    "tag": "a/1.1.0",
	                  },
	                },
	              },
	            ],
	          }),
	          "node": 0,
	        }),
	        "tag": "a/1.1.0",
	      },
	      "b/%2A": {
	        "item": tg.directory({
	          "graph": tg.graph({
	            "nodes": [
	              {
	                "kind": "directory",
	                "entries": {
	                  "tangram.ts": {
	                    "node": 1,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"b/*\";\n"),
	                "dependencies": {
	                  "b/%2A": {
	                    "item": {
	                      "node": 2,
	                    },
	                    "tag": "b/1.0.0",
	                  },
	                },
	              },
	              {
	                "kind": "directory",
	                "entries": {
	                  "foo.tg.ts": {
	                    "node": 3,
	                  },
	                  "tangram.ts": {
	                    "node": 4,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
	                "dependencies": {
	                  "./tangram.ts": {
	                    "item": {
	                      "node": 4,
	                    },
	                    "path": "tangram.ts",
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as a from \"a/*\";\nimport * as foo from \"./foo.tg.ts\";\n"),
	                "dependencies": {
	                  "./foo.tg.ts": {
	                    "item": {
	                      "node": 3,
	                    },
	                    "path": "foo.tg.ts",
	                  },
	                  "a/%2A": {
	                    "item": {
	                      "node": 0,
	                    },
	                    "tag": "a/1.1.0",
	                  },
	                },
	              },
	            ],
	          }),
	          "node": 2,
	        }),
	        "tag": "b/1.0.0",
	      },
	    },
	  }),
	})
	"#);
	assert_json_snapshot!(lock.unwrap(), @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/%2A": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_01ybr8jp6yasqkwng3777n1r2m7zkqa5gdh3mnqn32a5zcakzjmh60",
	            "tag": "a/1.1.0"
	          }
	        },
	        "b/%2A": {
	          "item": {
	            "node": 3
	          },
	          "options": {
	            "id": "dir_01ctfx95x4rjxts0axh5rj24n3gzz9zktja0h35af79vxw6mkepgrg",
	            "tag": "b/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 4
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "foo.tg.ts": {
	          "node": 5
	        },
	        "tangram.ts": {
	          "node": 6
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "b/%2A": {
	          "item": {
	            "node": 3
	          },
	          "options": {
	            "id": "dir_01ctfx95x4rjxts0axh5rj24n3gzz9zktja0h35af79vxw6mkepgrg",
	            "tag": "b/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "./tangram.ts": {
	          "item": {
	            "node": 6
	          },
	          "options": {
	            "path": "tangram.ts"
	          }
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "./foo.tg.ts": {
	          "item": {
	            "node": 5
	          },
	          "options": {
	            "path": "foo.tg.ts"
	          }
	        },
	        "a/%2A": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_01ybr8jp6yasqkwng3777n1r2m7zkqa5gdh3mnqn32a5zcakzjmh60",
	            "tag": "a/1.1.0"
	          }
	        }
	      }
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn tag_diamond_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import b from "b";
			import c from "c";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![
		(
			"d/1.0.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default () => "d/1.0.0";
				"#),
			}
			.into(),
		),
		(
			"d/1.1.0".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default () => "d/1.1.0";
				"#),
			}
			.into(),
		),
		(
			"b".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import d from "d/^1";
					export default () => "b";
				"#),
			}
			.into(),
		),
		(
			"c".into(),
			temp::directory! {
				"tangram.ts" => indoc!(r#"
					import d from "d/^1.0";
					export default () => "c";
				"#),
			}
			.into(),
		),
	];
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
	assert_snapshot!(object, @r#"
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("import b from \"b\";\nimport c from \"c\";\n"),
	    "dependencies": {
	      "b": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import d from \"d/^1\";\nexport default () => \"b\";\n"),
	            "dependencies": {
	              "d/%5E1": {
	                "item": tg.directory({
	                  "tangram.ts": tg.file({
	                    "contents": tg.blob("export default () => \"d/1.1.0\";\n"),
	                  }),
	                }),
	                "tag": "d/1.1.0",
	              },
	            },
	          }),
	        }),
	        "tag": "b",
	      },
	      "c": {
	        "item": tg.directory({
	          "tangram.ts": tg.file({
	            "contents": tg.blob("import d from \"d/^1.0\";\nexport default () => \"c\";\n"),
	            "dependencies": {
	              "d/%5E1.0": {
	                "item": tg.directory({
	                  "tangram.ts": tg.file({
	                    "contents": tg.blob("export default () => \"d/1.1.0\";\n"),
	                  }),
	                }),
	                "tag": "d/1.1.0",
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
	assert_json_snapshot!(lock.unwrap(), @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "b": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_01f87ev0es7bk0bzb0hc1h5rxe4d7qhf1srqachk2ga3bmprx5ry2g",
	            "tag": "b"
	          }
	        },
	        "c": {
	          "item": {
	            "node": 3
	          },
	          "options": {
	            "id": "dir_012vhy5zrsdgfgxc04630z8c1sd6zgj2nnqdm26e1hzdatgckam970",
	            "tag": "c"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 4
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 5
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "d/%5E1": {
	          "item": {
	            "node": 6
	          },
	          "options": {
	            "id": "dir_01pxhm32h8qysrdjmpwastx1p1qqtnvkzq4n27gkwwgj50vmzs28a0",
	            "tag": "d/1.1.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "d/%5E1.0": {
	          "item": {
	            "node": 6
	          },
	          "options": {
	            "id": "dir_01pxhm32h8qysrdjmpwastx1p1qqtnvkzq4n27gkwwgj50vmzs28a0",
	            "tag": "d/1.1.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn tagged_package_reproducible_checkin() {
	// Create a remote server.
	let remote_server = Server::new(TG).await.unwrap();

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
	let local_server1 = Server::new(TG).await.unwrap();
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
	let local_server2 = Server::new(TG).await.unwrap();
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
		"tangram.ts" => indoc!(r#"
			import * as foo from "foo";
		"#),
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];

	// Confirm the two outputs are the same.
	let (object_output1, _metadata_output1, _lockfile1) = test_inner(
		&local_server1,
		artifact.clone(),
		path,
		destructive,
		tags.clone(),
		true,
	)
	.await;
	let (object_output2, _metadata_output2, _lockfile2) = test_inner(
		&local_server2,
		artifact.clone(),
		path,
		destructive,
		tags,
		true,
	)
	.await;
	assert_eq!(object_output1, object_output2);
}

#[tokio::test]
async fn tag_dependencies_after_clean() {
	// Create the first server.
	let server1 = Server::new(TG).await.unwrap();

	// Create the second server.
	let server2 = Server::new(TG).await.unwrap();
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
		"tangram.ts" => indoc!(r#"
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
	let referrer: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "foo";
			export default () => foo();
		"#)
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (output1, _, _) =
		test_inner(&server2, referrer.clone(), path, destructive, tags, true).await;

	// Clean up server 2.
	server2.stop().await;

	// Create the second server again.
	let server2 = Server::new(TG).await.unwrap();
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

	// Checkin the artifact to server 2 again, this time the lock has been written to disk.
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (output2, _, _) =
		test_inner(&server2, referrer.clone(), path, destructive, tags, true).await;

	// Confirm the outputs are the same.
	assert_eq!(output1, output2);
}

#[tokio::test]
async fn update_tagged_package() {
	let server = Server::new(TG).await.unwrap();

	let old: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
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

	let local: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import a from "a/^1";
			export default () => tg.run(a);
		"#),
	}
	.into();
	let local_temp = Temp::new();
	local.to_path(local_temp.path()).await.unwrap();

	let output = server
		.tg()
		.arg("checkin")
		.arg(local_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let lock = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice::<tg::graph::Data>(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize the lockfile"))
		.unwrap();
	assert_json_snapshot!(lock, @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/%5E1": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_0183znhfqkp82txvn009a7ahnemg9fvkfztm7qrk2k74dx17mjm2gg",
	            "tag": "a/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);

	let new: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
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

	let output = server
		.tg()
		.arg("checkin")
		.arg(local_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let lock = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice::<tg::graph::Data>(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize lock"))
		.unwrap();
	assert_json_snapshot!(lock, @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/%5E1": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_0183znhfqkp82txvn009a7ahnemg9fvkfztm7qrk2k74dx17mjm2gg",
	            "tag": "a/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);

	let output = server
		.tg()
		.arg("update")
		.arg(local_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let lock = tokio::fs::read(local_temp.path().join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice::<tg::graph::Data>(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize lock"))
		.unwrap();
	assert_json_snapshot!(lock, @r#"
	{
	  "nodes": [
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 1
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "dependencies": {
	        "a/%5E1": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "id": "dir_01q46gmjms1bbs1hqz1jwq076c3w8gyghm5anbj8nx9sffephf8nyg",
	            "tag": "a/1.1.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory"
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn cyclic_tag_dependency() {
	let server = Server::new(TG).await.unwrap();

	// Tag b with an empty package.
	let temp = Temp::new();
	let artifact = temp::directory! {};
	artifact.to_path(&temp).await.unwrap();
	let output = server
		.tg()
		.arg("tag")
		.arg("b")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let artifact = temp::directory! {
		"a" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import b from "b" with { local: "../b" };
			"#),
		},
		"b" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import a from "a" with { local: "../a" };
			"#),
		},
	};
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();

	// Check in and tag a with local dependencies.
	let output = server
		.tg()
		.arg("tag")
		.arg("a")
		.arg(temp.path().join("a"))
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Check in and tag a with local dependencies.
	let output = server
		.tg()
		.arg("tag")
		.arg("--no-local-dependencies")
		.arg("b")
		.arg(temp.path().join("b"))
		.output()
		.await
		.unwrap();
	assert_success!(output);
}

async fn test(
	artifact: temp::Artifact,
	path: &Path,
	destructive: bool,
	tags: Vec<(String, temp::Artifact)>,
) -> (String, String, Option<tg::graph::Data>) {
	let server = Server::new(TG).await.unwrap();
	test_inner(&server, artifact, path, destructive, tags, true).await
}

async fn test_failure(
	artifact: temp::Artifact,
	path: &Path,
	destructive: bool,
	tags: Vec<(String, temp::Artifact)>,
) -> (String, String) {
	let server = Server::new(TG).await.unwrap();
	let (stdout, stderr, _) = test_inner(&server, artifact, path, destructive, tags, false).await;
	(stdout, stderr)
}

async fn test_inner(
	server: &Server,
	artifact: temp::Artifact,
	path: &Path,
	destructive: bool,
	tags: Vec<(String, temp::Artifact)>,
	expect_success: bool,
) -> (String, String, Option<tg::graph::Data>) {
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
	if !expect_success {
		eprintln!(
			"process succeeded: {}",
			std::str::from_utf8(&output.stdout).unwrap()
		);
		assert_failure!(output);
		let stdout = String::from_utf8(output.stdout).unwrap();
		let stderr = String::from_utf8(output.stderr).unwrap();
		let stdout = stdout.replace(path.to_str().unwrap(), "<path>/");
		let stderr = stderr.replace(path.to_str().unwrap(), "<path>/");
		return (stdout, stderr, None);
	}
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
		.arg("--format=tgon")
		.arg("--print-blobs")
		.arg("--print-depth=inf")
		.arg("--print-pretty=true")
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

	// Get the lock.
	let lock = tokio::fs::read(path.join(tg::package::LOCKFILE_FILE_NAME))
		.await
		.ok()
		.map(|bytes| serde_json::from_slice(&bytes))
		.transpose()
		.map_err(|source| tg::error!(!source, "failed to deserialize lock"))
		.unwrap();

	(object, metadata, lock)
}
