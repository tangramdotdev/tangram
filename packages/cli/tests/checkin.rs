use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::path::Path;
use tangram_cli_test::{Server, assert_success};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let artifact = temp::directory! {
		"directory" => temp::directory! {
			"hello.txt" => "Hello, world!",
			"link" => temp::symlink!("hello.txt"),
			"subdirectory" => temp::directory! {
				"sublink" => temp::symlink!("../link"),
			}
		}
	}
	.into();
	let path = Path::new("directory");
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
	  "weight": 439
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn file() {
	let artifact = temp::directory! {
		"directory" => temp::directory! {
			"README.md" => "Hello, World!",
		}
	}
	.into();
	let path = Path::new("directory");
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
	  "weight": 173
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn symlink() {
	let artifact = temp::directory! {
		"directory" => temp::directory! {
			"link" => temp::symlink!("."),
		}
	}
	.into();
	let path = Path::new("directory");
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
	  "weight": 93
	}
	"#);
	assert!(lock.is_none());
}

#[tokio::test]
async fn directory_with_duplicate_entries() {
	let artifact = temp::directory! {
		"directory" => temp::directory! {
			"a.txt" => "Hello, World!",
			"b.txt" => "Hello, World!",
		}
	}
	.into();
	let path = Path::new("directory");
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
	  "weight": 238
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
	  "weight": 394
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
	  "weight": 694
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
	  "weight": 637
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
	  "weight": 466
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
	  "weight": 527
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
	}
	.into();
	let path = Path::new("foo");
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
		"directory" => temp::directory! {
			"tangram.ts" => "export default () => {};",
		}
	}
	.into();
	let path = Path::new("directory");
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
		"directory" => temp::directory! {
			"a" => temp::directory! {
				"mod.tg.ts" => r#"import a from ".";"#
			},
		}
	}
	.into();
	let path = Path::new("directory");
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
		"directory" => temp::directory! {
			"a" => temp::directory! {
				"mod.tg.ts" => r#"import * as a from ".";"#,
				"tangram.ts" => ""
			},
		}
	}
	.into();
	let path = Path::new("directory");
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
		"directory" => temp::directory! {
			"a" => temp::directory! {},
			"tangram.ts" => r#"import a from "./a";"#,
		}
	}
	.into();
	let path = Path::new("directory");
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
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a" with { type: "directory" };"#,
		}
	}
	.into();
	let path = Path::new("directory");
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
		"directory" => temp::directory! {
			"a" => temp::directory!{
				"tangram.ts" => "",
			},
			"tangram.ts" => r#"import a from "./a";"#,
		}
	}
	.into();
	let path = Path::new("directory");
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
		"package" => temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts";"#,
			"foo.tg.ts" => r#"import * as root from "./tangram.ts";"#,
		}
	}
	.into();
	let path = Path::new("package");
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
	          "item": "fil_01b64fk2r3af0mp8wek1630m1k57bq8fqp0yvqjq7701b3tngbfyxg",
	          "options": {
	            "tag": "hello"
	          }
	        }
	      }
	    }
	  ]
	}
	"#);
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
	let artifact = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
			export default tg.command(async () => {
				return await a();
			});
		"#)
	}
	.into();
	let destructive = false;
	let path = Path::new("");
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
	            "tag": "a"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": "fil_01wfv1nny15t09ts6estb4pw7qsz3j7bqq2wyfyhapnydarven8jng"
	      }
	    }
	  ]
	}
	"#);
}

#[tokio::test]
async fn tagged_package_with_cyclic_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import a from "a";
		"#),
	}
	.into();
	let path = Path::new("");
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
	            "tag": "a"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "foo.tg.ts": {
	          "node": 3
	        },
	        "tangram.ts": {
	          "node": 4
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "contents": "blb_01ycqx996y57ta8qpg72zsn6g446x37htxw7v3se7xmaa5nwrwx2t0",
	      "dependencies": {
	        "./tangram.ts": {
	          "item": {
	            "node": 4
	          },
	          "options": {
	            "path": "tangram.ts"
	          }
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "contents": "blb_018dz0kathbh9mktnftc933pd35gy651y1ppqe3tg2q0an0dt35zr0",
	      "dependencies": {
	        "./foo.tg.ts": {
	          "item": {
	            "node": 3
	          },
	          "options": {
	            "path": "foo.tg.ts"
	          }
	        }
	      }
	    }
	  ]
	}
	"#);
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

	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as b from "b/*";
			import * as a from "a/*";
		"#),
	}
	.into();

	let path = Path::new("");
	let destructive = false;
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	                  "foo.tg.ts": {
	                    "node": 1,
	                  },
	                  "tangram.ts": {
	                    "node": 2,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
	                "dependencies": {
	                  "./tangram.ts": {
	                    "item": {
	                      "node": 2,
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
	                      "node": 1,
	                    },
	                    "path": "foo.tg.ts",
	                  },
	                  "a/*": {
	                    "item": {
	                      "node": 3,
	                    },
	                    "tag": "a/1.1.0",
	                  },
	                },
	              },
	              {
	                "kind": "directory",
	                "entries": {
	                  "tangram.ts": {
	                    "node": 4,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"b/*\";\n"),
	                "dependencies": {
	                  "b/*": {
	                    "item": {
	                      "node": 0,
	                    },
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
	                  "foo.tg.ts": {
	                    "node": 1,
	                  },
	                  "tangram.ts": {
	                    "node": 2,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"./tangram.ts\";\n"),
	                "dependencies": {
	                  "./tangram.ts": {
	                    "item": {
	                      "node": 2,
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
	                      "node": 1,
	                    },
	                    "path": "foo.tg.ts",
	                  },
	                  "a/*": {
	                    "item": {
	                      "node": 3,
	                    },
	                    "tag": "a/1.1.0",
	                  },
	                },
	              },
	              {
	                "kind": "directory",
	                "entries": {
	                  "tangram.ts": {
	                    "node": 4,
	                  },
	                },
	              },
	              {
	                "kind": "file",
	                "contents": tg.blob("import * as b from \"b/*\";\n"),
	                "dependencies": {
	                  "b/*": {
	                    "item": {
	                      "node": 0,
	                    },
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
	        "a/*": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "tag": "a/1.1.0"
	          }
	        },
	        "b/*": {
	          "item": {
	            "node": 4
	          },
	          "options": {
	            "tag": "b/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 3
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "contents": "blb_0158re2012fvbq8s0zxgsdmkmg7k05y79mnbeha500h9k973hk06k0",
	      "dependencies": {
	        "b/*": {
	          "item": {
	            "node": 4
	          },
	          "options": {
	            "tag": "b/1.0.0"
	          }
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
	      "contents": "blb_01mv4a5380n5nacg4cvgh1r1f0vcrk489j6rfsj031gxp9b8t9gxq0",
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
	      "contents": "blb_01ajr136dx93ph4zrx9eqbq5gz05gh530ew34qzh0dgh4jbvvx6m30",
	      "dependencies": {
	        "./foo.tg.ts": {
	          "item": {
	            "node": 5
	          },
	          "options": {
	            "path": "foo.tg.ts"
	          }
	        },
	        "a/*": {
	          "item": {
	            "node": 2
	          },
	          "options": {
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
		"tangram.ts" => indoc::indoc!(r#"
			import b from "b";
			import c from "c";
		"#),
	}
	.into();
	let path = Path::new("");
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
	let (object, _metadata, lock) = test(artifact, path, destructive, tags).await;
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
	            "tag": "b"
	          }
	        },
	        "c": {
	          "item": {
	            "node": 5
	          },
	          "options": {
	            "tag": "c"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 3
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "contents": "blb_01dra984m32dp47yvrynmkcxzcxmgmptmgnyhb8xk6th2wk3wgkae0",
	      "dependencies": {
	        "a/^1": {
	          "item": {
	            "node": 4
	          },
	          "options": {
	            "tag": "a/1.1.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": "fil_01es7rz5mgcv9zas8dnavagvd3j80nz8vzvgar41fw7xt2b66qa3j0"
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": {
	          "node": 6
	        }
	      }
	    },
	    {
	      "kind": "file",
	      "contents": "blb_01xv04mkcazz11b2wsjy0kp279772f9wb2gbsn2maer3qxhbdvvppg",
	      "dependencies": {
	        "a/^1.0": {
	          "item": {
	            "node": 4
	          },
	          "options": {
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
		"tangram.ts" => indoc::indoc!(r#"
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
	)
	.await;
	let (object_output2, _metadata_output2, _lockfile2) =
		test_inner(&local_server2, artifact.clone(), path, destructive, tags).await;
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
	let referrer: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
			import foo from "foo";
			export default () => foo();
		"#)
	}
	.into();
	let path = Path::new("");
	let destructive = false;
	let tags = vec![];
	let (output1, _, _) = test_inner(&server2, referrer.clone(), path, destructive, tags).await;

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
	let (output2, _, _) = test_inner(&server2, referrer.clone(), path, destructive, tags).await;

	// Confirm the outputs are the same.
	assert_eq!(output1, output2);
}

#[ignore = "unimplemented"]
#[tokio::test]
async fn update_tagged_package() {
	let server = Server::new(TG).await.unwrap();

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

	let local: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc::indoc!(r#"
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
	        "a/^1": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "tag": "a/1.0.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": "fil_0124wz8r6hhv205c0xxe0k2dq20yrbs71cpjr74np8rhrtak3jnv1g"
	      }
	    }
	  ]
	}
	"#);

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
	        "a/^1": {
	          "item": "dir_013a21q59fsmhghm22hyzfnbh9c9j0h4h1v6ep59t8f7tyvs8vdpeg",
	          "options": {
	            "tag": "a/1.0.0"
	          }
	        }
	      }
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
	        "a/^1": {
	          "item": {
	            "node": 2
	          },
	          "options": {
	            "tag": "a/1.1.0"
	          }
	        }
	      }
	    },
	    {
	      "kind": "directory",
	      "entries": {
	        "tangram.ts": "fil_01es7rz5mgcv9zas8dnavagvd3j80nz8vzvgar41fw7xt2b66qa3j0"
	      }
	    }
	  ]
	}
	"#);
}

async fn test(
	artifact: temp::Artifact,
	path: &Path,
	destructive: bool,
	tags: Vec<(String, temp::Artifact)>,
) -> (String, String, Option<tg::graph::Data>) {
	let server = Server::new(TG).await.unwrap();
	test_inner(&server, artifact, path, destructive, tags).await
}

async fn test_inner(
	server: &Server,
	artifact: temp::Artifact,
	path: &Path,
	destructive: bool,
	tags: Vec<(String, temp::Artifact)>,
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
