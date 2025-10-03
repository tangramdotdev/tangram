use {
	indoc::indoc,
	insta::assert_json_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test caching a directory.
#[tokio::test]
async fn directory() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_0161140hvfyh9k9x0wmw1ad9cnghemznwsn4p5n2qfvvjr6n6a9fd0": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01vjrnapxxd037hpq3sbd2wb9bp5czqtavp57ve9x6ap6dxdgbbf8g": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\t\"hello.txt\": \"Hello, World!\",\n\t});\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a file.
#[tokio::test]
async fn file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("Hello, World!");
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_0161g41yea30wb48ta1dt778xfgfxrm09e1p1dznezech34e27tp60": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    },
	    "fil_01cdpb7y42700ea0vpjjdd99r7dbt2dw97z8r86w0nxj05ypc5pwm0": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"Hello, World!\");\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching an executable file.
#[tokio::test]
async fn executable_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("Hello, World!", { executable: true });
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_018gyv78y738ns83fmn1rrsvfs8fey4dqv8ygbx02khbka4kh67wc0": {
	      "kind": "file",
	      "contents": "Hello, World!",
	      "executable": true
	    },
	    "fil_01sypjs1f3tceazrht9bcq8zhtp7hkbzeys0sv23g5yytd3p266ksg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"Hello, World!\", { executable: true });\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a directory with two identical files.
#[tokio::test]
async fn directory_with_two_identical_files() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": "Hello, World!",
					"world.txt": "Hello, World!",
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_015x7vb1f3wfv5h55h7s72e0x04ns0ak9bs0ayxm5hxp71dnkbkqzg": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        },
	        "world.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01xn2kf70vf7xfa9523df88b16r4wzf2dawhw5twvcthvk8evygjeg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\t\"hello.txt\": \"Hello, World!\",\n\t\t\"world.txt\": \"Hello, World!\",\n\t});\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a file with a dependency.
#[tokio::test]
async fn file_with_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.file("foo", {
					dependencies: {
						"bar": tg.file("bar"),
					},
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01epw23z8dy6w1w2tfvhzs8mdtr6zye7ackxcdhkjt7174bd5y73jg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"foo\", {\n\t\tdependencies: {\n\t\t\t\"bar\": tg.file(\"bar\"),\n\t\t},\n\t});\n}\n"
	    },
	    "fil_01kkwpqw5jmbt5md0ck62tnwrf5mrn75atrrf05n2338yqrdfc1800": {
	      "kind": "file",
	      "contents": "foo",
	      "xattrs": {
	        "user.tangram.dependencies": "[\"bar\"]"
	      }
	    }
	  }
	}
	"#);
}

/// Test caching a symlink.
#[tokio::test]
async fn symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.symlink("/bin/sh");
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_01j2905wk3r0yr5q75zj04r0mn3s8qvkyaxvfq7bc57mxst4e10wcg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.symlink(\"/bin/sh\");\n}\n"
	    },
	    "sym_01a5tvcfq8dj5pdwe4v5q0g0mjfk2b4daazgb8e9vbsd3ksd1j368g": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
	"#);
}

/// Test caching a directory with a symlink.
#[tokio::test]
async fn directory_with_symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					"hello.txt": tg.file("Hello, World!"),
					"link": tg.symlink("hello.txt"),
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_01mfcyy0ayssk405f66n95a9ead33vmer8y9tvne0xqz01w2t4etxg": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        },
	        "link": {
	          "kind": "symlink",
	          "path": "hello.txt"
	        }
	      }
	    },
	    "fil_01j4xxv6f6stf4z86v1qrapykseyavd50nykezzccdv8jemrz1gs10": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\t\"hello.txt\": tg.file(\"Hello, World!\"),\n\t\t\"link\": tg.symlink(\"hello.txt\"),\n\t});\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a directory with a file with a dependency.
#[tokio::test]
async fn directory_with_file_with_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					foo: tg.file("foo", {
						dependencies: {
							"bar": tg.file("bar"),
						},
					}),
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_01y4pdrkvwrpe63trn2jaqs59fm012wdymntq6nm0qcfd4a0se4d40": {
	      "kind": "directory",
	      "entries": {
	        "foo": {
	          "kind": "file",
	          "contents": "foo",
	          "xattrs": {
	            "user.tangram.dependencies": "[\"bar\"]"
	          }
	        }
	      }
	    },
	    "fil_0101xcwnyk8v568vmb65ytg3ffm0nrzacvzpm8pzw6vtffnc2pwz70": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\tfoo: tg.file(\"foo\", {\n\t\t\tdependencies: {\n\t\t\t\t\"bar\": tg.file(\"bar\"),\n\t\t\t},\n\t\t}),\n\t});\n}\n"
	    },
	    "fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g": {
	      "kind": "file",
	      "contents": "bar"
	    }
	  }
	}
	"#);
}

/// Test caching a directory with a symlink with a dependency.
#[tokio::test]
async fn directory_with_symlink_with_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				return tg.directory({
					foo: tg.symlink(tg.file("bar")),
				});
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_0109qjm6akjjcf25wsyqdpdmegmghkjdtmdb9b6p756yzxt4kdefq0": {
	      "kind": "directory",
	      "entries": {
	        "foo": {
	          "kind": "symlink",
	          "path": "../fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g"
	        }
	      }
	    },
	    "fil_01drxezv07bnpqt9w6jw4hqrc73b1n66y19krh1krscbc307124z2g": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01qt7gahyfjc54fw0f3qcm79ft5bcg8q10qagvjn3zvgpyvxdm0j0g": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\tfoo: tg.symlink(tg.file(\"bar\")),\n\t});\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a directory that is a member of a graph.
#[tokio::test]
async fn graph_directory() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "directory",
							entries: {
								"hello.txt": tg.file("Hello, World!")
							}
						}
					]
				});
				return tg.directory({ graph, node: 0 });
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_013yj7djcxmew6cg7psf5ywc0sbm3bcmnhm07wnz2xjskfs2fmp85g": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01pdv2pv605gzfsq8frpbh2r6s4ms5wbw14qbvdykzxd0nvqgy2g90": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"directory\",\n\t\t\t\tentries: {\n\t\t\t\t\t\"hello.txt\": tg.file(\"Hello, World!\")\n\t\t\t\t}\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.directory({ graph, node: 0 });\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a file that is a member of a graph.
#[tokio::test]
async fn graph_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "file",
							contents: "Hello, World!",
						}
					]
				});
				return tg.file({ graph, node: 0 });
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_01frvykaz0sqdpbcpwncqjrv9m6bqrnykbhs3mj3hq70c9a1430rm0": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    },
	    "fil_01fycg10ma8gthk5fj67y6khanprs5n6aszm3ma89egy07edqkcf2g": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"file\",\n\t\t\t\tcontents: \"Hello, World!\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.file({ graph, node: 0 });\n}\n"
	    }
	  }
	}
	"#);
}

/// Test caching a symlink that is a member of a graph.
#[tokio::test]
async fn graph_symlink() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "symlink",
							path: "/bin/sh",
						}
					]
				});
				return tg.symlink({ graph, node: 0 });
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "fil_01crgmht1kc9kanrnvqff480c3dn68wrq3xa6ajrrpqep1ew113me0": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"symlink\",\n\t\t\t\tpath: \"/bin/sh\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.symlink({ graph, node: 0 });\n}\n"
	    },
	    "sym_01j4terqyd7btz9tm8wsh4ybwavbkt8dnc512p9mewa140hsdg7ydg": {
	      "kind": "symlink",
	      "path": "/bin/sh"
	    }
	  }
	}
	"#);
}

/// Test caching a directory with an artifact symlink that points to itself.
#[tokio::test]
async fn directory_with_symlink_cycle() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let graph = tg.graph({
					nodes: [
						{
							kind: "directory",
							entries: { link: { node: 1 } },
						},
						{
							kind: "symlink",
							artifact: { node: 0 },
							path: "link",
						}
					]
				});
				return tg.symlink({ graph, node: 0 });
			}
		"#),
	}
	.into();
	let artifact = test(artifact).await;
	assert_json_snapshot!(artifact, @r#"
	{
	  "kind": "directory",
	  "entries": {
	    "dir_01jx4k03rrr7z7ma9f0vepqzzj3wysvx7ga73twmmz28r1ywtjn2fg": {
	      "kind": "directory",
	      "entries": {
	        "link": {
	          "kind": "symlink",
	          "path": "link"
	        }
	      }
	    },
	    "fil_01wpf3x2k8gr2gb974hs75ynzzb6pnev5xt2r1zyn9m0ct4t1dq0e0": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"directory\",\n\t\t\t\tentries: { link: { node: 1 } },\n\t\t\t},\n\t\t\t{\n\t\t\t\tkind: \"symlink\",\n\t\t\t\tartifact: { node: 0 },\n\t\t\t\tpath: \"link\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.symlink({ graph, node: 0 });\n}\n"
	    }
	  }
	}
	"#);
}

async fn test(artifact: temp::Artifact) -> temp::Artifact {
	let server = Server::new(TG).await.unwrap();

	// Write the artifact.
	let temp = Temp::new();
	artifact.to_path(temp.path()).await.unwrap();

	// Build the module.
	let output = server
		.tg()
		.arg("build")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let id = std::str::from_utf8(&output.stdout).unwrap().trim();

	// Cache the artifact.
	let mut command = server.tg();
	command.arg("cache").arg(id);
	let output = command.output().await.unwrap();
	assert_success!(output);

	temp::Artifact::with_path(&server.temp().path().join(".tangram/artifacts"))
		.await
		.unwrap()
}
