use indoc::indoc;
use insta::assert_json_snapshot;
use tangram_cli_test::{Server, assert_success};
use tangram_temp::{self as temp, Temp};

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
	    "dir_015nxhedsebk1vassrgs1kypbdfg56j3v49k3v0e6n899gny8evjng": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01ab391rmw9mcb7ra82y741x6shh96tg7g63djndaemaq1gz2zrd50": {
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
	    "fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    },
	    "fil_01we83e22gns6qqy6j7ygxkvpgs5cywfbzv2hkyd94n16j5rv0m0tg": {
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
	    "fil_0199fj35x8ck4wcq13bdvwkg5www2jvvc269r77kw6ttj85g9sjcfg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"Hello, World!\", { executable: true });\n}\n"
	    },
	    "fil_01jt5z2knevk3bmxj738veen9qtc674y9d33nrjk19pqc42fez6kjg": {
	      "kind": "file",
	      "contents": "Hello, World!",
	      "executable": true
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
	    "dir_01b8yvtd48c6k6rvm3fkfj870qn56zesp2593t1sjkm7qf7cbd1mr0": {
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
	    "fil_019zw38hy6s55emdrt5c1sn8zqhyq5gzy9n94vnmt761fg6b7zg0e0": {
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
	    "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01amfa43614nxvbezdxn3mrt9m65gg037tgvw91ecbra521jhj6mc0": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"foo\", {\n\t\tdependencies: {\n\t\t\t\"bar\": tg.file(\"bar\"),\n\t\t},\n\t});\n}\n"
	    },
	    "fil_01zypw2x59k9q5g4at7nqs2kdeqdv21583435kxx9rsjz8z3fe17n0": {
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
	    "fil_0100cegr3mk77acak3kp8nmdvxkvp5egt5sa1jnt61xfq91q71p10g": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.symlink(\"/bin/sh\");\n}\n"
	    },
	    "sym_01wcsacv99pffr6zgsndnehtpqjeds7scp617mjr355b2797b3nzmg": {
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
	    "dir_01wrj3pfw1qdyvht69c0d2srxc6b7fax0p1y9ekh7bke6caf1gkkjg": {
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
	    "fil_01h9219whd8eh68hwp674s9jbwd34nn5g9h5gh589athfg5z2z71tg": {
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
	    "dir_01zexm3zwna9arqk6kb15vp40sqtrj7nzq07pcxqs2japt89ty9pg0": {
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
	    "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01nt6dnj1pgg71k8jqjjy23z31tfrhd2j6gh22b6dsc5dn2qk8z450": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.directory({\n\t\tfoo: tg.file(\"foo\", {\n\t\t\tdependencies: {\n\t\t\t\t\"bar\": tg.file(\"bar\"),\n\t\t\t},\n\t\t}),\n\t});\n}\n"
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
	    "dir_01m94jq9ya92azhgrkj09c5k9axk3ybqmv0wz3qmf26vyqh74qjrz0": {
	      "kind": "directory",
	      "entries": {
	        "foo": {
	          "kind": "symlink",
	          "path": "../fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg"
	        }
	      }
	    },
	    "fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01js4h4ght83twa65g2h81z43shfybxb1v4rwcka5nzm4rgbaytjm0": {
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
	    "dir_017r9tkq9cced8gdp62p83dx1s2ascadrgxwh8m81twxqv57xrjedg": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01e274jafxy40xthwexrmnqyaz01qzn25z9qebmkmme1pxbn4jnx90": {
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
	    "fil_017jbt8bpwx6mfe2vzw0f7syajknz89jx1pjfpzcnf36ab647tams0": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    },
	    "fil_01ecd5b9abhhq8ntrvjjpg2t7tqygx378fx29697553wcxmsxtsch0": {
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
	    "fil_01bd942ejhym84rqdpmryxk41qqtp1bhgpes0ry481gag4wp6r3ghg": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"symlink\",\n\t\t\t\tpath: \"/bin/sh\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.symlink({ graph, node: 0 });\n}\n"
	    },
	    "sym_01tka6c2r1f2367zvznc58mhscp372kdbqk79w121j00grn2fa0d5g": {
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
	    "dir_01h6rfmj742k64qgyc30yk88vefpm24afvhv5z70pqjqwa46x4mgm0": {
	      "kind": "directory",
	      "entries": {
	        "link": {
	          "kind": "symlink",
	          "path": "link"
	        }
	      }
	    },
	    "fil_01jc7emyabnm7d89w79z0nsv3m275qx0p906bdcj36gwn9f75ys47g": {
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
