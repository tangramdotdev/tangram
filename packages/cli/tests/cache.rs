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
	    "dir_01pwxwwvy02jgj944jbsf3a9e8ck2yaz3nfs3bxgk221tm5tkdhq40": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01bh4xqfmywbbbka3r71vkjn3v2my402dv65dd657kd5yf33ppmx60": {
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
	    "fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60": {
	      "kind": "file",
	      "contents": "Hello, World!"
	    },
	    "fil_01608j8dp7x2wn515t0k0n04j8442k49gbthds6v2my3rnc8bkxvw0": {
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
	    "fil_01qdmew2fxgyx15vq2zh2726pvhbfpa6jzvhrr7sn1pv0dpt9ms0vg": {
	      "kind": "file",
	      "contents": "Hello, World!",
	      "executable": true
	    },
	    "fil_01vkzst22p1wrv4cpztveh71b13q5v3bnw1nf2y6a14315cs543qrg": {
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
	    "dir_018zb48hy214fgq3gnnbjcpdcer4ft89b366n5hp3d2ea2zm3awjsg": {
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
	    "fil_01h477wadj6184q5yn7kwg74d1knd2486bg9mc3rrw79estx8x9cf0": {
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
	    "fil_011fd0zvc0853ztfq0sm0p8gxf3w874a6zwfmsf14bgm8way2yj8eg": {
	      "kind": "file",
	      "contents": "foo",
	      "xattrs": {
	        "user.tangram.dependencies": "[\"bar\"]"
	      }
	    },
	    "fil_017kwmd39nc52547zwm6bxffv3ds9p2gvbq9byg9marzgd5pmtb9qg": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.file(\"foo\", {\n\t\tdependencies: {\n\t\t\t\"bar\": tg.file(\"bar\"),\n\t\t},\n\t});\n}\n"
	    },
	    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
	      "kind": "file",
	      "contents": "bar"
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
	    "fil_01cdcq0v3p35pybv6p6331hmnzbkqrezjsbhbt18vtvf4pm5bpnq2g": {
	      "kind": "file",
	      "contents": "export default () => {\n\treturn tg.symlink(\"/bin/sh\");\n}\n"
	    },
	    "sym_01sjeq01fm5g3jqq57bams2kxsprz7e8mhtpkbagxhvb1xg8f12z2g": {
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
	    "dir_014fdgcek7ms0ztrzvcf1phxcz6d29bedrxnmkf7r5c8xy5a33sh40": {
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
	    "fil_01fqzdrbc6qyzxxc2adwde0z9m79jhe2a524bf6zrx62vqfseh0yfg": {
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
	    "dir_01kepgqkqmm2f3am76vhcf612mmjppedkjpqdmrve5zqk5enhx1tyg": {
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
	    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01y7yazf4k4gtrq98c6epn7kreq0dz7p5p46h6nf5kee6frraxzcg0": {
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
	    "dir_013da6hc37p6py2gq37em7npkhhxrr145ec6aw9qenx6d62nftbwsg": {
	      "kind": "directory",
	      "entries": {
	        "foo": {
	          "kind": "symlink",
	          "path": "../fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg"
	        }
	      }
	    },
	    "fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg": {
	      "kind": "file",
	      "contents": "bar"
	    },
	    "fil_01qyh30tqbd6agrmpwsghnezq8e38bpkh13vh2k5shfjq0sk4gnbgg": {
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
	    "dir_01xq6jv8d4ye1qkxf3kmc0jzm2szavhfypem762rms22sqmqaeab2g": {
	      "kind": "directory",
	      "entries": {
	        "hello.txt": {
	          "kind": "file",
	          "contents": "Hello, World!"
	        }
	      }
	    },
	    "fil_01d1pdpc61qdqcf0rcmb3kx7xn0511x40shmc9sy2rbq665bvwyzr0": {
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
	    "fil_01vkad95n9t0h86jpfy594hdaezeddts8z6f3bgv1t05hqt64kzrb0": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"file\",\n\t\t\t\tcontents: \"Hello, World!\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.file({ graph, node: 0 });\n}\n"
	    },
	    "fil_01yw9bpm8d571j3jg3pqx9xsa087s8w3tpf0ewr7jcq53gvpe2hmjg": {
	      "kind": "file",
	      "contents": "Hello, World!"
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
	    "fil_01vggpszzs7vhc58rfv8jabaxvvctsqtdtyychxz6t6729k6j8f1kg": {
	      "kind": "file",
	      "contents": "export default () => {\n\tlet graph = tg.graph({\n\t\tnodes: [\n\t\t\t{\n\t\t\t\tkind: \"symlink\",\n\t\t\t\tpath: \"/bin/sh\",\n\t\t\t}\n\t\t]\n\t});\n\treturn tg.symlink({ graph, node: 0 });\n}\n"
	    },
	    "sym_01xcz8v0pcvedwb43gx6dcrdy7w87rnmv3wfyc6s2zsmchme6628cg": {
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
	    "dir_01vnd6dch3m0bwf5x6vmdy8dt5zqf9y0jkhcn058sekx8zce2a22x0": {
	      "kind": "directory",
	      "entries": {
	        "link": {
	          "kind": "symlink",
	          "path": "link"
	        }
	      }
	    },
	    "fil_01hwhqwa32gx974z2vgyg74ztxq9ps7792c2bjcedywnykqn9wd4hg": {
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
