use {
	indoc::indoc,
	insta::assert_snapshot,
	num::ToPrimitive as _,
	tangram_cli_test::{Server, assert_failure, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"export default () => "Hello, World!""#,
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!""#);
}

#[tokio::test]
async fn hello_world_remote() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"export default () => "Hello, World!""#,
	}
	.into();
	let reference = ".";
	let (local, remote) = test_remote(artifact, reference).await;
	assert_success!(local);
	assert_success!(remote);
	let local = std::str::from_utf8(&local.stdout).unwrap();
	let remote = std::str::from_utf8(&remote.stdout).unwrap();
	assert_snapshot!(local, @r#""Hello, World!""#);
	assert_snapshot!(remote, @r#""Hello, World!""#);
}

/// Test building a module without a package.
#[tokio::test]
async fn module_without_package() {
	let artifact = temp::directory! {
		"foo.tg.ts" => indoc!(r#"
			export default () => "Hello, World!";
		"#),
	}
	.into();
	let reference = "./foo.tg.ts";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!""#);
}

#[tokio::test]
async fn no_return_value() {
	let artifact = temp::directory! {
		"tangram.ts" => r"export default () => {};",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"");
}

#[tokio::test]
async fn args() {
	let artifact = temp::directory! {
		"tangram.ts" => r"export default (name: string) => `Hello, ${name}!`;",
	}
	.into();
	let reference = ".";
	let args = vec!["Tangram".into()];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, Tangram!""#);
}

#[tokio::test]
async fn host_command_hello_world() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT");
			};
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01rrfg6pn5b8ve55qe70rrdx4tbwy57rrpbe8ebhe27cg0vc43dz4g");
}

#[tokio::test]
async fn host_command_hello_world_remote() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT");
			};
		"#),
	}
	.into();
	let reference = ".";
	let (local, remote) = test_remote(artifact, reference).await;
	let stdout = std::str::from_utf8(&local.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01rrfg6pn5b8ve55qe70rrdx4tbwy57rrpbe8ebhe27cg0vc43dz4g");
	let stdout = std::str::from_utf8(&remote.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01rrfg6pn5b8ve55qe70rrdx4tbwy57rrpbe8ebhe27cg0vc43dz4g");
}

#[tokio::test]
async fn two_modules() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import bar from "./bar.tg.ts";
			export default () => tg.run(bar);
		"#),
		"bar.tg.ts" => r#"export default () => "Hello from bar""#,
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello from bar""#);
}

#[tokio::test]
async fn path_dependency() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => tg.run(bar);
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => r#"export default () => "Hello from bar";"#
		}
	}
	.into();
	let reference = "./foo";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello from bar""#);
}

#[tokio::test]
async fn path_dependency_import_attribute() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "bar" with { local: "../bar" };
				export default () => tg.run(bar);
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => r#"export default () => "Hello from bar";"#
		}
	}
	.into();
	let reference = "./foo";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello from bar""#);
}

#[tokio::test]
async fn named_command() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export let five = () => 5;
			export let six = () => 6;
		"),
	}
	.into();
	let reference = ".#five";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"5");
}

#[tokio::test]
async fn concurrent_commands() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default async () => {
				let results = await Promise.all(Array.from(Array(100).keys()).map((i) => tg.run(double, i)));
				return results.reduce((acc, el) => acc + el, 0);
			};
			export let double = (i: number) => i * 2;
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"9900");
}

#[tokio::test]
async fn capture_error() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"export default () => { throw new error("not so fast!"); };"#,
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn import_file() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./hello.txt";
			export default () => file.text();
		"#),
		"hello.txt" => "Hello, World!",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!""#);
}

#[tokio::test]
async fn import_directory() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import directory from "./directory" with { type: "directory" };
			export default async () =>
				directory.get("hello.txt")
					.then(tg.File.expect)
					.then((f) => f.text())
			;
		"#),
		"directory" => temp::directory! {
			"hello.txt" => "Hello, World!",
		}
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!""#);
}

#[tokio::test]
async fn template_raw() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => tg.Template.raw`\n\tHello, World!\n`;
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template(["\n\tHello, World!\n"])"#);
}

#[tokio::test]
async fn template_single_line() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./hello.txt";
			export default () => tg`cat ${file}`;
		"#),
		"hello.txt" => "Hello, World!",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template(["cat ",fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00])"#);
}

#[tokio::test]
async fn template_with_quote() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./hello.txt";
			export default () => tg`
				other_command

				other_command

				other_command

				echo 'exec ${file} "$@"' >> script.sh
			`;
		"#),
		"hello.txt" => "Hello, World!",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template(["other_command\n\nother_command\n\nother_command\n\necho 'exec ",fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00," \"$@\"' >> script.sh\n"])"#);
}

#[tokio::test]
async fn template_single_line_two_artifacts() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "./foo.txt";
			import bar from "./bar.txt";
			export default () => tg`${foo} ${bar}`;
		"#),
		"foo.txt" => "foo",
		"bar.txt" => "bar",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template([fil_0167z9jdx71jkf1paq2b43shcezqrz4m19qhr8nec8zwnwgdjt89z0," ",fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg])"#);
}

#[tokio::test]
async fn template_empty_lines() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => tg`
				function foo() {
					echo "Hello, World!"

				}
			`;
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template(["function foo() {\n\techo \"Hello, World!\"\n\n}\n"])"#);
}

#[tokio::test]
async fn template_only_placeholders_on_a_line() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./hello.txt";
			export default () => tg`
				${file}${file}
			`;
		"#),
		"hello.txt" => "Hello, World!",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template([fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00,fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00,"\n"])"#);
}

#[tokio::test]
async fn template_single_line_explicit_newline() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "./foo.txt";
			import bar from "./bar.txt";
			export default () => tg`${foo}\n${bar}`;
		"#),
		"foo.txt" => "foo",
		"bar.txt" => "bar",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template([fil_0167z9jdx71jkf1paq2b43shcezqrz4m19qhr8nec8zwnwgdjt89z0,"\n",fil_014fr29kstq7da0d7q658r9ahm279znng5fhxfzph8bhdr40g71agg])"#);
}

#[tokio::test]
async fn template_multiple_placeholders() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file1 from "./hello.txt";
			import file2 from "./hello.txt";
			import file3 from "./hello.txt";
			export default () => tg`
				cat\t${file1}\t${file1}
			`;
		"#),
		"hello.txt" => "Hello, World!",
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#"tg.template(["cat\t",fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00,"\t",fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00,"\n"])"#);
}

#[tokio::test]
async fn blob_template() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => tg.blob`\n\tHello, World!\n`.then((b) => b.text());
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!\n""#);
}

#[tokio::test]
async fn blob_template_two_placeholders() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let a = "string!";
				return tg.blob`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
			}
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!\nstring!\n""#);
}

#[tokio::test]
async fn blob_template_raw() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => tg.Blob.raw`\n\tHello, World!\n`.then((b) => b.text());
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""\n\tHello, World!\n""#);
}

#[tokio::test]
async fn file_template() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => tg.file`\n\tHello, World!\n`.then((f) => f.text());
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!\n""#);
}

#[tokio::test]
async fn file_template_two_placeholders() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				let a = "string!";
				return tg.file`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
			}
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""Hello, World!\nstring!\n""#);
}

#[tokio::test]
async fn file_template_raw() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default () => tg.File.raw`\n\tHello, World!\n`.then((f) => f.text());
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""\n\tHello, World!\n""#);
}

#[tokio::test]
async fn directory_get_follows_intermediate_component_symlinks() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import directory from "./directory" with { type: "directory" };
			export default async () => {
				let file = await directory.get("link/hello.txt");
				tg.File.assert(file);
				return file.text();
			};
		"#),
		"directory" => temp::directory! {
			"hello.txt" => "foo",
			"link" => temp::symlink!(".")
		}
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""foo""#);
}

#[tokio::test]
async fn directory_get_follows_final_component_symlinks() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import directory from "./directory" with { type: "directory" };
			export default async () => {
				let file = await directory.get("link");
				tg.File.assert(file);
				return file.text();
			};
		"#),
		"directory" => temp::directory! {
			"hello.txt" => "foo",
			"link" => temp::symlink!("hello.txt")
		}
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""foo""#);
}

#[tokio::test]
async fn cycle() {
	let artifact = temp::directory! {
		"tangram.ts" => "export let x = () => tg.build(x);"
	}
	.into();
	let reference = ".#x";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn cycle_between_packages() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => tg.build(bar);
			"#)
		},
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "../foo";
				export default () => tg.build(foo);
			"#)
		}
	}
	.into();
	let reference = "./foo";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn import_cycle_without_process_cycle() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => tg.build(bar);
				export let greeting = () => "foo";
			"#)
		},
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import * as foo from "../foo";
				export default () => tg.build(foo.greeting);
			"#)
		}
	}
	.into();
	let reference = "./foo";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r###""foo""###);
}

#[tokio::test]
async fn value_cycle_detection_object() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!("
			export default () => {
				let x = {};
				x.a = x;
				return x;
			};
		"),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn value_cycle_detection_array() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!("
			export default () => {
				let x = [];
				x[0] = x;
				return x;
			};
		")
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn builtin_download_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://www.example.com", "sha256:any");
				return tg.file(blob);
			};
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01raxbsz7jdc6ys4r5ajg9zgb4jhm04zyh360ahp9n5rpn5nnmmvr0");
}

#[tokio::test]
async fn builtin_download_exact_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://www.example.com", "sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9");
				return tg.file(blob);
			};
		"#),
	}.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01raxbsz7jdc6ys4r5ajg9zgb4jhm04zyh360ahp9n5rpn5nnmmvr0");
}

#[tokio::test]
async fn builtin_download_rejects_incorrect_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://www.example.com", "sha256:0000000000000000000000000000000000000000000000000000000000000000");
				return tg.file(blob);
			};
		"#),
	}.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn builtin_download_rejects_malformed_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://www.example.com", "nonsense");
				return tg.file(blob);
			};
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn command_none_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT", { checksum: "none" });
			};
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn command_set_checksum() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT", { checksum: "sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017" });
			};
		"#),
	}.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_failure!(output);
}

#[tokio::test]
async fn builtin_artifact_archive_extract_simple_dir_roundtrip_tar() {
	let module = indoc!(
		r#"
			export default async () => {
				let artifact = await tg.directory({
					"hello.txt": "contents",
					"link": tg.symlink("./hello.txt"),
				});
				let archive = await tg.archive(artifact, "format");
				let extracted = await tg.extract(archive);
				tg.assert(extracted.id === artifact.id);
			};
		"#
	);

	let format = "tar";
	let output = test_archive(module, format).await;
	assert_success!(output);
}

#[ignore = "zip is not working"]
#[tokio::test]
async fn builtin_artifact_archive_extract_simple_dir_roundtrip_zip() {
	let module = indoc!(
		r#"
			export default async () => {
				let artifact = await tg.directory({
					"hello.txt": "contents",
					"link": tg.symlink("./hello.txt"),
				});
				let archive = await tg.archive(artifact, "format");
				let extracted = await tg.extract(archive);
				tg.assert(extracted.id === artifact.id);
			};
		"#
	);

	let format = "zip";
	let output = test_archive(module, format).await;
	assert_success!(output);
}

async fn test_archive(module: &str, format: &str) -> std::process::Output {
	let module = module.replace("format", format);
	let artifact = temp::directory! {
		"tangram.ts" => module,
	}
	.into();
	let reference = ".";
	let args = vec![];
	test(artifact, reference, args).await
}

#[tokio::test]
async fn import_from_tag() {
	// Create a remote server.
	let remote_server = Server::new(TG).await.unwrap();

	// Create a local server.
	let local_server = Server::new(TG).await.unwrap();
	let output = local_server
		.tg()
		.arg("remote")
		.arg("put")
		.arg("default")
		.arg(remote_server.url().to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Create a package and tag it.
	let foo = temp::directory! {
		"tangram.ts" => temp::file!(r#"export default () => "foo";"#)
	};
	let artifact: temp::Artifact = foo.into();
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();
	let tag = "foo";
	let output = remote_server
		.tg()
		.arg("tag")
		.arg(tag)
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Create a package that imports the other package.
	let bar = temp::directory! {
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "foo";
				export default () => foo();
			"#)
		}
	};
	let artifact: temp::Artifact = bar.into();
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();
	let path = temp.path().join("bar");
	let output = local_server
		.tg()
		.arg("build")
		.arg(path.clone())
		.output()
		.await
		.unwrap();
	assert_success!(output);
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(),@r#""foo""#);
}

#[tokio::test]
async fn import_from_tag_with_nested_module() {
	// Create a local server.
	let server = Server::new(TG).await.unwrap();

	// Create a package and tag it.
	let a = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export { foo } from "./foo/foo.tg.ts";
			export { bar } from "./bar.tg.ts";
			export const baz = () => "baz";
		"#),
		"bar.tg.ts" => indoc!(r#"
			import * as b from "./tangram.ts";
			export const bar = () => b.foo();
		"#),
		"foo" => temp::directory! {
			"foo.tg.ts" => indoc!(r#"
				export const foo = () => "foo";
			"#)
		}
	};
	let artifact: temp::Artifact = a.into();
	let a_temp = Temp::new();
	artifact.to_path(&a_temp).await.unwrap();
	let tag = "a";
	let output = server
		.tg()
		.arg("tag")
		.arg(tag)
		.arg(a_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Create a package that imports a by tag.
	let b = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as a from "a";
			export default async () => {
			  return a.bar();
			};
		"#)
	};
	let artifact: temp::Artifact = b.into();
	let b_temp = Temp::new();
	artifact.to_path(&b_temp).await.unwrap();
	let output = server
		.tg()
		.arg("build")
		.arg(b_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Mutate b_temp/tangram.ts to instead call a.baz();
	let tangram_ts_path = b_temp.path().join("tangram.ts");
	let new_content = indoc!(
		r#"
			import * as a from "a";
			export default async () => {
			  return a.baz();
			};
		"#
	);
	std::fs::write(&tangram_ts_path, new_content).unwrap();
	// Rebuild.
	let output = server
		.tg()
		.arg("build")
		.arg(b_temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);
}

#[tokio::test]
async fn builtin_blob_compress_decompress_gz_roundtrip() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.blob("contents");
				let compressed = await tg.compress(blob, "gz");
				let decompressed = await tg.decompress(compressed, "gz");
				return blob.text();
			};
		"#),
	}
	.into();
	let reference = ".";
	let args = vec![];
	let output = test(artifact, reference, args).await;
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @r#""contents""#);
}

#[tokio::test]
async fn signal_cacheable_processs() {
	let server = Server::new(TG).await.unwrap();
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export let foo = async () => {
				await tg.sleep(1);
				return 42;
			};
		")
	};
	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();
	let output = server
		.tg()
		.arg("build")
		.arg("--detach")
		.arg(format!("{}#foo", temp.path().display()))
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let process = String::from_utf8(output.stdout).unwrap();
	let process = process.trim();
	let output = server
		.tg()
		.arg("signal")
		.arg(process)
		.output()
		.await
		.unwrap();
	assert_failure!(output);
	let error_msg = String::from_utf8(output.stderr).unwrap();

	insta::with_settings!({
		filters => vec![(process, "[PROCESS]")],
	}, {
		assert_snapshot!(error_msg, @r"
		error failed to run the command
		-> failed to post process signal
		-> cannot signal cacheable processes
		   id = [PROCESS]
		");
	});
}

#[tokio::test]
async fn single_cancellation() {
	let server = Server::new(TG).await.unwrap();

	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export let foo = async () => {
				return await tg.build(baz);
			};

			export let bar = async () => {
				return await tg.build(baz);
			};

			export let baz = async () => {
				await tg.sleep(2);
				return 42;
			};
		")
	};
	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();

	let foo = server
		.tg()
		.arg("build")
		.arg(format!("{}#foo", temp.path().display()))
		.spawn()
		.unwrap();
	let bar = server
		.tg()
		.arg("build")
		.arg(format!("{}#bar", temp.path().display()))
		.spawn()
		.unwrap();

	// send ctrl+C to foo.
	unsafe {
		libc::kill(foo.id().unwrap().to_i32().unwrap(), libc::SIGINT);
	}

	let foo_output = foo.wait_with_output().await.unwrap();
	let bar_output = bar.wait_with_output().await.unwrap();
	assert_failure!(foo_output);
	assert_success!(bar_output);
	let output = String::from_utf8(bar_output.stdout).unwrap();
	assert_snapshot!(output, @"42");
}

#[tokio::test]
async fn multiple_cancellation() {
	let server = Server::new(TG).await.unwrap();

	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r"
			export let foo = async () => {
				return await tg.build(baz);
			};

			export let bar = async () => {
				return await tg.build(baz);
			};

			export let baz = async () => {
				await tg.sleep(2);
				return 42;
			};
		")
	};
	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();

	let foo = server
		.tg()
		.arg("build")
		.arg(format!("{}#foo", temp.path().display()))
		.spawn()
		.unwrap();
	let bar = server
		.tg()
		.arg("build")
		.arg(format!("{}#bar", temp.path().display()))
		.spawn()
		.unwrap();

	unsafe {
		libc::kill(foo.id().unwrap().to_i32().unwrap(), libc::SIGINT);
		libc::kill(bar.id().unwrap().to_i32().unwrap(), libc::SIGINT);
	}

	let foo_output = foo.wait_with_output().await.unwrap();
	let bar_output = bar.wait_with_output().await.unwrap();
	assert_failure!(foo_output);
	assert_failure!(bar_output);

	let output = String::from_utf8(bar_output.stderr).unwrap();
	assert_snapshot!(output, @"");
}

async fn test(
	artifact: temp::Artifact,
	reference: &str,
	args: Vec<String>,
) -> std::process::Output {
	let server = Server::new(TG).await.unwrap();
	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();
	let mut command = server.tg();
	command.current_dir(temp.path()).arg("build").arg(reference);
	for arg in args {
		command.arg("-a");
		command.arg(arg);
	}
	command.output().await.unwrap()
}

async fn test_remote(
	artifact: temp::Artifact,
	reference: &str,
) -> (std::process::Output, std::process::Output) {
	let temp = Temp::new();
	artifact.to_path(temp.as_ref()).await.unwrap();

	// Create a server.
	let server = Server::new(TG).await.unwrap();

	// Create a remote server.
	let remote_server = Server::new(TG).await.unwrap();

	// Set the remote.
	let output = server
		.tg()
		.current_dir(temp.path())
		.arg("remote")
		.arg("put")
		.arg("default")
		.arg(remote_server.url().to_string())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Build on the remote.
	let output = server
		.tg()
		.current_dir(temp.path())
		.arg("build")
		.arg("--detach")
		.arg("--remote=default")
		.arg(reference)
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let process_id = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Get the output.
	let output = server
		.tg()
		.current_dir(temp.path())
		.arg("process")
		.arg("output")
		.arg(process_id.clone())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Get the output from the remote.
	let remote_output = remote_server
		.tg()
		.current_dir(temp.path())
		.arg("process")
		.arg("output")
		.arg(process_id)
		.output()
		.await
		.unwrap();
	assert_success!(remote_output);

	(output, remote_output)
}
