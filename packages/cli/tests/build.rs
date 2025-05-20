use indoc::indoc;
use insta::assert_snapshot;
use tangram_cli::{assert_failure, assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let directory = temp::directory! {
		"tangram.ts" => r#"export default () => "Hello, World!""#,
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!""#);
	};
	let args = vec![];
	let path = "";
	let command = "default";
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn hello_world_remote() {
	let directory = temp::directory! {
		"tangram.ts" => r#"export default () => "Hello, World!""#,
	};
	let assertions = |local: std::process::Output, remote: std::process::Output| async move {
		assert_success!(local);
		assert_success!(remote);
		let local = std::str::from_utf8(&local.stdout).unwrap();
		let remote = std::str::from_utf8(&remote.stdout).unwrap();
		assert_snapshot!(local, @r#""Hello, World!""#);
		assert_snapshot!(remote, @r#""Hello, World!""#);
	};
	let args = vec![];
	let path = "";
	let command = "default";
	test_build_remote(directory, path, command, args, assertions).await;
}

/// Test building a module without a package.
#[tokio::test]
async fn module_without_package() {
	let directory = temp::directory! {
		"foo.tg.ts" => indoc!(r#"
			export default () => "Hello, World!";
		"#),
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!""#);
	};
	let path = "foo.tg.ts";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn no_return_value() {
	let directory = temp::directory! {
		"tangram.ts" => r"export default () => {};",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"");
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn args() {
	let directory = temp::directory! {
		"tangram.ts" => r"export default (name: string) => `Hello, ${name}!`;",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, Tangram!""#);
	};
	let path = "";
	let command = "default";
	let args = vec![r#""Tangram""#.into()];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn host_command_hello_world() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT");
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"fil_01d399v34jw3wztpzsnqycbk64mc06tgg6jfeaeh57cgcrcg0swvag");
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn host_command_hello_world_remote() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT");
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |local: std::process::Output, remote: std::process::Output| async move {
		assert_success!(local);
		assert_success!(remote);
		let local = std::str::from_utf8(&local.stdout).unwrap();
		let remote = std::str::from_utf8(&remote.stdout).unwrap();
		assert_snapshot!(local, @"fil_01d399v34jw3wztpzsnqycbk64mc06tgg6jfeaeh57cgcrcg0swvag");
		assert_snapshot!(remote, @"fil_01d399v34jw3wztpzsnqycbk64mc06tgg6jfeaeh57cgcrcg0swvag");
	};
	test_build_remote(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn two_modules() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import bar from "./bar.tg.ts";
			export default () => tg.run(bar);
		"#),
		"bar.tg.ts" => r#"export default () => "Hello from bar""#,
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello from bar""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn path_dependency() {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => tg.run(bar);
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => r#"export default () => "Hello from bar";"#
		}
	};
	let path = "foo";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello from bar""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn path_dependency_import_attribute() {
	let directory = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "bar" with { path: "../bar" };
				export default () => tg.run(bar);
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => r#"export default () => "Hello from bar";"#
		}
	};
	let path = "foo";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello from bar""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn named_command() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r"
			export let five = () => 5;
			export let six = () => 6;
		"),
	};
	let path = "";
	let command = "five";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"5");
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn concurrent_commands() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r"
			export default async () => {
				let results = await Promise.all(Array.from(Array(100).keys()).map((i) => tg.run(double, i)));
				return results.reduce((acc, el) => acc + el, 0);
			};
			export let double = (i: number) => i * 2;
		"),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"9900");
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn capture_error() {
	let directory = temp::directory! {
		"tangram.ts" => r#"export default () => { throw new error("not so fast!"); };"#,
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn import_file() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import file from "./hello.txt";
			export default () => file.text();
		"#),
		"hello.txt" => "Hello, World!",
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn import_directory() {
	let directory = temp::directory! {
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
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_raw() {
	let directory = temp::directory! {
		"tangram.ts" => r"
			export default () => tg.Template.raw`\n\tHello, World!\n`;
		",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template(["\n\tHello, World!\n"])"#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_single_line() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file from "./hello.txt";
			export default () => tg`cat ${file}`;
		"#,
		"hello.txt" => "Hello, World!",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template(["cat ",fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60])"#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_with_quote() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file from "./hello.txt";
			export default () => tg`
				other_command

				other_command

				other_command

				echo 'exec ${file} "$@"' >> script.sh
			`;
		"#,
		"hello.txt" => "Hello, World!",
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template(["other_command\n\nother_command\n\nother_command\n\necho 'exec ",fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60," \"$@\"' >> script.sh\n"])"#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_single_line_two_artifacts() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import foo from "./foo.txt";
			import bar from "./bar.txt";
			export default () => tg`${foo} ${bar}`;
		"#,
		"foo.txt" => "foo",
		"bar.txt" => "bar",
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template([fil_01rjnfkrgx5j33g04j2htzk0yrtc3bk9k5259msv5b47w909cwp8y0," ",fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg])"#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_empty_lines() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			export default () => tg`
				function foo() {
					echo "Hello, World!"

				}
			`;
		"#,
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template(["function foo() {\n\techo \"Hello, World!\"\n\n}\n"])"#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_only_placeholders_on_a_line() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file from "./hello.txt";
			export default () => tg`
				${file}${file}
			`;
		"#,
		"hello.txt" => "Hello, World!",
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template([fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60,fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60,"\n"])"#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_single_line_explicit_newline() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import foo from "./foo.txt";
			import bar from "./bar.txt";
			export default () => tg`${foo}\n${bar}`;
		"#,
		"foo.txt" => "foo",
		"bar.txt" => "bar",
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template([fil_01rjnfkrgx5j33g04j2htzk0yrtc3bk9k5259msv5b47w909cwp8y0,"\n",fil_019xazfm02zwbr13avkcdhmdqkvrb770e6m97r7681jp9a3c57agyg])"#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn template_multiple_placeholders() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			import file1 from "./hello.txt";
			import file2 from "./hello.txt";
			import file3 from "./hello.txt";
			export default () => tg`
				cat\t${file1}\t${file1}
			`;
		"#,
		"hello.txt" => "Hello, World!",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#"tg.template(["cat\t",fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60,"\t",fil_012aeh2qchn5np70n340y7fn1jecczp8f8bff7jneb8ecbvyyrrq60,"\n"])"#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn blob_template() {
	let directory = temp::directory! {
		"tangram.ts" => r"
			export default () => tg.blob`\n\tHello, World!\n`.then((b) => b.text());
		",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn blob_template_two_placeholders() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			export default () => {
				let a = "string!";
				return tg.blob`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
			}
		"#,
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!\nstring!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn blob_template_raw() {
	let directory = temp::directory! {
		"tangram.ts" => r"
			export default () => tg.Blob.raw`\n\tHello, World!\n`.then((b) => b.text());
		",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""\n\tHello, World!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn file_template() {
	let directory = temp::directory! {
		"tangram.ts" => r"
			export default () => tg.file`\n\tHello, World!\n`.then((f) => f.text());
		",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn file_template_two_placeholders() {
	let directory = temp::directory! {
		"tangram.ts" => r#"
			export default () => {
				let a = "string!";
				return tg.file`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
			}
		"#,
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""Hello, World!\nstring!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn file_template_raw() {
	let directory = temp::directory! {
		"tangram.ts" => r"
			export default () => tg.File.raw`\n\tHello, World!\n`.then((f) => f.text());
		",
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""\n\tHello, World!\n""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn directory_get_follows_intermediate_component_symlinks() {
	let directory = temp::directory! {
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
	};
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""foo""#);
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn directory_get_follows_final_component_symlinks() {
	let directory = temp::directory! {
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
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""foo""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[ignore]
#[tokio::test]
async fn command_cycle_detection() {
	let directory = temp::directory! {
		"tangram.ts" => "export let x = () => tg.build(x);"
	};
	let path = "";
	let command = "x";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[ignore]
#[tokio::test]
async fn command_cycle_detection_between_packages() {
	let directory = temp::directory! {
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
	};
	let path = "foo";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[ignore]
#[tokio::test]
async fn package_cycle_without_command_cycle() {
	let directory = temp::directory! {
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
	};
	let path = "foo";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r###""foo""###);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn value_cycle_detection_object() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!("
			export default () => {
				let x = {};
				x.a = x;
				return x;
			};
		"),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn value_cycle_detection_array() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!("
			export default () => {
				let x = [];
				x[0] = x;
				return x;
			};
		")
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn builtin_download_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://example.com", "sha256:any");
				return tg.file(blob);
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"fil_01cvc6nxd5cmqrp4v6jq08735m6r4e08kk9wxz3fs17b5cvqs50n00");
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn builtin_download_exact_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://example.com", "sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9");
				return tg.file(blob);
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"fil_01cvc6nxd5cmqrp4v6jq08735m6r4e08kk9wxz3fs17b5cvqs50n00");
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn builtin_download_rejects_incorrect_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://example.com", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
				return tg.file(blob);
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn builtin_download_rejects_malformed_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://example.com", "nonsense");
				return tg.file(blob);
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn command_none_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT", { checksum: "none" });
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn command_set_checksum() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				return await tg.run("echo 'Hello, World!' > $OUTPUT", { checksum: "sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017" });
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_failure!(output);
	};
	test_build(directory, path, command, args, assertions).await;
}

#[ignore]
#[tokio::test]
async fn builtin_artifact_archive_extract_simple_dir_roundtrip() {
	let module = indoc!(
		r#"
			export default async () => {
				let artifact = await tg.directory({
					"hello.txt": "contents",
					"link": tg.symlink("./hello.txt"),
				});
				let archive = await tg.archive(artifact, "format");
				let extracted = await tg.extract(archive);
				tg.assert(await extracted.id() === await artifact.id());
			};
		"#
	);

	let format = "tar";
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		assert_success!(output);
	};
	test_archive(module, format, assertions).await;

	let format = "zip";
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		assert_success!(output);
	};
	test_archive(module, format, assertions).await;
}

async fn test_archive<F, Fut>(module: &str, format: &str, assertions: F)
where
	F: FnOnce(std::process::Output) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	let module = module.replace("format", format);
	let directory = temp::directory! {
		"tangram.ts" => module,
	};
	let path = "";
	let command = "default";
	let args = vec![];
	test_build(directory, path, command, args, assertions).await;
}

#[tokio::test]
async fn import_from_tag() {
	test(TG, async move |context| {
		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

		// Create a local server.
		let local_server = context.spawn_server().await.unwrap();
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

		// Create a a package that imports the other package.
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
	})
	.await;
}

#[tokio::test]
async fn builtin_blob_compress_decompress_gz_roundtrip() {
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.blob("contents");
				let compressed = await tg.compress(blob, "gz");
				let decompressed = await tg.decompress(compressed, "gz");
				return blob.text();
			};
		"#),
	};
	let path = "";
	let command = "default";
	let args = vec![];
	let assertions = |output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r#""contents""#);
	};
	test_build(directory, path, command, args, assertions).await;
}

async fn test_build<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	command: &str,
	args: Vec<String>,
	assertions: F,
) where
	F: FnOnce(std::process::Output) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let artifact: temp::Artifact = artifact.into();
		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.unwrap();

		let path = temp.path().join(path);
		let command_ = format!("{path}#{command}", path = path.display());

		// Build.
		let mut command = server.tg();
		command.arg("build").arg(command_);
		for arg in args {
			command.arg("--arg");
			command.arg(arg);
		}
		let output = command.output().await.unwrap();

		assertions(output).await;
	})
	.await;
}

async fn test_build_remote<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send + 'static,
	path: &str,
	command: &str,
	_args: Vec<String>,
	assertions: F,
) where
	F: FnOnce(std::process::Output, std::process::Output) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let artifact: temp::Artifact = artifact.into();
		// Create a directory with a module.
		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.unwrap();

		let path = temp.path().join(path);
		let command = format!("{path}#{command}", path = path.display());

		// Create a remote server.
		let remote_server = context.spawn_server().await.unwrap();

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

		// Build on the remote.
		let output = local_server1
			.tg()
			.arg("build")
			.arg("--remote")
			.arg("default")
			.arg("--detach")
			.arg(command)
			.output()
			.await
			.unwrap();
		assert_success!(output);
		let process_id = std::str::from_utf8(&output.stdout)
			.unwrap()
			.trim()
			.to_owned();

		// Build the module.

		// Get the output on the local server.
		let local_output = local_server1
			.tg()
			.arg("process")
			.arg("output")
			.arg(process_id.clone())
			.output()
			.await
			.unwrap();
		assert_success!(local_output);

		// Get the output on the remote server.
		let remote_output = remote_server
			.tg()
			.arg("process")
			.arg("output")
			.arg(process_id)
			.output()
			.await
			.unwrap();
		assert_success!(remote_output);

		assertions(local_output, remote_output).await;
	})
	.await;
}
