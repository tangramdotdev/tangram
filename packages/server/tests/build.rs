use futures::{Future, FutureExt as _};
use indoc::indoc;
use insta::assert_snapshot;
use std::{panic::AssertUnwindSafe, pin::pin, str::FromStr};
use tangram_client::{self as tg, handle::Ext};
use tangram_futures::stream::TryStreamExt as _;
use tangram_server::{Config, Server};
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn hello_world() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello, World!")"#,
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, World!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn hello_world_remote() -> tg::Result<()> {
	test_remote(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello, World!")"#,
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, World!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn accepts_target_with_no_return_value() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r"export default tg.target(() => {})",
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"null");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn accepts_arg() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r"export default tg.target((name: string) => `Hello, ${name}!`)",
			}
		},
		"foo",
		"default",
		vec!["Tangram".into()],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, Tangram!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn host_target_hello_world() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let target = await tg.target("echo 'Hello, World!' > $OUTPUT");
						let output = await target.output();
						return output;
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"fil_01r4jx5ae6bkr2q5gbhewjrdzfban0kx9pmqmvh2prhkxwxj45mg6g");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn host_target_hello_world_remote() -> tg::Result<()> {
	test_remote(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let target = await tg.target("echo 'Hello, World!' > $OUTPUT");
						let output = await target.output();
						return output;
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"fil_01r4jx5ae6bkr2q5gbhewjrdzfban0kx9pmqmvh2prhkxwxj45mg6g");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn two_modules() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import bar from "./bar.tg.ts";
					export default tg.target(() => bar());
				"#),
				"bar.tg.ts" => r#"export default tg.target(() => "Hello from bar")"#,
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello from bar""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import bar from "../bar";
					export default tg.target(() => bar());
				"#),
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello from bar");"#
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello from bar""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency_import_attribute() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import bar from "bar" with { path: "../bar" };
					export default tg.target(() => bar());
				"#),
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello from bar");"#
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello from bar""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn named_target() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r"
					export let five = tg.target(() => 5);
					export let six = tg.target(() => 6);
				"),
			},
		},
		"foo",
		"five",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"5");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn concurrent_targets() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r"
					export default tg.target(async () => {
						let results = await Promise.all(Array.from(Array(100).keys()).map((i) => double(i)));
						return results.reduce((acc, el) => acc + el, 0);
					});
					export let double = tg.target((i: number) => i * 2);
				"),
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"9900");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn capture_error() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => { throw new Error("not so fast!"); });"#,
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"Uncaught Error: not so fast!");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn import_file() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import file from "./hello.txt";
					export default tg.target(() => file.text());
				"#),
				"hello.txt" => "Hello, World!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, World!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn import_directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import directory from "./directory" with { type: "directory" };
					export default tg.target(async () =>
						directory.get("hello.txt")
							.then(tg.File.expect)
							.then((f) => f.text())
					);
				"#),
				"directory" => temp::directory! {
					"hello.txt" => "Hello, World!",
				}
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, World!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_raw() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r"
					export default tg.target(() => tg.Template.raw`\n\tHello, World!\n`);
				",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template(["\n\tHello, World!\n"])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_single_line() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import file from "./hello.txt";
					export default tg.target(() => tg`cat ${file}`);
				"#,
				"hello.txt" => "Hello, World!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template(["cat ",fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_with_quote() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import file from "./hello.txt";
					export default tg.target(() => tg`
						other_command

						other_command
					
						other_command
					
						echo 'exec ${file} "$@"' >> script.sh
					`);
				"#,
				"hello.txt" => "Hello, World!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template(["other_command\n\nother_command\n\nother_command\n\necho 'exec ",fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g," \"$@\"' >> script.sh\n"])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_single_line_two_artifacts() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import foo from "./foo.txt";
					import bar from "./bar.txt";
					export default tg.target(() => tg`${foo} ${bar}`);
				"#,
				"foo.txt" => "foo",
				"bar.txt" => "bar",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template([fil_01mav0wfrn654f51gn5dbk8t8akh830xd1a97yjd1j85w5y8evmc1g," ",fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_empty_lines() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					export default tg.target(() => tg`
						function foo() {
							echo "Hello, World!"

						}


					`);
				"#,
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template(["function foo() {\n\techo \"Hello, World!\"\n\n}\n\n\n"])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_only_placeholders_on_a_line() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import file from "./hello.txt";
					export default tg.target(() => tg`
						${file}${file}
					`);
				"#,
				"hello.txt" => "Hello, World!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template([fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g,fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g,"\n"])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_single_line_explicit_newline() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import foo from "./foo.txt";
					import bar from "./bar.txt";
					export default tg.target(() => tg`${foo}\n${bar}`);
				"#,
				"foo.txt" => "foo",
				"bar.txt" => "bar",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template([fil_01mav0wfrn654f51gn5dbk8t8akh830xd1a97yjd1j85w5y8evmc1g,"\n",fil_01kj2srg33pbcnc7hwbg11xs6z8mdkd9bck9e1nrte4py3qjh5wb80])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn template_multiple_placeholders() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"
					import file1 from "./hello.txt";
					import file2 from "./hello.txt";
					import file3 from "./hello.txt";
					export default tg.target(() => tg`
						cat\t${file1}\t${file1}
					`);
				"#,
				"hello.txt" => "Hello, World!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#"tg.template(["cat\t",fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g,"\t",fil_01tvcqmbbf8dkkejz6y69ywvgfsh9gyn1xjweyb9zgv0sf4752446g,"\n"])"#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory_get_follows_intermediate_component_symlinks() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import directory from "./directory" with { type: "directory" };
					export default tg.target(async () => {
						let file = await directory.get("link/hello.txt");
						tg.File.assert(file);
						return file.text();
					});
				"#),
				"directory" => temp::directory! {
					"hello.txt" => "foo",
					"link" => temp::symlink!(".")
				}
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""foo""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory_get_follows_final_component_symlinks() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import directory from "./directory" with { type: "directory" };
					export default tg.target(async () => {
						let file = await directory.get("link");
						tg.File.assert(file);
						return file.text();
					});
				"#),
				"directory" => temp::directory! {
					"hello.txt" => "foo",
					"link" => temp::symlink!("hello.txt")
				}
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""foo""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn target_cycle_detection() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => "export let x = tg.target(() => x());"
			},
		},
		"foo",
		"x",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"failed to build the target");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn target_cycle_detection_between_packages() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import bar from "../bar";
					export default tg.target(() => bar());
				"#)
			},
			"bar" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import foo from "../foo";
					export default tg.target(() => foo());
				"#)
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"failed to build the target");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_cycle_without_target_cycle() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import bar from "../bar";
					export default tg.target(() => bar());
					export let greeting = tg.target(() => "foo");
				"#)
			},
			"bar" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import * as foo from "../foo";
					export default tg.target(() => foo.greeting());
				"#)
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#""foo""#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn value_cycle_detection_object() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!("
					export default tg.target(() => {
						let x = {};
						x.a = x;
						return x;
					});
				"),
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"Uncaught Error: cycle detected");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn value_cycle_detection_array() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!("
					export default tg.target(() => {
						let x = [];
						x[0] = x;
						return x;
					});
				")
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"Uncaught Error: cycle detected");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_download_unsafe_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.download("https://example.com", "unsafe");
						return tg.file(blob);
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"fil_015s0zvjgtbm0j9jd8pn46e275v9sd13174p3w4twdw17826zb08c0");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_download_exact_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.download("https://example.com", "sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9");
						return tg.file(blob);
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"fil_015s0zvjgtbm0j9jd8pn46e275v9sd13174p3w4twdw17826zb08c0");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_download_rejects_incorrect_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.download("https://example.com", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
						return tg.file(blob);
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"failed to build the target");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_download_rejects_malformed_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.download("https://example.com", "nonsense");
						return tg.file(blob);
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"the syscall failed");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn target_none_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let target = await tg.target("echo 'Hello, World!' > $OUTPUT", { checksum: "none" });
						let output = await target.output();
						return output;
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"failed to build the target");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn target_set_checksum() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let target = await tg.target("echo 'Hello, World!' > $OUTPUT", { checksum: "sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017" });
						let output = await target.output();
						return output;
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let error = outcome.into_result().unwrap();
			assert_snapshot!(error, @r"fil_01r4jx5ae6bkr2q5gbhewjrdzfban0kx9pmqmvh2prhkxwxj45mg6g");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_artifact_archive_extract_simple_dir_roundtrip() -> tg::Result<()> {
	let module = indoc!(
		r#"
			export default tg.target(async () => {
				let artifact = await tg.directory({
					"hello.txt": "contents",
					"link": tg.symlink("./hello.txt"),
				});
				let archived = await tg.archive(artifact, "format");
				let extracted = await tg.extract(archived, "format");
				tg.assert(await extracted.id() === await artifact.id());
			});
		"#
	);
	test_archive(module, |_, outcome| async move {
		outcome.into_result().unwrap();
		Ok::<_, tg::Error>(())
	})
	.await
}

async fn test_archive<F, Fut>(module: &str, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, tg::build::Outcome) -> Fut + Clone,
	Fut: Future<Output = tg::Result<()>>,
{
	for format in &["tar", "tgar", "zip"] {
		let module = module.replace("format", format);
		let directory = temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => module,
			}
		};
		test(directory, "foo", "default", vec![], assertions.clone()).await?;
	}
	Ok(())
}

#[tokio::test]
async fn import_from_tag() -> tg::Result<()> {
	let remote_temp = Temp::new();
	let remote_config = Config::with_path(remote_temp.path().to_owned());
	let remote = Server::start(remote_config).await?;

	let server_temp = Temp::new();
	let mut server_config = Config::with_path(server_temp.path().to_owned());
	server_config.build = Some(tangram_server::config::Build::default());
	server_config.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());
	server_config.remotes = [(
		"default".to_owned(),
		tangram_server::config::Remote {
			url: remote.url().clone(),
		},
	)]
	.into();
	let server = Server::start(server_config).await?;

	let result = AssertUnwindSafe(async {
		// create package
		let foo = tg::directory! {
			"tangram.ts" => tg::file!(r#"export default tg.target(() => "foo");"#)
		};
		let foo_id = foo.id(&server).await?;

		// create tag.
		let foo_tag = tg::Tag::from_str("foo").unwrap();
		let put_foo_arg = tg::tag::put::Arg {
			force: false,
			item: tangram_either::Either::Right(foo_id.into()),
			remote: Some("default".into()),
		};
		server.put_tag(&foo_tag, put_foo_arg).await?;

		// create consumer temp
		let bar_temp = Temp::new();
		let bar = temp::directory! {
			"bar" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import foo from "foo";
					export default tg.target(() => foo());
			"#)
			}
		};
		let bar: temp::Artifact = bar.into();
		bar.to_path(bar_temp.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = bar_temp.path().display(), "failed to write the artifact"),
		)?;

		// check in
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: bar_temp.as_ref().join("bar"),
		};
		let bar_artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();

		let subpath = Some("tangram.ts".parse().unwrap());
		let env = [("TANGRAM_HOST".to_owned(), tg::host().into())].into();
		let args = vec!["default".into()];
		let executable = Some(tg::target::Executable::Module(tg::target::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: bar_artifact.into(),
				path: None,
				subpath,
				tag: None,
			},
		}));
		let host = "js";
		let target = tg::target::Builder::new(host)
			.args(args)
			.env(env)
			.executable(executable)
			.build();
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: None,
			retry: tg::build::Retry::Canceled,
		};
		let target = target.id(&server).await?;
		let output = server.build_target(&target, arg).await?;
		let build = tg::Build::with_id(output.build);
		let outcome = build.outcome(&server).await?;
		let outcome = outcome.into_result()?;
		assert_snapshot!(outcome, @r#""foo""#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(server_temp, server).await;
	cleanup(remote_temp, remote).await;
	result.unwrap()
}

#[tokio::test]
async fn builtin_blob_compress_decompress_gz_roundtrip() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.blob("contents");
						let compressed = await tg.compress(blob, "gz");
						let decompressed = await tg.decompress(compressed, "gz");
						return blob.text();
					});
				"#),
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r#""contents""#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn build_create_false() -> tg::Result<()> {
	let artifact: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => "hello, world!");
		"#),
	}
	.into();
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(tangram_server::config::Build::default());
	options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());

	// Start the server.
	let server = Server::start(options.clone()).await?;
	let result = AssertUnwindSafe(async {
		// Create the temp.
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.path()).await.unwrap();

		// Checkin the artifact.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: artifact_temp.path().to_owned(),
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();
		let artifact = artifact.clone().into();
		let subpath = Some("tangram.ts".parse().unwrap());
		let env = [("TANGRAM_HOST".to_owned(), tg::host().into())].into();

		let args = vec![tg::Value::String("default".into())];
		let executable = Some(tg::target::Executable::Module(tg::target::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: artifact,
				path: Some(".".into()),
				subpath,
				tag: None,
			},
		}));
		let host = "js";
		let target = tg::target::Builder::new(host)
			.args(args)
			.env(env)
			.executable(executable)
			.build();

		// Build the target.
		let arg = tg::target::build::Arg {
			create: false,
			parent: None,
			remote: None,
			retry: tg::build::Retry::Canceled,
		};
		let target = target.id(&server).await?;
		let error = server.build_target(&target, arg.clone()).await.unwrap_err();
		assert_snapshot!(error, @"expected a build");
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn build_cache_hit() -> tg::Result<()> {
	let artifact: temp::Artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(() => "hello, world!");
		"#),
	}
	.into();
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.build = Some(tangram_server::config::Build::default());
	options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());

	// Start the server.
	let server = Server::start(options.clone()).await?;
	let result = AssertUnwindSafe(async {
		// Create the temp.
		let artifact_temp = Temp::new();
		artifact.to_path(artifact_temp.path()).await.unwrap();

		// Checkin the artifact.
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: artifact_temp.path().to_owned(),
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();
		let artifact = artifact.clone().into();
		let subpath = Some("tangram.ts".parse().unwrap());
		let env = [("TANGRAM_HOST".to_owned(), tg::host().into())].into();

		let args = vec![tg::Value::String("default".into())];
		let executable = Some(tg::target::Executable::Module(tg::target::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: artifact,
				path: Some(".".into()),
				subpath,
				tag: None,
			},
		}));
		let host = "js";
		let target = tg::target::Builder::new(host)
			.args(args)
			.env(env)
			.executable(executable)
			.build();

		// Build the target.
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: None,
			retry: tg::build::Retry::Canceled,
		};
		let target = target.id(&server).await?;
		let build1 = server.build_target(&target, arg.clone()).await?.build;
		let _outcome1 = tg::Build::with_id(build1.clone()).outcome(&server).await?;

		// Stop the server.
		server.stop();
		server.wait().await;

		// Restart it with no build configuration.
		options.build.take();
		options.build_heartbeat_monitor.take();
		options.build_indexer.take();
		let server = Server::start(options).await?;

		// Get a build.
		let build2 = server.build_target(&target, arg.clone()).await?.build;
		let _outcome2 = tg::Build::with_id(build2.clone()).outcome(&server).await?;

		// Stop the server.
		server.stop();
		server.wait().await;

		assert_eq!(build1, build2);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap()
}

async fn test<F, Fut>(
	artifact: impl Into<temp::Artifact>,
	path: &str,
	target: &str,
	args: Vec<tg::Value>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::build::Outcome) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact = artifact.into();
	let artifact_temp = Temp::new();
	artifact.to_path(artifact_temp.as_ref()).await.map_err(
		|source| tg::error!(!source, %path = artifact_temp.path().display(), "failed to write the artifact"),
	)?;
	let server_temp = Temp::new();
	let mut options = Config::with_path(server_temp.path().to_owned());
	options.build = Some(tangram_server::config::Build::default());
	options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: artifact_temp.as_ref().join(path),
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();
		let artifact = artifact.clone().into();
		let subpath = Some("tangram.ts".parse().unwrap());
		let env = [("TANGRAM_HOST".to_owned(), tg::host().into())].into();
		let args = std::iter::once(target.into())
			.chain(args.into_iter())
			.collect();
		let executable = Some(tg::target::Executable::Module(tg::target::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: artifact,
				path: Some(path.into()),
				subpath,
				tag: None,
			},
		}));
		let host = "js";
		let target = tg::target::Builder::new(host)
			.args(args)
			.env(env)
			.executable(executable)
			.build();
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: None,
			retry: tg::build::Retry::Canceled,
		};
		let target = target.id(&server).await?;
		let output = server.build_target(&target, arg).await?;
		let build = tg::Build::with_id(output.build);
		let outcome = build.outcome(&server).await?;
		(assertions)(server.clone(), outcome).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	if result.as_ref().is_ok_and(Result::is_ok) {
		server_temp.remove().await.ok();
	}
	artifact_temp.remove().await.ok();
	result.unwrap()
}

async fn test_remote<F, Fut>(
	artifact: impl Into<temp::Artifact>,
	path: &str,
	target: &str,
	args: Vec<tg::Value>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::build::Outcome) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact = artifact.into();
	let artifact_temp = Temp::new();
	artifact.to_path(artifact_temp.as_ref()).await.map_err(
		|source| tg::error!(!source, %path = artifact_temp.path().display(), "failed to write the artifact"),
	)?;
	let remote_temp = Temp::new();
	let mut remote_options = Config::with_path(remote_temp.path().to_owned());
	remote_options.build = Some(tangram_server::config::Build::default());
	remote_options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());
	let remote = Server::start(remote_options).await?;
	let local_temp = Temp::new();
	let mut local_options = Config::with_path(local_temp.path().to_owned());
	local_options.remotes = [(
		"default".to_owned(),
		tangram_server::config::Remote {
			url: remote.url().clone(),
		},
	)]
	.into();
	let local = Server::start(local_options).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::artifact::checkin::Arg {
			cache: false,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: artifact_temp.as_ref().join(path),
		};
		let artifact = tg::Artifact::check_in(&local, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();
		let artifact = artifact.clone().into();
		let subpath = Some("tangram.ts".parse().unwrap());
		let env = [("TANGRAM_HOST".to_owned(), tg::host().into())].into();
		let args = std::iter::once(target.into())
			.chain(args.into_iter())
			.collect();
		let executable = Some(tg::target::Executable::Module(tg::target::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: artifact,
				path: Some(path.into()),
				subpath,
				tag: None,
			},
		}));
		let host = "js";
		let target = tg::target::Builder::new(host)
			.args(args)
			.env(env)
			.executable(executable)
			.build();
		let arg = tg::target::build::Arg {
			create: true,
			parent: None,
			remote: Some("default".to_string()),
			retry: tg::build::Retry::Canceled,
		};
		let target = target.id(&local).await?;
		let push_stream = local
			.push_object(
				&target.clone().into(),
				tg::object::push::Arg {
					remote: "default".to_string(),
				},
			)
			.await?;
		pin!(push_stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let output = local.build_target(&target, arg).await?;
		let build = tg::Build::with_id(output.build);
		let outcome = build.outcome(&local).await?;
		(assertions)(local.clone(), outcome).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	local.stop();
	local.wait().await;
	if result.as_ref().is_ok_and(Result::is_ok) {
		local_temp.remove().await.ok();
	}
	remote.stop();
	remote.wait().await;
	if result.as_ref().is_ok_and(Result::is_ok) {
		remote_temp.remove().await.ok();
	}
	artifact_temp.remove().await.ok();
	result.unwrap()
}

#[ctor::ctor]
fn ctor() {
	// Set the file descriptor limit.
	let limit = 65536;
	let rlimit = libc::rlimit {
		rlim_cur: limit,
		rlim_max: limit,
	};
	let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlimit) };
	assert!(ret == 0, "failed to set the file descriptor limit");

	// Initialize v8.
	v8::icu::set_common_data_74(deno_core_icudata::ICU_DATA).unwrap();
	let platform = v8::new_default_platform(0, true);
	v8::V8::initialize_platform(platform.make_shared());
	v8::V8::initialize();
}

async fn cleanup(temp: tangram_temp::Temp, server: tangram_server::Server) {
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
}
