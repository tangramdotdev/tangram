use indoc::{formatdoc, indoc};
use insta::assert_snapshot;
use serde_json::json;
use std::collections::BTreeMap;
use tangram_temp::{self as temp, Temp};

mod common;
use common::test;

#[tokio::test]
async fn hello_world() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => r#"export default tg.target(() => "Hello, world!")"#,
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn no_return_value() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => r"export default tg.target(() => {})",
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @r###"
   [38;5;9m[1merror[0m: No overload matches this call.
     Overload 1 of 2, '(function_: () => Unresolved<Value>): Target<[], Value>', gave the following error.
       Argument of type '() => void' is not assignable to parameter of type '() => Unresolved<Value>'.
         Type 'void' is not assignable to type 'Unresolved<Value>'.
     Overload 2 of 2, '(...args: Args<Arg>): Promise<Target<Value[], Value>>', gave the following error.
       Argument of type '() => void' is not assignable to parameter of type 'Unresolved<MaybeNestedArray<string | Template | Artifact | Target<Value[], Value> | MaybeMutationMap<ArgObject> | undefined>>'.
         Type '() => void' is missing the following properties from type 'Target<Value[], Value>': id, args, checksum, env, and 3 more.
      [38;5;11mdir_01hw68xvst4xee12q5hcjm1v1n2payedrka0ehvhy1zc6msmga5pv0:tangram.ts:1:26[39m
   [38;5;9m[1merror[0m failed to run the command
   [38;5;9m->[39m type checking failed
   "###);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn nonexistent_function() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => r"export default tg.target(() => greet())",
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @r###"
   [38;5;9m[1merror[0m: Cannot find name 'greet'.
      [38;5;11mdir_01st8hbk745rhdjpnzn3qxghazfnpn297v4y3k438vn0x8zpmegg0g:tangram.ts:1:32[39m
   [38;5;9m[1merror[0m failed to run the command
   [38;5;9m->[39m type checking failed
   "###);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn call_target() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default tg.target(async () => {
				return await buildTarget();
			});
			export const createTarget = tg.target(() => {
				return tg.target("echo 'hi' > $OUTPUT");
			});
			export const buildTarget = async () => {
				const t = await createTarget();
				const output = await t.output();
				return output;
			};
		"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn rename_default() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import bar from "./bar.tg.ts";
			export default tg.target(() => bar());
		"#),
		"bar.tg.ts" => indoc!(r"
				export const build = tg.target(() => tg.directory());
				export default build;
			"),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn deps_issue() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import other from "./sdk/other_thing/other.tg.ts";
			export default tg.target(() => other());
		"#),
		"sdk" => temp::directory! {
			"other_thing" => temp::directory! {
				"other.tg.ts" => indoc!(r#"
						import deps from "../deps.tg.ts";
						export default tg.target(() => deps());
					"#),
			},
			"deps.tg.ts" => indoc!(r#"
					import zstd from "./deps/zstd.tg.ts";
					export default tg.target(() => zstd());
				"#),
			"deps" => temp::directory! {
				"zstd.tg.ts" => indoc!(r"
						export const build = tg.target(() => tg.directory());
						export default build;
					"),
			},
		}
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn default_by_name() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as baz from "./baz.tg.ts";
			export default tg.target(() => baz.build());
		"#),
		"bar.tg.ts" => indoc!(r"
				export const build = tg.target(() => tg.directory());
				export default build;
			"),
		"baz.tg.ts" => indoc!(r#"
				import * as bar from "./bar.tg.ts";
				export const build = tg.target(() => bar.build());
				export default build;
			"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn nested_export_default() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import * as bar from "./bar.tg.ts";
			export default tg.target(() => bar.build());
		"#),
		"bar.tg.ts" => indoc!(r"
				export const build = tg.target(() => tg.directory());
				export default build;
			"),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn default_by_name_many() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => {
			let mut contents = String::new();
			for i in 'a'..='z' {
				contents.push_str(&formatdoc!(r#"
					import * as {i} from "./modules/{i}.tg.ts";
					export let build{i} = tg.target(() => {i}.build());
				"#));
			}
			contents.push_str("export default tg.target(() => 5);");
			contents
		},
		"modules" => {
			let mut entries = BTreeMap::new();

			for i in 'a'..='z' {
				let filename = format!("{i}.tg.ts");
				let contents = indoc!(r"
						export const build = tg.target(() => tg.directory());
						export default build;
					");
				entries.insert(filename.into(), contents.into());
			}

			temp::Artifact::Directory { entries }
		}
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;
	dbg!(artifact_temp.path());

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn circular_imports() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import { getValue } from "./a.tg.ts";
			export default tg.target(() => getValue());
		"#),
		"a.tg.ts" => indoc!(r#"
			import { increment } from "./b.tg.ts";
			export const getValue = () => increment(5);
		"#),
		"b.tg.ts" => indoc!(r#"
			import { getValue } from "./a.tg.ts";
			export const increment = (n: number) => getValue() + 1;
		"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, _, stderr| async move {
			assert_snapshot!(stderr, @r###"
   [38;5;9m[1merror[0m: 'getValue' implicitly has return type 'any' because it does not have a return type annotation and is referenced directly or indirectly in one of its return expressions.
      [38;5;11mdir_0110fqn7yza66ybmpymxk9gf2y8z5m9hxmv4qf5yx5q0e6wcxsr800:./a.tg.ts:2:14[39m
   [38;5;9m[1merror[0m: 'increment' implicitly has return type 'any' because it does not have a return type annotation and is referenced directly or indirectly in one of its return expressions.
      [38;5;11mdir_0110fqn7yza66ybmpymxk9gf2y8z5m9hxmv4qf5yx5q0e6wcxsr800:./b.tg.ts:2:14[39m
   [38;5;9m[1merror[0m failed to run the command
   [38;5;9m->[39m type checking failed
   "###);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn re_export_default() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import myUtil from "./intermediate.tg.ts";
			export default tg.target(() => myUtil());
		"#),
		"intermediate.tg.ts" => indoc!(r#"
			export { default } from "./util.tg.ts";
		"#),
		"util.tg.ts" => indoc!(r#"
			export default tg.target(() => "re-exported!");
		"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn mixed_import_styles() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import defaultExport from "./a.tg.ts";
			import { namedExport } from "./b.tg.ts";
			import * as namespace from "./c.tg.ts";
			
			export default tg.target(() => {
				const v1 = defaultExport();
				const v2 = namedExport();
				const v3 = namespace.util();
				return `${v1}-${v2}-${v3}`;
			});
		"#),
		"a.tg.ts" => indoc!(r#"
			export default tg.target(() => "default");
		"#),
		"b.tg.ts" => indoc!(r#"
			export const namedExport = tg.target(() => "named");
		"#),
		"c.tg.ts" => indoc!(r#"
			export const util = tg.target(() => "namespace");
		"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn nested_directory_imports() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import { deep } from "./nested/very/deep/module.tg.ts";
			export default tg.target(() => deep());
		"#),
		"nested" => temp::directory! {
			"very" => temp::directory! {
				"deep" => temp::directory! {
					"module.tg.ts" => indoc!(r#"
						export const deep = tg.target(() => "found me!");
					"#),
				},
			},
		},
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, stdout, stderr| async move {
			assert_snapshot!(stdout, @"");
			assert_snapshot!(stderr, @"");
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn import_non_existent() -> std::io::Result<()> {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import { missing } from "./nonexistent.tg.ts";
			export default tg.target(() => missing());
		"#),
	};
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await?;

	test(
		json!({
			"tracing": {
				 "filter": "tangram_server=error"
			}
		}),
		&["check", artifact_temp.path().to_str().unwrap()],
		|_, _, stderr| async move {
			assert!(stderr.contains("No such file"));
			Ok(())
		},
	)
	.await
}
