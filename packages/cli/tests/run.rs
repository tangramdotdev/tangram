use indoc::indoc;
use insta::assert_snapshot;
use std::path::PathBuf;
use tangram_cli::{assert_failure, assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => {
				console.log("Hello, World!");
			};
		"#),
	}
	.into();
	let tags = vec![];
	let reference = ".";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r"
		Hello, World!
		");
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

#[tokio::test]
async fn assertion_failure() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "./foo.tg.ts";
			export default () => foo();
		"#),
		"foo.tg.ts" => indoc!(r"
			export default () => tg.assert(false);
		"),
	}
	.into();
	let tags = vec![];
	let reference = "./tangram.ts";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r#"
		error the process failed
		-> Uncaught Error: failed assertion
		   ╭─[./tangram.ts:2:22]
		 1 │ import foo from "./foo.tg.ts";
		 2 │ export default () => foo();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: failed assertion
		   ╰────
		   ╭─[./foo.tg.ts:1:25]
		 1 │ export default () => tg.assert(false);
		   ·                         ▲
		   ·                         ╰── Uncaught Error: failed assertion
		   ╰────
		"#);
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

#[tokio::test]
async fn assertion_failure_out_of_tree() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => tg.run(bar);
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r"
				export default () => tg.assert(false);
			")
		},
	}
	.into();
	let tags = vec![];
	let reference = "./foo";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> the child process failed
		-> Uncaught Error: failed assertion
		   ╭─[./bar/tangram.ts:1:25]
		 1 │ export default () => tg.assert(false);
		   ·                         ▲
		   ·                         ╰── Uncaught Error: failed assertion
		   ╰────
		");
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

#[tokio::test]
async fn assertion_failure_in_path_dependency() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import foo from "../bar";
				export default () => foo();
			"#),
		},
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => tg.assert(false, "error")
			"#),
		}
	}
	.into();
	let tags = vec![];
	let reference = "./foo/tangram.ts";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r#"
		error the process failed
		-> Uncaught Error: error
		   ╭─[./foo/tangram.ts:2:22]
		 1 │ import foo from "../bar";
		 2 │ export default () => foo();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: error
		   ╰────
		   ╭─[./bar/tangram.ts:1:25]
		 1 │ export default () => tg.assert(false, "error")
		   ·                         ▲
		   ·                         ╰── Uncaught Error: error
		   ╰────
		"#);
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

#[tokio::test]
async fn assertion_failure_in_tag_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "foo";
			export default () => foo();
		"#),
	}
	.into();
	let foo = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => tg.assert(false, "error in foo");
		"#)
	}
	.into();
	let tags = vec![("foo".into(), foo, None)];
	let reference = "./tangram.ts";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r#"
		error the process failed
		-> Uncaught Error: error in foo
		   ╭─[./tangram.ts:2:22]
		 1 │ import foo from "foo";
		 2 │ export default () => foo();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: error in foo
		   ╰────
		   ╭─[foo:./tangram.ts:1:25]
		 1 │ export default () => tg.assert(false, "error in foo");
		   ·                         ▲
		   ·                         ╰── Uncaught Error: error in foo
		   ╰────
		"#);
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

#[tokio::test]
async fn assertion_failure_in_tagged_cyclic_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			import foo from "foo";
			export default () => foo();
		"#),
	}
	.into();
	let foo = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import bar from "../bar";
				export default () => bar();
				export const failure = () => tg.assert(false, "failure in foo");
			"#)
		},
		"bar" => temp::directory! {
			"tangram.ts" => indoc!(r#"
				import { failure } from "../foo";
				export default () => failure();
			"#)
		}
	}
	.into();
	let tags = vec![("foo".into(), foo, Some("foo".into()))];
	let reference = "./tangram.ts";
	let args = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r#"
		error the process failed
		-> Uncaught Error: failure in foo
		   ╭─[./tangram.ts:2:22]
		 1 │ import foo from "foo";
		 2 │ export default () => foo();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: failure in foo
		   ╰────
		   ╭─[foo:./tangram.ts:2:22]
		 1 │ import bar from "../bar";
		 2 │ export default () => bar();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: failure in foo
		 3 │ export const failure = () => tg.assert(false, "failure in foo");
		   ╰────
		   ╭─[foo:../bar/tangram.ts:2:22]
		 1 │ import { failure } from "../foo";
		 2 │ export default () => failure();
		   ·                      ▲
		   ·                      ╰── Uncaught Error: failure in foo
		   ╰────
		   ╭─[foo:../foo/tangram.ts:3:33]
		 2 │ export default () => bar();
		 3 │ export const failure = () => tg.assert(false, "failure in foo");
		   ·                                 ▲
		   ·                                 ╰── Uncaught Error: failure in foo
		   ╰────
		"#);
	};
	test_run(artifact, tags, reference, args, assertions).await;
}

async fn test_run<F, Fut>(
	artifact: temp::Artifact,
	tags: Vec<(String, temp::Artifact, Option<PathBuf>)>,
	reference: &str,
	args: Vec<String>,
	assertions: F,
) where
	F: FnOnce(PathBuf, std::process::Output) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send,
{
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		for (tag, artifact, subpath) in tags {
			let temp = Temp::new();
			artifact.to_path(temp.as_ref()).await.unwrap();
			let path = if let Some(subpath) = subpath {
				temp.path().join(subpath)
			} else {
				temp.path().to_owned()
			};
			let output = server
				.tg()
				.arg("tag")
				.arg(tag)
				.arg(&path)
				.output()
				.await
				.unwrap();
			assert_success!(output);
		}

		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.unwrap();

		let mut command = server.tg();
		command.current_dir(temp.path()).arg("run").arg(reference);
		for arg in args {
			command.arg("--arg");
			command.arg(arg);
		}
		let output = command.output().await.unwrap();

		assertions(temp.path().to_owned(), output).await;
	})
	.await;
}
