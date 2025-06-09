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
	let path = "";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @r"
		Hello, World!
		");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}

#[tokio::test]
async fn basic_string_argument() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default (name: string) => {
				console.log(`Hello, ${name}!`);
			}
		"#),
	}.into();
	let tags = vec![];
	let path = "";
	let export = "default";
	let args = vec![r#""Alice""#.to_string()];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"Hello, Alice!");
	};
	test_run(
		artifact,
		tags,
		path,
		export,
		args,
		env,
		flags,
		assertions,
	)
	.await;
}

#[tokio::test]
async fn multiple_arguments() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default (x: number, y: number) => {
				console.log(`Sum: ${x + y}`);
			}
		"#),
	}.into();
	let tags = vec![];
	let path = "";
	let export = "default";
	let args = vec!["7".to_string(), "8".to_string()];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"Sum: 15");
	};
	test_run(
		artifact,
		tags,
		path,
		export,
		args,
		env,
		flags,
		assertions,
	)
	.await;
}

#[tokio::test]
async fn object_argument() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			interface Config {
				name: string;
				age: number;
			}
			export default (config: Config) => {
				console.log(`Name: ${config.name}, Age: ${config.age}`);
			}
		"#),
	}.into();
	let tags = vec![];
	let path = "";
	let export = "default";
	let args = vec![r#"'{"name":"Bob","age":25}'"#.to_string()];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"");
	};
	test_run(
		artifact,
		tags,
		path,
		export,
		args,
		env,
		flags,
		assertions,
	)
	.await;
}

#[tokio::test]
async fn read_env_var() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				const apiKey = (await tg.process.env()).API_KEY;
				if (!apiKey) {
					throw new Error("API_KEY is required");
				}
				console.log(`Using API key: ${apiKey}`);
			}
		"#),
	}.into();
	let tags = vec![];
	let path = "";
	let export = "default";
	let args = vec![];
	let env = vec![("API_KEY".to_string(), "test".to_string())];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"Using API key: test\n");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}

#[tokio::test]
async fn env_not_available_in_build() {
    let artifact = temp::directory! {
        "tangram.ts" => indoc!(r#"
	        export let build = async () => {
	            return tg.process.env().SECRET_KEY
	        }
	        export default async () => {
	            const buildSecret = await tg.build(build);
	            console.log(`run: ${(await tg.process.env()).SECRET_KEY}`);
	            console.log(`build: ${buildSecret}`);
	        }
        "#),
    }.into();
    let tags = vec![];
    let path = "";
    let export = "default";
    let flags = vec![];
    let assertions = |_path: PathBuf, output: std::process::Output| async move {
        assert_success!(output);
        let stdout = std::str::from_utf8(&output.stdout).unwrap();
        assert_snapshot!(stdout, @r###"
        run: test-secret
        build: undefined
        "###);
    };

    let args = vec![];
    let env = vec![("SECRET_KEY".to_string(), "test-secret".to_string())];
    test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}

#[tokio::test]
async fn run_download() {
	let artifact = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let blob = await tg.download("https://example.com", "sha256:any");
				return tg.file(blob);
			};
		"#),
	}
	.into();
	let tags = vec![];
	let path = "";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"fil_01cvc6nxd5cmqrp4v6jq08735m6r4e08kk9wxz3fs17b5cvqs50n00");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
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
	let path = "tangram.ts";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> Uncaught Error: failed assertion
		   ./tangram.ts:2:22
		   ./foo.tg.ts:1:25
		");
	};

	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
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
	let path = "foo";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> the child process failed
		-> Uncaught Error: failed assertion
		   ./bar/tangram.ts:1:25
		");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;

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
	let path = "foo/tangram.ts";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> Uncaught Error: error
		   ./foo/tangram.ts:2:22
		   ../bar/tangram.ts:1:25
		");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
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
	let path = "tangram.ts";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> Uncaught Error: error in foo
		   ./tangram.ts:2:22
		   foo:./tangram.ts:1:25
		");
	};
	test_run(artifact, tags, path, export, args, env,flags, assertions).await;

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
	let path = "tangram.ts";
	let export = "default";
	let args = vec![];
	let env = vec![];
	let flags = vec![];
	let assertions = |_path: PathBuf, output: std::process::Output| async move {
		assert_failure!(output);
		let stderr = std::str::from_utf8(&output.stderr).unwrap();
		assert_snapshot!(stderr, @r"
		error the process failed
		-> Uncaught Error: failure in foo
		   ./tangram.ts:2:22
		   foo:./tangram.ts:2:22
		   foo:../bar/tangram.ts:2:22
		   foo:./tangram.ts:3:33
		");
	};
	test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}

#[tokio::test]
async fn executable_subpath() {
    let artifact = temp::directory! {
        "tangram.ts" => indoc!(r##"
            export default async () => {
                const dir = tg.directory({
                    "main": tg.file({
                        contents: "#!/bin/sh\necho 'This is the main executable'",
                        executable: true,
                    }),
                    "tools/helper": tg.file({
                        contents: "#!/bin/sh\necho 'This is the helper tool'",
                        executable: true,
                    }),
                });
                return dir;
            }
        "##),
    }.into();

    let tags = vec![];
    let path = "";
    let export = "default";
    let args = vec![];
    let env = vec![];
    let flags = vec!["--executable-subpath=main".into(), "--build".into()];

    let assertions = |_path: PathBuf, output: std::process::Output| async move {
        assert_success!(output);
        dbg!(&output);
        let stdout = std::str::from_utf8(&output.stdout).unwrap();
        dbg!(&stdout);
        assert_snapshot!(stdout, @"This is the main executable\n");
    };

    test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}

#[tokio::test]
async fn executable_subpath_nested() {
    let artifact = temp::directory! {
        "tangram.ts" => indoc!(r##"
            export default async () => {
                const dir = tg.directory({
                    "main": tg.file({
                        contents: "#!/bin/sh\necho 'This is the main executable'",
                        executable: true,
                    }),
                    "tools/helper": tg.file({
                        contents: "#!/bin/sh\necho 'This is the helper tool'",
                        executable: true,
                    }),
                });
                return dir;
            }
        "##),
    }.into();

    let tags = vec![];
    let path = "";
    let export = "default";
    let args = vec![];
    let env = vec![];
    let flags = vec!["--executable-subpath=tools/helper".into(), "--build".into()];

    let assertions = |_path: PathBuf, output: std::process::Output| async move {
        assert_success!(output);
        let stdout = std::str::from_utf8(&output.stdout).unwrap();
        dbg!(&stdout);
        assert_snapshot!(stdout, @"This is the helper tool\n");
    };

    test_run(artifact, tags, path, export, args, env, flags, assertions).await;
}


async fn test_run<F, Fut>(
	artifact: temp::Artifact,
	tags: Vec<(String, temp::Artifact, Option<PathBuf>)>,
	path: &str,
	export: &str,
	args: Vec<String>,
	env: Vec<(String, String)>,
	flags: Vec<String>,
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

		let path = temp.path().join(path);
		let reference = format!("{path}#{export}", path = path.display());

		let mut command = server.tg();
		command.current_dir(temp.path()).arg("run").arg(reference);
		for arg in args {
			command.arg("--arg");
			command.arg(arg);
		}
		for (env, value) in env {
			command.arg("--env");
			command.arg(format!("{}={}", env, value));
		}
		for flag in flags {
			command.arg(format!("{}", flag));
		}
		let output = command.output().await.unwrap();

		assertions(temp.path().to_owned(), output).await;
	})
	.await;
}
