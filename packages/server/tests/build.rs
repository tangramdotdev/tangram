use futures::FutureExt as _;
use indoc::indoc;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe};
use tangram_client::{self as tg, handle::Ext};
use tangram_server::{Config, Server};
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn hello_world() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello, world!")"#,
			}
		},
		"hello",
		"default",
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, world!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn accepts_arg() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello" => temp::directory! {
				"tangram.ts" => r"export default tg.target((name: string) => `Hello, ${name}!`)",
			}
		},
		"hello",
		"default",
		Some(&["Cool Person"]),
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""Hello, Cool Person!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn unix_process() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(() => {
						return tg.target("echo 'Hello from a unix process!' > $OUTPUT")
							.then((t) => t.output());
					});
				"#),
			}
		},
		"hello",
		"default",
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
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
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
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
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
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
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn named_target() -> tg::Result<()> {
	test(
		temp::directory! {
			"two_targets" => temp::directory! {
				"tangram.ts" => indoc!(r"
					export let five = tg.target(() => 5);
					export let six = tg.target(() => 6);
				"),
			},
		},
		"two_targets",
		"five",
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @"5");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn captures_error() -> tg::Result<()> {
	test(
		temp::directory! {
			"throw_error" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => { throw new Error("not so fast!"); });"#,
			}
		},
		"throw_error",
		"default",
		None,
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"Uncaught Error: not so fast!");
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
		None,
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"");
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
		None,
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @r"");
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
					export let greeting = tg.target(() => "hello");
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
		None,
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
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
		None,
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"Uncaught Error: cycle detected");
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
		None,
		|_, outcome| async move {
			let error = outcome.into_result().unwrap_err();
			assert_snapshot!(error, @"Uncaught Error: cycle detected");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(
	artifact: temp::Artifact,
	path: &str,
	target: &str,
	target_args: Option<&[&str]>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::build::Outcome) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.advanced = tangram_server::config::Advanced {
		write_build_logs_to_stderr: true,
		..Default::default()
	};
	options.build = Some(tangram_server::config::Build::default());
	options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: directory.as_ref().join(path),
		};
		let artifact = tg::Artifact::check_in(&server, arg)
			.await?
			.try_unwrap_directory()
			.unwrap();
		let artifact = artifact.clone().into();
		let subpath = Some("tangram.ts".parse().unwrap());
		let executable = Some(tg::target::Executable::Module(tg::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: tg::module::Item::Object(artifact),
				subpath,
				tag: None,
			},
		}));
		let args: Vec<tg::Value> = std::iter::once(target.into())
			.chain(
				target_args
					.map(|args| args.iter())
					.into_iter()
					.flatten()
					.map(|arg| (*arg).into()),
			)
			.collect();
		let host = "js";
		let target = tg::target::Builder::new(host)
			.executable(executable)
			.args(args)
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
	temp.remove()
		.await
		.map_err(|source| tg::error!(!source, "failed to remove temp"))?;
	result.unwrap()
}

#[ctor::ctor]
fn ctor() {
	test_init();
}

fn test_init() {
	// Set file descriptor limit.
	let limit = 65536;
	let rlimit = libc::rlimit {
		rlim_cur: limit,
		rlim_max: limit,
	};
	let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlimit) };
	assert!(ret == 0, "failed to set the file descriptor limit");

	// Initialize v8.
	v8::icu::set_common_data_73(deno_core_icudata::ICU_DATA).unwrap();
	let platform = v8::new_default_platform(0, true);
	v8::V8::initialize_platform(platform.make_shared());
	v8::V8::initialize();
}
