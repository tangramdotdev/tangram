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
async fn current_target_id() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r"export default tg.target(() => tg.Target.current.id())",
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""tgt_01wb4tdwy3mhb99vv0q9n1z0x8xxmydf1rfaxa3eahfx36ygsx5wt0""###);
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
async fn captures_error() -> tg::Result<()> {
	test(
		temp::directory! {
			"throw_error" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => { throw new Error("not so fast!"); });"#,
			}
		},
		"throw_error",
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
					import file from "./file.txt" with { type: "file" };
					export default tg.target(() => file.text());
				"#),
				"file.txt" => "I'm a plain text file!",
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""I'm a plain text file!""###);
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
						directory.get("file.txt")
							.then(tg.File.expect)
							.then((f) => f.text())
					);
				"#),
				"directory" => temp::directory! {
					"file.txt" => "I'm a plain text file inside a directory!",
				}
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""I'm a plain text file inside a directory!""###);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn directory_get_follows_symlinks() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import directory from "./directory" with { type: "directory" };
					export default tg.target(async () =>
						directory.get("link")
							.then(tg.File.expect)
							.then((f) => f.text())
					);
				"#),
				"directory" => temp::directory! {
					"file.txt" => "I'm a plain text file inside a directory through a symlink!",
					"link" => temp::symlink!("./file.txt")
				}
			},
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r###""I'm a plain text file inside a directory through a symlink!""###);
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
			assert_snapshot!(error, @"failed to resolve");
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
			assert_snapshot!(error, @r"Uncaught Error: invalid checksum, expected sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa but got sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9");
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
			assert_snapshot!(error, @"Uncaught Error: invalid checksum");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_artifact_archive_extract_roundtrip() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					import directory from "./directory" with { type: "directory" };
					export default tg.target(async () => {
						let archived_directory = await tg.archive(directory, "tar");
						let extracted_archive = await tg.extract(archived_directory, "tar");
						return extracted_archive;
					});
				"#),
				"directory" => temp::directory! {
					"file.txt" => "contents",
					"link" => temp::symlink!("./file.txt")
				}
			}
		},
		"foo",
		"default",
		vec![],
		|_, outcome| async move {
			let output = outcome.into_result()?;
			assert_snapshot!(output, @r"");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn builtin_blob_compress_decompress_gz_roundtrip() -> tg::Result<()> {
	test(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => indoc!(r#"
					export default tg.target(async () => {
						let blob = await tg.blob("contents");
						let compressedBlob = await tg.compress(blob, "gz");
						let decompressedBlob = await tg.decompress(blob, "gz");
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
			assert_snapshot!(output, @r"");
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(
	artifact: temp::Artifact,
	path: &str,
	target: &str,
	args: Vec<tg::Value>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::build::Outcome) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let artifact_temp = Temp::new_persistent();
	artifact.to_path(artifact_temp.as_ref()).await.map_err(
		|source| tg::error!(!source, %path = artifact_temp.path().display(), "failed to write the artifact"),
	)?;
	let server_temp = Temp::new_persistent();
	let mut options = Config::with_path(server_temp.path().to_owned());
	options.advanced = tangram_server::config::Advanced {
		write_build_logs_to_stderr: true,
		..Default::default()
	};
	options.build = Some(tangram_server::config::Build::default());
	options.build_heartbeat_monitor =
		Some(tangram_server::config::BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
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
		let executable = Some(tg::target::Executable::Module(tg::Module {
			kind: tg::module::Kind::Js,
			referent: tg::Referent {
				item: tg::module::Item::Object(artifact),
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
