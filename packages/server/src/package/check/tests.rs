// use crate::{util::fs::cleanup, Config, Server};
// use futures::FutureExt as _;
// use indoc::indoc;
// use insta::assert_yaml_snapshot;
// use std::{future::Future, panic::AssertUnwindSafe};
// use tangram_client as tg;
// use tangram_temp::{self as temp, Temp};

// #[tokio::test]
// async fn hello_world() -> tg::Result<()> {
// 	test(
// 		temp::directory! {
// 			"tangram.ts" => indoc!(r#"
// 				export default tg.target(() => "Hello, world!");
// 			"#),
// 		},
// 		|_, output| async move {
// 			assert!(output.diagnostics.is_empty(), "expected no diagnostics");
// 			Ok(())
// 		},
// 	)
// 	.await
// }

// // #[tokio::test]
// // async fn nonexistent_function() -> tg::Result<()> {
// // 	test(
// // 		temp::directory! {
// // 			"tangram.ts" => indoc!(r"
// // 				export default tg.target(() => foo());
// // 			"),
// // 		},
// // 		|_, output| async move {
// // 			assert_eq!(output.diagnostics.len(), 1);
// // 			let diagnostic = output.diagnostics.first().unwrap();
// // 			assert_yaml_snapshot!(diagnostic, @r"");
// // 			Ok(())
// // 		},
// // 	)
// // 	.await
// // }

// async fn test<F, Fut>(artifact: temp::Artifact, assertions: F) -> tg::Result<()>
// where
// 	F: FnOnce(Server, tg::package::check::Output) -> Fut,
// 	Fut: Future<Output = tg::Result<()>>,
// {
// 	let directory = Temp::new_persistent();
// 	artifact.to_path(directory.as_ref()).await.map_err(
// 		|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
// 	)?;
// 	let temp = Temp::new_persistent();
// 	let options = Config::with_path(temp.path().to_owned());
// 	let server = Server::start(options).await?;

// 	// let rt = tokio::runtime::Runtime::new().unwrap();
// 	// let handle = rt.handle().clone();
// 	let handle = tokio::runtime::Handle::current();

// 	let result = AssertUnwindSafe(async {
// 		dbg!(directory.path());
// 		let checkin_arg = tg::artifact::checkin::Arg {
// 			destructive: false,
// 			deterministic: false,
// 			ignore: true,
// 			locked: false,
// 			path: directory.to_owned(),
// 		};
// 		let package = tg::Artifact::check_in(&server, checkin_arg)
// 			.await?
// 			.try_unwrap_directory()
// 			.map_err(|source| tg::error!(!source, "expected a directory"))?;
// 		let package = package.id(&server).await?;
// 		dbg!(&package);
// 		let arg = tg::package::check::Arg {
// 			package,
// 			remote: None,
// 		};
// 		// let output = server.check_package_with_handle(arg, handle).await?;
// 		let cloned = server.clone();
// 		let output = tokio::task::spawn(async move {
// 			cloned.check_package_with_handle(arg, handle.clone()).await
// 		})
// 		.await
// 		.unwrap()
// 		.unwrap();
// 		(assertions)(server.clone(), output).await?;
// 		Ok(())
// 	})
// 	.catch_unwind()
// 	.await;
// 	cleanup(temp, server).await;
// 	// drop(rt);
// 	result.unwrap()
// }
