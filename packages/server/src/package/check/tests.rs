use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use indoc::indoc;
use insta::assert_json_snapshot;
use std::{future::Future, panic::AssertUnwindSafe};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn hello_world() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default tg.target(() => "Hello, World!");
			"#),
		},
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "diagnostics": []
   }
   "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn nonexistent_function() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" => indoc!(r"
				export default tg.target(() => foo());
			"),
		},
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "diagnostics": [
       {
         "location": {
           "module": {
             "kind": "ts",
             "referent": {
               "item": "dir_01sb97esva2e75cyaar1szc6835pcvkzd0eb486m5fmye3etcd3nc0",
               "subpath": "tangram.ts"
             }
           },
           "range": {
             "start": {
               "line": 0,
               "character": 31
             },
             "end": {
               "line": 0,
               "character": 34
             }
           }
         },
         "severity": "error",
         "message": "Cannot find name 'foo'."
       }
     ]
   }
   "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn no_return_value() -> tg::Result<()> {
	test(
		temp::directory! {
			"tangram.ts" => indoc!(r"
				export default tg.target(() => {});
			"),
		},
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "diagnostics": []
   }
   "#);
			Ok(())
		},
	)
	.await
}

async fn test<F, Fut>(artifact: temp::Artifact, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, tg::package::check::Output) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let directory = Temp::new_persistent();
	artifact.to_path(directory.as_ref()).await.map_err(
		|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
	)?;
	let temp = Temp::new_persistent();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let checkin_arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: directory.to_owned(),
		};
		let package = tg::Artifact::check_in(&server, checkin_arg)
			.await?
			.try_unwrap_directory()
			.map_err(|source| tg::error!(!source, "expected a directory"))?;
		let package = package.id(&server).await?;
		let arg = tg::package::check::Arg {
			package,
			remote: None,
		};
		let server = server.clone();
		let output = server.check_package(arg).await?;
		(assertions)(server.clone(), output).await?;
		Ok(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
