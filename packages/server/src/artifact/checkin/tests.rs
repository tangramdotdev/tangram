use crate::{Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe, path::Path, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, artifact, Temp};

macro_rules! test {
	($artifact:expr, $path:expr, $f:expr,) => {
		async {
			let temp = Temp::new();
			let options = Config::with_path(temp.path().to_owned());
			let server = Server::start(options).await?;
			let result = AssertUnwindSafe(async {
				let directory = Temp::new();
				$artifact
					.to_path(directory.as_ref())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the artifact"))?;
				let arg = tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					path: directory.as_ref().join($path),
				};
				let artifact = tg::Artifact::check_in(&server, arg).await?;
				let object = tg::Object::from(artifact.clone());
				object.load_recursive(&server).await?;
				let value = tg::Value::from(artifact.clone());
				let options = tg::value::print::Options {
					recursive: true,
					style: tg::value::print::Style::Pretty { indentation: "\t" },
				};
				let output = value.print(options);
				$f(server.clone(), artifact, output).await?;
				Ok::<_, tg::Error>(())
			})
			.catch_unwind()
			.await;
			server.stop();
			server.wait().await;
			result.unwrap()
		}
	};
}

#[tokio::test]
async fn readme() -> tg::Result<()> {
	test!(
		artifact!({
			"directory": {
				"README.md": "Hello, World!!",
			}
		}),
		"directory",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"README.md": tg.file({
   		"contents": tg.leaf("Hello, World!!"),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn default_module() -> tg::Result<()> {
	test!(
		artifact!({
			"directory": {
				"tangram.ts": "export default tg.target(() => {})",
			}
		}),
		"directory",
		|_, _, output| async move {
			assert_snapshot!(output, @r#"
   tg.directory({
   	"tangram.ts": tg.file({
   		"contents": tg.leaf("export default tg.target(() => {})"),
   	}),
   })
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}
