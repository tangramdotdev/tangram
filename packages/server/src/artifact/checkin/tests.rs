use crate::{Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe, path::Path, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, artifact, Temp};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn basic() -> Result {
	let artifact = artifact!({
		"directory": {
			"README.md": "Hello, World!",
		}
	});
	let path = "directory";
	test_checkin(artifact, path, |server, artifact| async { Ok(()) }).await
}

async fn test_checkin<F, Fut>(artifact: temp::Artifact, path: &str, assertions: F) -> Result
where
	F: FnOnce(&Server, &tg::Artifact) -> Fut,
	Fut: Future<Output = Result>,
{
	test_server(|server| async move {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await?;
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: directory.as_ref().join(path),
		};
		let stream = server.check_in_artifact(arg).await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = tg::Artifact::with_id(output.artifact);
		tg::Object::from(artifact.clone())
			.load_recursive(&server)
			.await?;
		let output = tg::Value::from(artifact.clone()).print(tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		});
		assert_snapshot!(output);
		Ok(())
	})
	.await
}

async fn test_server<F, Fut>(f: F) -> Result
where
	F: FnOnce(Server) -> Fut,
	Fut: Future<Output = Result>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe((f)(server.clone())).catch_unwind().await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
