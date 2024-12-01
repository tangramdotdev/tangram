use crate::{Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::{panic::AssertUnwindSafe, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

/// Test checking out a directory.
#[tokio::test]
async fn directory() -> tg::Result<()> {
	let artifact = tg::directory! {
		"hello.txt" => "Hello, World!",
	};
	test(artifact, |_, cache| async move {
		assert_json_snapshot!(cache, @r#"
  {
    "kind": "directory",
    "entries": {
      "dir_01gn2yn2wk00wh3w1tcse628ghxj734a2c6qjd8e7g4553qzq2vs1g": {
        "kind": "directory",
        "entries": {
          "hello.txt": {
            "kind": "file",
            "contents": "Hello, World!",
            "executable": false
          }
        }
      }
    }
  }
  "#);
		Ok::<_, tg::Error>(())
	})
	.await
}

async fn test<F, Fut>(artifact: impl Into<tg::Artifact>, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, temp::Artifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let config = Config::with_path(temp.path().to_owned());
	let server = Server::start(config).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::artifact::checkout::Arg {
			force: false,
			path: None,
		};
		let artifact = artifact.into().id(&server).await?;
		let stream = server.check_out_artifact(&artifact, arg).await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = temp::Artifact::with_path(&server.cache_path()).await?;
		(assertions)(server.clone(), artifact).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap()
}
