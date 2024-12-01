/// Test checking out a directory.
#[tokio::test]
async fn directory() -> tg::Result<()> {
	let artifact = tg::directory! {
		"hello.txt" => "Hello, World!",
	};
	test(artifact, |server| async move {
		let path = assert_json_snapshot!(artifact, @r#"
  {
    "kind": "directory",
    "entries": {
      "hello.txt": {
        "kind": "file",
        "contents": "Hello, World!",
        "executable": false
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
	F: FnOnce(Server, PathBuf) -> Fut,
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
		(assertions)(server.clone()).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap()
}
