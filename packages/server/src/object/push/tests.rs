use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use std::{panic::AssertUnwindSafe, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::Temp;

#[tokio::test]
async fn push_file() -> tg::Result<()> {
	let remote_temp = Temp::new();
	let remote_options = Config::with_path(remote_temp.path().to_owned());
	let remote = Server::start(remote_options).await?;

	let server_temp = Temp::new();
	let server_options =
		Config::with_path_and_remote(server_temp.path().to_owned(), remote_temp.path());
	let server = Server::start(server_options).await?;

	let other_temp = Temp::new();
	let other_options =
		Config::with_path_and_remote(other_temp.path().to_owned(), remote_temp.path());
	let other = Server::start(other_options).await?;

	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let file = file.id(&server).await?;
		let server_get_output = server.try_get_object(&file.clone().into()).await?;

		let arg = tg::object::push::Arg {
			remote: "default".to_string(),
		};
		let stream = server.push_object(&file.clone().into(), arg).await?;
		pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let other_get_output = other.try_get_object(&file.into()).await?;
		assert_eq!(server_get_output, other_get_output);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(server_temp, server).await;
	cleanup(other_temp, other).await;
	cleanup(remote_temp, remote).await;

	result.unwrap()
}

#[tokio::test]
async fn push_simple_directory() -> tg::Result<()> {
	let remote_temp = Temp::new();
	let remote_options = Config::with_path(remote_temp.path().to_owned());
	let remote = Server::start(remote_options).await?;

	let server_temp = Temp::new();
	let server_options =
		Config::with_path_and_remote(server_temp.path().to_owned(), remote_temp.path());
	let server = Server::start(server_options).await?;

	let other_temp = Temp::new();
	let other_options =
		Config::with_path_and_remote(other_temp.path().to_owned(), remote_temp.path());
	let other = Server::start(other_options).await?;

	let result = AssertUnwindSafe(async {
		let directory = tg::directory! {
			"hello.txt" => tg::file!("Hello, world!"),
			"subdirectory" => tg::directory! {
				"nested.txt" => tg::file!("I'm nested!")
			}
		};
		let directory = directory.id(&server).await?;
		let server_get_output = server.try_get_object(&directory.clone().into()).await?;

		let arg = tg::object::push::Arg {
			remote: "default".to_string(),
		};
		let stream = server.push_object(&directory.clone().into(), arg).await?;
		pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let arg = tg::object::pull::Arg {
			remote: "default".to_string(),
		};
		let stream = other.pull_object(&directory.clone().into(), arg).await?;
		pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let other_get_output = other.try_get_object(&directory.clone().into()).await?;

		assert_eq!(server_get_output, other_get_output);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;

	cleanup(server_temp, server).await;
	cleanup(other_temp, other).await;
	cleanup(remote_temp, remote).await;

	result.unwrap()
}
