use insta::assert_yaml_snapshot;
use scopeguard::defer;
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_server::{Config, Server};
use tangram_temp::{artifact, Temp};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test() -> Result {
	println!("wow");
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	defer! {
		println!("hi");
		server.stop();
	}
	let directory = Temp::new();
	artifact!({
		"directory": {
			"README.md": "Hello, World!",
		}
	})
	.to_path(&directory)
	.await?;
	let arg = tg::artifact::checkin::Arg {
		destructive: false,
		deterministic: false,
		ignore: true,
		locked: false,
		path: directory.as_ref().join("directory"),
	};
	let stream = server.check_in_artifact(arg).await?;
	let output = pin!(stream)
		.try_last()
		.await?
		.and_then(|event| event.try_unwrap_output().ok())
		.ok_or_else(|| tg::error!("stream ended without output"))?;
	assert_yaml_snapshot!(output);
	println!("bottom");
	Ok(())
}