use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_server::{Config, Server};
use tangram_temp::Temp;

#[tokio::test]
async fn get_symlink() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = tg::directory! {
			"file.txt" => "Hello, World!",
			"link" => tg::symlink!(None, Some("file.txt".into())),
		};
		let artifact = directory.get(&server, "link").await?;
		assert!(artifact.is_file());
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap().unwrap();
	Ok(())
}

#[tokio::test]
async fn get_file_through_symlink() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = tg::directory! {
			"directory" => tg::directory! {
				"file.txt" => "Hello, World!",
			},
			"link" => tg::symlink!(None, Some("directory".into())),
		};
		let artifact = directory.get(&server, "link/file.txt").await?;
		assert!(artifact.is_file());
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	temp.remove().await.ok();
	result.unwrap().unwrap();
	Ok(())
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
}
