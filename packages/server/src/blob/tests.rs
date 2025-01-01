use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use insta::assert_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn create_from_file() -> tg::Result<()> {
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		// Create the file.
		let temp = Temp::new();
		let artifact: temp::Artifact = temp::file!("hello, world!\n").into();
		artifact.to_path(temp.path()).await.unwrap();

		// Create the reader.
		let reader = tokio::fs::File::open(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;

		let blob = server.create_blob_with_reader(reader).await?.blob;
		let blob = tg::Blob::with_id(blob);
		let object: tg::Object = blob.into();
		object.load_recursive(&server).await?;
		let value = tg::Value::from(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"tg.leaf("hello, world!\n")"#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn create_from_file_in_cache() -> tg::Result<()> {
	let temp = Temp::new();
	let mut options = Config::with_path(temp.path().to_owned());
	options.advanced.write_blobs_to_blobs_directory = true;

	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		// Create the file.
		let temp = Temp::new();
		let artifact: temp::Artifact = temp::file!("hello, world!\n").into();
		artifact.to_path(temp.path()).await.unwrap();

		// Create the blob.
		let blob = server.create_blob_with_path(temp.path()).await?.blob;
		let blob = tg::Blob::with_id(blob);
		let object: tg::Object = blob.into();
		object.load_recursive(&server).await?;
		let value = tg::Value::from(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		let output = value.print(options);
		assert_snapshot!(output, @r#"tg.leaf("hello, world!\n")"#);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(temp, server).await;
	result.unwrap()
}
