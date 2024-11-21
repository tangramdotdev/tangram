use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt;
use insta::assert_snapshot;
use std::{future::Future, panic::AssertUnwindSafe, path::Path};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

struct TestArtifact {
	// The server the artifact exists upon.
	server: Server,

	// The tangram artifact.
	artifact: tg::Artifact,

	// The artifact that was checked in.
	checkin: temp::Artifact,

	// The artifact that was checked out.
	checkout: temp::Artifact,
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	test(
		temp::directory! {
			"hello.txt" => "hello, world!",
		},
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.directory({
            	"hello.txt": tg.file({
            		"contents": tg.leaf("hello, world!"),
            	}),
            })
            "#);
			assert_snapshot!(&output_b, @r#"
            tg.directory({
            	"hello.txt": tg.file({
            		"contents": tg.leaf("hello, world!"),
            	}),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn file() -> tg::Result<()> {
	test(
		temp::file!("hello, world!"),
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.file({
            	"contents": tg.leaf("hello, world!"),
            })
            "#);
			assert_snapshot!(&output_b, @r#"
            tg.file({
            	"contents": tg.leaf("hello, world!"),
            })
            "#);
			Ok(())
		},
	)
	.await
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	test(
		temp::directory! {
			"link" => temp::symlink!("hello.txt"),
			"hello.txt" => "hello, world!",
		},
		"".as_ref(),
		|a, b| async move {
			assert_eq!(a.id().await, b.id().await);
			let output_a = a.output().await;
			let output_b = b.output().await;
			assert_eq!(&output_a, &output_b);
			assert_snapshot!(&output_a, @r#"
            tg.symlink({
            	"subpath": "hello.txt",
            })
            "#);
			assert_snapshot!(&output_b, @r#"
            tg.symlink({
            	"subpath": "hello.txt",
            })"#);
			Ok(())
		},
	)
	.await
}

async fn test<F, Fut>(checkin: temp::Artifact, path: &Path, assertions: F) -> tg::Result<()>
where
	F: FnOnce(TestArtifact, TestArtifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	// Create the first server.
	let temp1 = Temp::new();
	let config = Config::with_path(temp1.path().to_owned());
	let server1 = Server::start(config.clone()).await?;

	// Create the second server.
	let temp2 = Temp::new();
	let config = Config::with_path(temp2.path().to_owned());
	let server2 = Server::start(config.clone()).await?;

	// Run the test.
	let result = AssertUnwindSafe(async {
		// Create an initial temp.
		let temp = Temp::new();

		// Realize the artifact.
		checkin.to_path(temp.path()).await.unwrap();

		// Check in the artifact to the first server.
		let arg = tg::artifact::checkin::Arg {
			path: temp.path().join(path),
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		let artifact = tg::Artifact::check_in(&server1, arg).await?;

		// Check the artifact out from the first server.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			dependencies: true,
			force: false,
		};
		let path = artifact.check_out(&server1, arg).await?;
		let checkout = temp::Artifact::with_path(&path).await?;

		// Create the test data for the first server.
		let artifact1 = TestArtifact {
			server: server1.clone(),
			artifact,
			checkin,
			checkout,
		};

		// Check the artifact into a second server.
		let checkin = temp::Artifact::with_path(&path).await?;
		let arg = tg::artifact::checkin::Arg {
			path,
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
		};
		let artifact = tg::Artifact::check_in(&server2, arg).await?;

		// Check it out.
		let temp = Temp::new();
		let arg = tg::artifact::checkout::Arg {
			path: Some(temp.path().to_owned()),
			dependencies: true,
			force: false,
		};
		let path = artifact.check_out(&server2, arg).await?;
		let checkout = temp::Artifact::with_path(&path).await?;
		let artifact2 = TestArtifact {
			server: server2.clone(),
			artifact,
			checkin,
			checkout,
		};

		assertions(artifact1, artifact2).await
	})
	.catch_unwind()
	.await;

	// Cleanup the servers.
	cleanup(temp1, server1).await;
	cleanup(temp2, server2).await;
	result.unwrap()
}

impl TestArtifact {
	async fn output(&self) -> String {
		let object: tg::Object = self.artifact.clone().into();
		object.load_recursive(&self.server).await.unwrap();
		let value = tg::Value::Object(object);
		let options = tg::value::print::Options {
			recursive: true,
			style: tg::value::print::Style::Pretty { indentation: "\t" },
		};
		value.print(options)
	}

	async fn id(&self) -> tg::artifact::Id {
		self.artifact.id(&self.server).await.unwrap()
	}
}
