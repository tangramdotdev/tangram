use crate::{util::fs::cleanup_instance, Config, Server};
use futures::FutureExt as _;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn file() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let file = tg::File::with_contents("test");
		let file_id = file.id(&server).await?;

		let actual_temp = Temp::new();
		tokio::fs::create_dir_all(actual_temp.path())
			.await
			.map_err(
				|source| tg::error!(!source, %path = actual_temp.path().display(), "failed to create temp dir"),
			)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			path: Some(actual_temp.path().to_owned().join("test.txt")),
		};
		let stream = server
			.check_out_artifact(&file_id.clone().into(), checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let expected_artifact = temp::directory! { "test.txt" => "test" };
		let matches = expected_artifact.matches(actual_temp.path()).await.unwrap();
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}

#[tokio::test]
async fn executable_file() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let file = tg::file::Builder::new("test").executable(true).build();
		let file_id = file.id(&server).await?;

		let actual_temp = Temp::new();
		tokio::fs::create_dir_all(actual_temp.path())
			.await
			.map_err(
				|source| tg::error!(!source, %path = actual_temp.path().display(), "failed to create temp dir"),
			)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			path: Some(actual_temp.path().to_owned().join("test.txt")),
		};
		let stream = server
			.check_out_artifact(&file_id.clone().into(), checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let expected_artifact = temp::directory! {
			"test.txt" => temp::file!("test", true)
		};
		let matches = expected_artifact.matches(actual_temp.path()).await.unwrap();
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}

#[tokio::test]
async fn symlink() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let test_artifact = temp::directory! {
			"outer" => temp::directory! {
				"file.txt" => "test",
				"link" => temp::symlink!("file.txt")
			}
		};
		let incoming = Temp::new();
		test_artifact.to_path(incoming.path()).await.map_err(
			|source| tg::error!(!source, %path = incoming.path().display(), "failed to write the artifact"),
		)?;
		let checkin_arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: incoming.as_ref().join("outer"),
		};
		let incoming_artifact = tg::Artifact::check_in(&server, checkin_arg).await?;
		let incoming_artifact_id = incoming_artifact.id(&server).await?;

		let outgoing = Temp::new();
		tokio::fs::create_dir_all(outgoing.path()).await.map_err(
			|source| tg::error!(!source, %path = outgoing.path().display(), "failed to create temp dir"),
		)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(outgoing.path().to_owned().join("outer")),
		};
		let stream = server
			.check_out_artifact(&incoming_artifact_id, checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let expected_artifact = temp::directory! {
			"outer" => temp::directory! {
				".tangram" => temp::directory! {
					"artifacts" => temp::directory! {
						incoming_artifact_id.to_string() => temp::directory! {
							"file.txt" => "test",
							"link" => temp::symlink!(format!("../{}/file.txt", incoming_artifact_id.to_string()))
						}
					}
				},
				"file.txt" => "test",
				"link" => temp::symlink!(format!("./.tangram/artifacts/{}/file.txt", incoming_artifact_id.to_string()))
			}
		};
		let matches = expected_artifact
			.matches(outgoing.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to match artifact"))?;
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}

#[tokio::test]
async fn symlink_to_symlink() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let test_artifact = temp::directory! {
			"outer" => temp::directory! {
				"file.txt" => "test",
				"link" => temp::symlink!("file.txt"),
				"subdirectory" => temp::directory! {
					"sublink" => temp::symlink!("../link")
				}
			}
		};
		let incoming = Temp::new();
		test_artifact.to_path(incoming.path()).await.map_err(
			|source| tg::error!(!source, %path = incoming.path().display(), "failed to write the artifact"),
		)?;
		let checkin_arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: incoming.as_ref().join("outer"),
		};
		let incoming_artifact = tg::Artifact::check_in(&server, checkin_arg).await?;
		let incoming_artifact_id = incoming_artifact.id(&server).await?;

		let outgoing = Temp::new();
		tokio::fs::create_dir_all(outgoing.path()).await.map_err(
			|source| tg::error!(!source, %path = outgoing.path().display(), "failed to create temp dir"),
		)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(outgoing.path().to_owned().join("outer")),
		};
		let stream = server
			.check_out_artifact(&incoming_artifact_id, checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let expected_artifact = temp::directory! {
			"outer" => temp::directory! {
				".tangram" => temp::directory! {
					"artifacts" => temp::directory! {
						incoming_artifact_id.to_string() => temp::directory! {
							"file.txt" => "test",
							"link" => temp::symlink!(format!("../{}/file.txt", incoming_artifact_id.to_string())),
							"subdirectory" => temp::directory! {
								"sublink" => temp::symlink!(format!("../../{}/link", incoming_artifact_id.to_string()))
							}
						}
					}
				},
				"file.txt" => "test",
				"link" => temp::symlink!(format!("./.tangram/artifacts/{}/file.txt", incoming_artifact_id.to_string())),
				"subdirectory" => temp::directory! {
					"sublink" => temp::symlink!(format!("../.tangram/artifacts/{}/link", incoming_artifact_id.to_string()))
				}
			}
		};
		let matches = expected_artifact
			.matches(outgoing.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to match artifact"))?;
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}

#[tokio::test]
async fn file_with_graph_object() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let blob = tg::Blob::from("hi from a graph");
		let file_node = tg::graph::node::Node::File(tg::graph::node::File {
			contents: blob,
			dependencies: BTreeMap::new(),
			executable: false,
		});
		let graph_object = tg::graph::Object {
			nodes: vec![file_node],
		};
		let graph = tg::Graph::with_object(graph_object);
		let file = tg::File::with_graph_and_node(graph, 0);
		let file_id = file.id(&server).await?;

		let actual_temp = Temp::new();
		tokio::fs::create_dir_all(actual_temp.path())
			.await
			.map_err(
				|source| tg::error!(!source, %path = actual_temp.path().display(), "failed to create temp dir"),
			)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			path: Some(actual_temp.path().to_owned().join("test.txt")),
		};
		let stream = server
			.check_out_artifact(&file_id.clone().into(), checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;

		let expected_artifact = temp::directory! { "test.txt" => "hi from a graph" };
		let matches = expected_artifact.matches(actual_temp.path()).await.unwrap();
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}

#[tokio::test]
async fn directory() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let test_artifact = temp::directory! {
			"directory" => temp::directory! {
				"hello.txt" => "Hello, world!",
				"subdirectory" => temp::directory! {
					"inner.txt" => "nested file!",
				}
			}
		};
		let incoming = Temp::new();
		test_artifact.to_path(incoming.path()).await.map_err(
			|source| tg::error!(!source, %path = incoming.path().display(), "failed to write the artifact"),
		)?;
		let checkin_arg = tg::artifact::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			path: incoming.as_ref().join("directory"),
		};
		let incoming_artifact = tg::Artifact::check_in(&server, checkin_arg).await?;
		let incoming_artifact_id = incoming_artifact.id(&server).await?;

		let outgoing = Temp::new();
		tokio::fs::create_dir_all(outgoing.path()).await.map_err(
			|source| tg::error!(!source, %path = outgoing.path().display(), "failed to create temp dir"),
		)?;
		let checkout_arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(outgoing.path().to_owned().join("directory")),
		};
		let stream = server
			.check_out_artifact(&incoming_artifact_id, checkout_arg)
			.await?;
		let _ = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let matches = test_artifact
			.matches(outgoing.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to match artifact"))?;
		assert!(matches);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup_instance(server_temp, server).await?;
	result.unwrap()
}
