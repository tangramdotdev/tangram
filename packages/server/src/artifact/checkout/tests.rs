use crate::{util::fs::cleanup, Config, Server};
use futures::FutureExt as _;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, path::PathBuf, pin::pin};
use tangram_client as tg;
use tangram_either::Either;
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
	cleanup(server_temp, server).await;
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
	cleanup(server_temp, server).await;
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
	cleanup(server_temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn symlink_shared_target() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let test_artifact = temp::directory! {
			"outer" => temp::directory! {
				"file.txt" => "test",
				"link" => temp::symlink!("file.txt"),
				"other_link" => temp::symlink!("file.txt")
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
							"other_link" => temp::symlink!(format!("../{}/file.txt", incoming_artifact_id.to_string()))
						}
					}
				},
				"file.txt" => "test",
				"link" => temp::symlink!(format!("./.tangram/artifacts/{}/file.txt", incoming_artifact_id.to_string())),
				"other_link" => temp::symlink!(format!("./.tangram/artifacts/{}/file.txt", incoming_artifact_id.to_string()))
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
	cleanup(server_temp, server).await;
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
	cleanup(server_temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn file_with_graph_object() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let blob = tg::Blob::from("hi from a graph");
		let file_node = tg::graph::object::Node::File(tg::graph::object::File {
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
	cleanup(server_temp, server).await;
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
	cleanup(server_temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn nested_cyclic_links() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;

	let result = AssertUnwindSafe(async {
		let test_artifact = temp::directory! {
			"a" => temp::directory! {
				"b" => temp::directory! {
					"c" => temp::symlink!("d"),
					"d" => temp::symlink!("c"),
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
			path: incoming.as_ref().join("a"),
		};
		let incoming_artifact = tg::Artifact::check_in(&server, checkin_arg).await?;
		let incoming_artifact_id = incoming_artifact.id(&server).await?;

		let outgoing = Temp::new();
		tokio::fs::create_dir_all(outgoing.path()).await.map_err(
			|source| tg::error!(!source, %path = outgoing.path().display(), "failed to create temp dir"),
		)?;
		let arg = tg::artifact::checkout::Arg {
			dependencies: true,
			force: false,
			path: Some(outgoing.path().to_owned().join("a")),
		};
		let output = tg::Artifact::with_id(incoming_artifact_id.clone())
			.check_out(&server, arg)
			.await?;
		let target = tokio::fs::read_link(output.join("b/c"))
			.await
			.map_err(|source| tg::error!(!source, "failed to read link"))?;
		let expected = PathBuf::from("../.tangram/artifacts")
			.join(incoming_artifact_id.to_string())
			.join("b/d");
		assert_eq!(target, expected);

		let target = tokio::fs::read_link(output.join("b/d"))
			.await
			.map_err(|source| tg::error!(!source, "failed to read link"))?;
		let expected = PathBuf::from("../.tangram/artifacts")
			.join(incoming_artifact_id.to_string())
			.join("b/c");
		assert_eq!(target, expected);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	cleanup(server_temp, server).await;
	result.unwrap()
}

#[tokio::test]
async fn modified_directory() -> tg::Result<()> {
	let server_temp = Temp::new();
	let server_options = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_options).await?;
	let result = AssertUnwindSafe({
		let server = server.clone();
		async move {
			// Create a new graph that contains a file and link that points to it.
			let graph = tg::graph::Object {
				nodes: vec![
					tg::graph::Node::Directory(tg::graph::object::Directory {
						entries: [
							("file".to_owned(), Either::Left(1)),
							("link".to_owned(), Either::Left(2)),
						]
						.into_iter()
						.collect(),
					}),
					tg::graph::Node::File(tg::graph::object::File {
						contents: tg::Blob::with_reader(&server, b"a".as_slice()).await?,
						dependencies: BTreeMap::new(),
						executable: false,
					}),
					tg::graph::Node::Symlink(tg::graph::object::Symlink {
						artifact: Some(Either::Left(0)),
						subpath: Some("file".into()),
					}),
				],
			};
			let graph = tg::Graph::with_object(graph);

			// Create a new directory with a file not contained within the graph.
			let other_file =
				tg::File::with_contents(tg::Blob::with_reader(&server, b"b".as_slice()).await?);
			let entries = [
				(
					"file".to_owned(),
					tg::File::with_graph_and_node(graph.clone(), 1).into(),
				),
				(
					"link".to_owned(),
					tg::Symlink::with_graph_and_node(graph.clone(), 2).into(),
				),
				("other-file".to_owned(), other_file.clone().into()),
			]
			.into_iter()
			.collect();
			let directory = tg::Directory::with_entries(entries);

			// Assert the entries of the new directory.
			assert_eq!(
				tg::File::with_graph_and_node(graph.clone(), 1)
					.id(&server)
					.await?,
				directory
					.entries(&server)
					.await?
					.get("file")
					.unwrap()
					.id(&server)
					.await?
					.unwrap_file(),
			);
			assert_eq!(
				tg::Symlink::with_graph_and_node(graph.clone(), 2)
					.id(&server)
					.await?,
				directory
					.entries(&server)
					.await?
					.get("link")
					.unwrap()
					.id(&server)
					.await?
					.unwrap_symlink(),
			);
			assert_eq!(
				other_file.id(&server).await?,
				directory
					.entries(&server)
					.await?
					.get("other-file")
					.unwrap()
					.id(&server)
					.await?
					.unwrap_file(),
			);
			let temp = temp::Temp::new();
			let artifact = tg::Artifact::from(directory);
			let arg = tg::artifact::checkout::Arg {
				path: Some(temp.path().to_owned()),
				..tg::artifact::checkout::Arg::default()
			};
			let _path = artifact.check_out(&server, arg).await?;
			Ok::<_, tg::Error>(())
		}
	})
	.catch_unwind()
	.await;
	cleanup(server_temp, server).await;
	result.unwrap()
}
