use crate::{Config, Server};
use futures::FutureExt as _;
use std::{collections::BTreeMap, panic::AssertUnwindSafe, pin::pin};
use tangram_client as tg;
use tangram_futures::stream::TryStreamExt as _;
use tangram_temp::{directory, Temp};

#[tokio::test]
async fn graph_object() -> tg::Result<()> {
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
		let expected_artifact = directory! { "test.txt" => "hi from a graph" };
		let matches = expected_artifact.matches(actual_temp.path()).await.unwrap();
		assert!(matches);
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
