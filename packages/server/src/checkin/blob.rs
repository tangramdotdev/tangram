use super::state::State;
use crate::Server;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(super) async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let server = self.clone();
		let nodes = state
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				let is_file = node.variant.is_file();
				if !is_file {
					return None;
				}
				let path = node.path.clone()?;
				Some((index, path))
			})
			.collect::<Vec<_>>();
		let blobs = stream::iter(nodes)
			.map(|(index, path)| {
				let server = server.clone();
				async move {
					let mut file = tokio::fs::File::open(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
					)?;
					let blob = server.create_blob_inner(&mut file, None).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
					)?;
					Ok::<_, tg::Error>((index, blob))
				}
			})
			.buffer_unordered(8)
			.try_collect::<Vec<_>>()
			.await?;
		for (index, blob) in blobs {
			state.blobs.insert(blob.id.clone(), blob.clone());
			state.graph.nodes[index]
				.variant
				.unwrap_file_mut()
				.contents
				.replace(Either::Left(blob));
		}
		Ok(())
	}
}
