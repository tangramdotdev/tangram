use {
	super::state::State,
	crate::Server,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	tangram_client as tg,
	tangram_either::Either,
};

const CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let server = self.clone();
		let nodes = state
			.graph
			.nodes
			.iter()
			.filter_map(|(node_id, node)| {
				let is_file = node.variant.is_file();
				if !is_file {
					return None;
				}
				if !node.dirty {
					return None;
				}
				let path = node.path.clone()?;
				if !state.graph.paths.contains_key(&path) {
					return None;
				}
				Some((*node_id, path))
			})
			.collect::<Vec<_>>();
		let blobs = stream::iter(nodes)
			.map(|(node_id, path)| {
				let server = server.clone();
				async move {
					let mut file = tokio::fs::File::open(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
					)?;
					let blob = server.write_inner(&mut file, None).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
					)?;
					Ok::<_, tg::Error>((node_id, blob))
				}
			})
			.buffer_unordered(CONCURRENCY)
			.try_collect::<Vec<_>>()
			.await?;
		for (node_id, blob) in blobs {
			state.blobs.insert(blob.id.clone(), blob.clone());
			state
				.graph
				.nodes
				.get_mut(&node_id)
				.unwrap()
				.variant
				.unwrap_file_mut()
				.contents
				.replace(Either::Left(blob));
		}
		Ok(())
	}
}
