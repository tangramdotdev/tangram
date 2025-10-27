use {
	super::state::{CacheReferenceRange, Object},
	crate::{
		Server,
		checkin::{Graph, state::Objects},
	},
	futures::{StreamExt as _, TryStreamExt as _, stream},
	tangram_client as tg,
};

const CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn checkin_create_blobs(
		&self,
		graph: &mut Graph,
		objects_: &mut Objects,
	) -> tg::Result<()> {
		let server = self.clone();
		let nodes = graph
			.nodes
			.iter()
			.filter_map(|(index, node)| {
				let is_file = node.variant.is_file();
				if !is_file {
					return None;
				}
				let path = node.path.clone()?;
				Some((*index, path))
			})
			.collect::<Vec<_>>();
		let blobs = stream::iter(nodes)
			.map(|(index, path)| {
				let server = server.clone();
				async move {
					let mut file = tokio::fs::File::open(&path).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
					)?;
					let blob = server.write_inner(&mut file, None).await.map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
					)?;
					Ok::<_, tg::Error>((index, blob))
				}
			})
			.buffer_unordered(CONCURRENCY)
			.try_collect::<Vec<_>>()
			.await?;

		// Convert blobs to objects and collect them.
		let mut objects = Vec::new();
		for (_, blob) in &blobs {
			let mut stack = vec![blob];
			while let Some(blob) = stack.pop() {
				let metadata = tg::object::Metadata {
					count: Some(blob.count),
					depth: Some(blob.depth),
					weight: Some(blob.weight),
				};
				let cache_reference_range = CacheReferenceRange {
					position: blob.position,
					length: blob.length,
				};
				let object = Object {
					bytes: blob.bytes.clone(),
					cache_reference: None,
					cache_reference_range: Some(cache_reference_range),
					complete: true,
					data: blob.data.clone().map(Into::into),
					id: blob.id.clone().into(),
					metadata: Some(metadata),
					size: blob.size,
				};
				objects.push((object.id.clone(), object));
				stack.extend(&blob.children);
			}
		}

		// Add the objects in reverse topological order.
		objects_.extend(objects.into_iter().rev());

		// Update file node contents to reference the blob IDs.
		for (index, blob) in blobs {
			let metadata = tg::object::Metadata {
				count: Some(blob.count),
				depth: Some(blob.depth),
				weight: Some(blob.weight),
			};
			let file = graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_file_mut();
			file.contents_metadata = Some(metadata);
			file.contents = Some(blob.id);
		}

		Ok(())
	}
}
