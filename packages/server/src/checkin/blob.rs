use {
	crate::{
		Server,
		checkin::{Graph, IndexObjectMessages, StoreArgs, graph::Contents},
	},
	futures::{StreamExt as _, TryStreamExt as _, stream},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

const CONCURRENCY: usize = 8;

impl Server {
	pub(super) async fn checkin_create_blobs(
		&self,
		graph: &mut Graph,
		next: usize,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
	) -> tg::Result<()> {
		let nodes = graph
			.nodes
			.range(next..)
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
			.map(|(index, path)| async move {
				let blob = tokio::task::spawn_blocking({
					let path = path.clone();
					move || {
						let file = std::fs::File::open(&path).map_err(
							|source| tg::error!(!source, path = %path.display(), "failed to open the file"),
						)?;
						Self::write_inner_sync(file, None).map_err(
							|source| tg::error!(!source, path = %path.display(), "failed to create the blob"),
						)
					}
				})
				.await
				.map_err(|source| tg::error!(!source, "the blob task panicked"))??;
				Ok::<_, tg::Error>((index, blob))
			})
			.buffer_unordered(CONCURRENCY)
			.try_collect::<Vec<_>>()
			.await?;

		// Convert blobs to store args and index messages.
		let mut entries = Vec::new();
		for (_, output) in &blobs {
			let mut stack = vec![output];
			while let Some(output) = stack.pop() {
				let id: tg::object::Id = output.id.clone().into();
				let metadata = tg::object::Metadata {
					count: Some(output.count),
					depth: Some(output.depth),
					weight: Some(output.weight),
				};
				let size = output.size;
				let bytes = output.bytes.clone();

				// Extract children from the blob data.
				let mut children = BTreeSet::new();
				if let Some(data) = &output.data {
					let data: tg::object::Data = data.clone().into();
					data.children(&mut children);
				}

				// Create store arg.
				let store_arg = crate::store::PutArg {
					bytes,
					cache_reference: None,
					id: id.clone(),
					touched_at,
				};

				// Create index message.
				let index_message = crate::index::message::PutObject {
					cache_entry: None,
					children,
					complete: true,
					id: id.clone(),
					metadata,
					size,
					touched_at,
				};

				entries.push((id, store_arg, index_message));
				stack.extend(&output.children);
			}
		}

		// Add the entries in reverse topological order.
		for (id, store_arg, index_message) in entries.into_iter().rev() {
			store_args.insert(id.clone(), store_arg);
			object_messages.insert(id, index_message);
		}

		// Set the file contents.
		for (index, output) in blobs {
			let file = graph
				.nodes
				.get_mut(&index)
				.unwrap()
				.variant
				.unwrap_file_mut();
			file.contents = Some(Contents::Write(Box::new(output)));
		}

		Ok(())
	}
}
