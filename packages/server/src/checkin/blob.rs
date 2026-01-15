use {
	crate::{
		Server,
		checkin::{Graph, IndexObjectMessages, StoreArgs, graph::Contents},
		write::Destination,
	},
	futures::{StreamExt as _, TryStreamExt as _, stream},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl Server {
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn checkin_create_blobs(
		&self,
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		next: usize,
		store_args: &mut StoreArgs,
		object_messages: &mut IndexObjectMessages,
		touched_at: i64,
		progress: &crate::progress::Handle<super::TaskOutput>,
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
				let size = node.path_metadata.as_ref().map(std::fs::Metadata::len);
				Some((*index, path, size))
			})
			.collect::<Vec<_>>();

		progress.spinner("hashing", "hashing");
		let total = nodes.iter().filter_map(|(_, _, size)| *size).sum::<u64>();
		progress.start(
			"bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			Some(total),
		);

		let cache_pointers = arg.options.cache_pointers;
		let blobs = stream::iter(nodes)
			.map(|(index, path, size)| {
				let progress = progress.clone();
				let server = self.clone();
				async move {
					let blob = tokio::task::spawn_blocking({
						let path = path.clone();
						move || {
							let file = std::fs::File::open(&path).map_err(
								|source| tg::error!(!source, path = %path.display(), "failed to open the file"),
							)?;
							let destination = if cache_pointers {
								None
							} else {
								Some(Destination::Store { touched_at })
							};
							server.write_inner_sync(file, destination.as_ref()).map_err(
								|source| tg::error!(!source, path = %path.display(), "failed to create the blob"),
							)
						}
					})
					.await
					.map_err(|source| tg::error!(!source, "the blob task panicked"))??;
					if let Some(size) = size {
						progress.increment("bytes", size);
					}
					Ok::<_, tg::Error>((index, blob))
				}
			})
			.buffer_unordered(self.config.checkin.blob.concurrency)
			.try_collect::<Vec<_>>()
			.await?;

		progress.finish("hashing");
		progress.finish("bytes");

		// Convert blobs to store args and index messages.
		let mut entries = Vec::new();
		for (_, output) in &blobs {
			let mut stack = vec![output];
			while let Some(output) = stack.pop() {
				let id: tg::object::Id = output.id.clone().into();
				let metadata = output.metadata.clone();
				let bytes = output.bytes.clone();

				// Extract children from the blob data.
				let mut children = BTreeSet::new();
				if let Some(data) = &output.data {
					let data: tg::object::Data = data.clone().into();
					data.children(&mut children);
				}

				// Create the store arg only if needed.
				let store_arg = if cache_pointers || bytes.is_some() {
					Some(crate::store::PutObjectArg {
						bytes,
						cache_pointer: None,
						id: id.clone(),
						touched_at,
					})
				} else {
					None
				};

				// Create the index message.
				let index_message = crate::index::message::PutObject {
					cache_entry: None,
					children,
					id: id.clone(),
					metadata,
					stored: crate::index::ObjectStored { subtree: true },
					touched_at,
				};

				entries.push((id, store_arg, index_message));

				stack.extend(&output.children);
			}
		}

		// Add the entries in reverse topological order.
		for (id, store_arg, index_message) in entries.into_iter().rev() {
			if let Some(store_arg) = store_arg {
				store_args.insert(id.clone(), store_arg);
			}
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
