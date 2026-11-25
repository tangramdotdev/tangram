use {
	crate::{
		Component, {CacheArgs, Contents, Graph, IndexObjectArgs},
	},
	futures::{StreamExt as _, TryStreamExt as _, stream},
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_server_core::{Session, temp::Temp, write::Destination},
};

/// The temps that hold the copies of the files that were hashed, by node index.
pub(super) type Temps = BTreeMap<usize, Temp>;

pub(super) struct CheckinCreateBlobsArg<'a> {
	pub arg: &'a tg::checkin::Arg,
	pub graph: &'a mut Graph,
	pub next: usize,
	pub cache_args: &'a mut CacheArgs,
	pub index_object_args: &'a mut IndexObjectArgs,
	pub touched_at: i64,
	pub progress: &'a tg::progress::Handle<super::TaskOutput>,
}

impl Component {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_create_blobs(
		&self,
		session: &Session,
		arg: CheckinCreateBlobsArg<'_>,
	) -> tg::Result<Temps> {
		let CheckinCreateBlobsArg {
			arg,
			graph,
			next,
			cache_args,
			index_object_args,
			touched_at,
			progress,
		} = arg;
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

		let checkout_pointers = arg.options.checkout_pointers;
		let copy = checkout_pointers && !arg.options.destructive;
		let outputs = stream::iter(nodes)
			.map(|(index, path, _)| {
				let progress = progress.clone();
				let session = session.clone();
				async move {
					let (blob, temp) = tokio::task::spawn_blocking({
						let path = path.clone();
						move || {
							// Copy the file to a temp, so that the bytes that are hashed are the bytes that are checked out.
							let temp = copy.then(|| Temp::new(&session.server));
							if let Some(temp) = &temp {
								reflink_copy::reflink_or_copy(&path, temp.path()).map_err(
									|error| tg::error!(!error, path = %path.display(), "failed to copy the file"),
								)?;
							}

							// Hash the file.
							let path = temp.as_ref().map_or(path.as_path(), Temp::path);
							let mut file = std::fs::File::open(path).map_err(
								|error| tg::error!(!error, path = %path.display(), "failed to open the file"),
							)?;
							let destination = if checkout_pointers {
								None
							} else {
								Some(Destination::Cache)
							};
							let blob = session
								.server
								.components
								.write
								.write_inner_sync(
									&session,
									&mut file,
									destination.as_ref(),
									&progress,
								)
								.map_err(
									|error| tg::error!(!error, path = %path.display(), "failed to create the blob"),
								)?;

							Ok::<_, tg::Error>((blob, temp))
						}
					})
					.await
					.map_err(|error| tg::error!(!error, "the blob task panicked"))??;
					Ok::<_, tg::Error>((index, blob, temp))
				}
			})
			.buffer_unordered(session.server.config.checkin.blob.concurrency)
			.try_collect::<Vec<_>>()
			.await?;
		let mut blobs = Vec::with_capacity(outputs.len());
		let mut temps = Temps::new();
		for (index, blob, temp) in outputs {
			blobs.push((index, blob));
			if let Some(temp) = temp {
				temps.insert(index, temp);
			}
		}

		progress.finish("hashing");
		progress.finish("bytes");

		// Convert blobs to cache args and index messages.
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

				// Create the cache arg only if needed.
				let cache_arg = if checkout_pointers || bytes.is_some() {
					Some(tangram_cache::object::put::Arg {
						bytes,
						checkout_pointer: None,
						id: id.clone(),
						length: Some(output.length),
						put: output.put,
					})
				} else {
					None
				};

				// Create the index message.
				let index_message = tangram_index::object::put::Arg {
					account: None,
					checkout: None,
					children,
					id: id.clone(),
					metadata,
					put: output.put,
					storage: tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
					time_to_touch: session.server.config.object.time_to_touch,
					touched_at,
				};

				entries.push((id, cache_arg, index_message));

				stack.extend(&output.children);
			}
		}

		// Add the entries in reverse topological order.
		for (id, cache_arg, index_message) in entries.into_iter().rev() {
			if let Some(cache_arg) = cache_arg {
				cache_args.insert(id.clone(), cache_arg);
			}
			index_object_args.insert(id, index_message);
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

		Ok(temps)
	}
}
