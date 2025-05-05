use super::{Blob, File, State, Variant, set_permissions_and_times};
use crate::Server;
use bytes::Bytes;
use futures::{TryStreamExt, stream::FuturesUnordered};
use num::ToPrimitive;
use std::{ops::Not, sync::Arc};
use tangram_client as tg;
use tangram_messenger::Messenger as _;

impl Server {
	pub(super) async fn checkin_cache_task(
		&self,
		state: Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		state
			.graph
			.roots
			.iter()
			.map(|(root, nodes)| {
				let server = self.clone();
				let root = *root;
				let nodes = nodes.clone();
				let state = state.clone();
				async move {
					// Copy the root artifact to the cache directory.
					let semaphore = self.file_descriptor_semaphore.acquire().await.unwrap();
					tokio::task::spawn_blocking({
						let server = server.clone();
						let state = state.clone();
						move || server.checkin_copy_or_move_to_cache_directory(&state, root, &nodes)
					})
					.await
					.unwrap()
					.map_err(|source| tg::error!(!source, "failed to copy root object to temp"))?;
					drop(semaphore);

					// Publish a notification that the root is ready to be indexed.
					let id = state.graph.nodes[root]
						.object
						.as_ref()
						.unwrap()
						.id
						.clone()
						.try_into()
						.unwrap();
					let message =
						crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
							id,
							touched_at,
						});
					let message = serde_json::to_vec(&message)
						.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
					let _published = server
						.messenger
						.stream_publish("index".to_owned(), message.into())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	fn checkin_copy_or_move_to_cache_directory(
		&self,
		state: &State,
		root: usize,
		nodes: &[usize],
	) -> tg::Result<()> {
		// Attempt to rename if this is a destructive checkin.
		if state.arg.destructive {
			let id = &state.graph.nodes[root].object.as_ref().unwrap().id;
			let path = state.graph.nodes[root].path();
			let dst = self.cache_path().join(id.to_string());
			match std::fs::rename(path, &dst) {
				Ok(()) => {
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(&dst, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
					)?;
					return Ok(());
				},
				Err(ref error) if error.raw_os_error() == Some(libc::EEXIST | libc::ENOTEMPTY) => {
					return Ok(());
				},
				Err(source) => return Err(tg::error!(!source, "failed to rename root")),
			}
		}

		// If this is a non destructive checkin, only copy the blobs to the cache.
		for node in std::iter::once(root).chain(nodes.iter().copied()) {
			// Skip nodes that don't exist on the system.
			if !state.graph.nodes[node]
				.metadata
				.as_ref()
				.is_some_and(|metadata| metadata.is_file())
			{
				continue;
			}
			let id = state.graph.nodes[node]
				.object
				.as_ref()
				.unwrap()
				.id
				.to_string();
			let src = state.graph.nodes[node].path();
			let dst = self.cache_path().join(id);
			match std::fs::copy(src, &dst) {
				Ok(_) => {
					set_permissions_and_times(
						&dst,
						state.graph.nodes[node].metadata.as_ref().unwrap(),
					)?;
				},
				Err(ref error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
				Err(source) => return Err(tg::error!(!source, "failed to copy the file")),
			}
		}

		Ok(())
	}

	pub(super) async fn checkin_messenger_task(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		if self.messenger.is_left() {
			self.checkin_messenger_memory(state, touched_at).await?;
		} else {
			self.checkin_messenger_nats(state, touched_at).await?;
		}
		Ok(())
	}

	async fn checkin_messenger_memory(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut messages: Vec<Bytes> = Vec::new();
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];
				let object = node.object.as_ref().unwrap();
				let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
					children: object.data.children(),
					id: object.id.clone(),
					size: object.bytes.len().to_u64().unwrap(),
					touched_at,
					cache_reference: None,
				});
				let message = serde_json::to_vec(&message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				messages.push(message.into());
				// Send messages for the blob.
				if let Variant::File(File {
					blob: Some(Blob::Create(blob)),
					..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let children = blob
							.data
							.as_ref()
							.map(tg::blob::Data::children)
							.unwrap_or_default();
						let id = blob.id.clone().into();
						let size = blob.size;
						let cache_reference = state
							.arg
							.destructive
							.then(|| root.clone())
							.unwrap_or_else(|| object.id.clone().try_into().unwrap());
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(cache_reference),
								children,
								id,
								size,
								touched_at,
							});
						let message = serde_json::to_vec(&message).map_err(|source| {
							tg::error!(!source, "failed to serialize the message")
						})?;
						messages.push(message.into());
						stack.extend(&blob.children);
					}
				}
			}
		}
		while !messages.is_empty() {
			let published = self
				.messenger
				.stream_batch_publish("index".to_owned(), messages.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;
			messages = messages.split_off(published.len());
		}
		Ok(())
	}

	async fn checkin_messenger_nats(&self, state: &Arc<State>, touched_at: i64) -> tg::Result<()> {
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];
				let object = node.object.as_ref().unwrap();
				let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
					children: object.data.children(),
					id: object.id.clone(),
					size: object.bytes.len().to_u64().unwrap(),
					touched_at,
					cache_reference: None,
				});
				let message = serde_json::to_vec(&message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				let _published = self
					.messenger
					.stream_publish("index".to_owned(), message.into())
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

				// Send messages for the blob.
				if let Variant::File(File {
					blob: Some(Blob::Create(blob)),
					..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let children = blob
							.data
							.as_ref()
							.map(tg::blob::Data::children)
							.unwrap_or_default();
						let id = blob.id.clone().into();
						let size = blob.size;
						let cache_reference = state
							.arg
							.destructive
							.then(|| root.clone())
							.unwrap_or_else(|| object.id.clone().try_into().unwrap());
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(cache_reference),
								children,
								id,
								size,
								touched_at,
							});
						let message = serde_json::to_vec(&message).map_err(|source| {
							tg::error!(!source, "failed to serialize the message")
						})?;
						let _published = self
							.messenger
							.stream_publish("index".to_owned(), message.into())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})?;
						stack.extend(&blob.children);
					}
				}
			}
		}
		Ok(())
	}

	pub(super) async fn checkin_store_task(
		&self,
		state: &State,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut objects = Vec::with_capacity(state.graph.nodes.len());

		// Add the graph objects.
		for graph in &state.graph_objects {
			objects.push((graph.id.clone().into(), Some(graph.bytes.clone()), None));
			// Add all the graph children, too.
			for (node, data) in graph.data.nodes.iter().enumerate() {
				let (id, bytes) = match data.kind() {
					tg::artifact::Kind::Directory => {
						let data = tg::directory::data::Directory::Graph {
							graph: graph.id.clone(),
							node,
						};
						let bytes = data.serialize()?;
						let id = tg::directory::Id::new(&bytes);
						(id.into(), bytes)
					},
					tg::artifact::Kind::File => {
						let data = tg::file::data::File::Graph {
							graph: graph.id.clone(),
							node,
						};
						let bytes = data.serialize()?;
						let id = tg::file::Id::new(&bytes);
						(id.into(), bytes)
					},
					tg::artifact::Kind::Symlink => {
						let data = tg::symlink::data::Symlink::Graph {
							graph: graph.id.clone(),
							node,
						};
						let bytes = data.serialize()?;
						let id = tg::symlink::Id::new(&bytes);
						(id.into(), bytes)
					},
				};
				objects.push((id, Some(bytes), None));
			}
		}

		// Add the nodes.
		for (root, nodes) in &state.graph.roots {
			let nodes = std::iter::once(*root).chain(nodes.iter().copied());
			let root_path = state.graph.nodes[*root].path();
			let root: tg::artifact::Id = state.graph.nodes[*root]
				.object
				.as_ref()
				.unwrap()
				.id
				.clone()
				.try_into()
				.unwrap();
			for node in nodes {
				let node = &state.graph.nodes[node];

				// Add the object to the list to store.
				let object = node.object.as_ref().unwrap();
				objects.push((object.id.clone(), Some(object.bytes.clone()), None));

				// If the file has a blob, add it too.
				if let Variant::File(File {
					blob: Some(Blob::Create(blob)),
					..
				}) = &node.variant
				{
					let mut stack = vec![blob];
					while let Some(blob) = stack.pop() {
						let (artifact, subpath) = if state.arg.destructive {
							let subpath = node.path().strip_prefix(root_path).unwrap().to_owned();
							let subpath = subpath
								.to_str()
								.unwrap()
								.is_empty()
								.not()
								.then_some(subpath);
							(root.clone(), subpath)
						} else {
							let artifact = object.id.clone().try_into().unwrap();
							(artifact, None)
						};
						let reference = crate::store::CacheReference {
							artifact,
							subpath,
							position: blob.position,
							length: blob.length,
						};
						objects.push((blob.id.clone().into(), blob.bytes.clone(), Some(reference)));
						stack.extend(&blob.children);
					}
				}
			}
		}
		let arg = crate::store::PutBatchArg {
			objects,
			touched_at,
		};
		self.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;

		Ok(())
	}

	pub(super) async fn checkin_create_lockfile_task(&self, state: &State) -> tg::Result<()> {
		// Skip creating a lockfile if this is a destructive checkin, the caller did not request a lockfile, or the root is not a directory.
		if state.arg.destructive
			|| !state.arg.lockfile
			|| !state.graph.nodes[0].metadata.as_ref().unwrap().is_dir()
		{
			return Ok(());
		}

		// Create a lockfile.
		let lockfile = Self::create_lockfile(state)?;

		// Do nothing if the lockfile is empty.
		if lockfile.nodes.is_empty() {
			return Ok(());
		}

		// Serialize the lockfile.
		let contents = serde_json::to_vec_pretty(&lockfile)
			.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

		// Write to disk.
		tokio::fs::write(
			state.graph.nodes[0]
				.path()
				.join(tg::package::LOCKFILE_FILE_NAME),
			contents,
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
		Ok(())
	}
}
