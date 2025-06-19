use super::{Blob, File, State, Variant};
use crate::{Server, temp::Temp};
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
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
				let state = state.clone();
				let root = *root;
				let nodes = nodes.clone();
				async move {
					if state.arg.destructive {
						server
							.checkin_cache_task_destructive(state, touched_at, root)
							.await?;
					} else {
						let semaphore = self.file_descriptor_semaphore.acquire().await.unwrap();
						tokio::task::spawn_blocking({
							let server = server.clone();
							let state = state.clone();
							move || server.checkin_cache_task_inner(&state, root, &nodes)
						})
						.await
						.unwrap()
						.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;
						drop(semaphore);
					}
					Ok::<_, tg::Error>(())
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	pub(super) async fn checkin_cache_task_destructive(
		&self,
		state: Arc<State>,
		touched_at: i64,
		root: usize,
	) -> tg::Result<()> {
		// Rename the root to the cache directory.
		let id = &state.graph.nodes[root].object.as_ref().unwrap().id;
		let path = state.graph.nodes[root].path();
		let dst = self.cache_path().join(id.to_string());
		match tokio::fs::rename(path, &dst).await {
			Ok(()) => {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(&dst, epoch, epoch).map_err(
					|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
				)?;
			},
			Err(error) if matches!(error.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => {
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to rename root"));
			},
		}

		// Publish the cache entry message for the root.
		let id = state.graph.nodes[root]
			.object
			.as_ref()
			.unwrap()
			.id
			.clone()
			.try_into()
			.unwrap();
		let message = crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
			id,
			touched_at,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	fn checkin_cache_task_inner(
		&self,
		state: &State,
		root: usize,
		nodes: &[usize],
	) -> tg::Result<()> {
		for node in std::iter::once(root).chain(nodes.iter().copied()) {
			if !state.graph.nodes[node]
				.metadata
				.as_ref()
				.is_some_and(std::fs::Metadata::is_file)
			{
				continue;
			}
			let id = state.graph.nodes[node]
				.object
				.as_ref()
				.unwrap()
				.id
				.to_string();

			// Copy the file to a temp.
			let src = state.graph.nodes[node].path();
			let temp = Temp::new(self);
			let dst = temp.path();
			std::fs::copy(src, dst)
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;

			// Rename the temp to the cache directory.
			let src = temp.path();
			let dst = &self.cache_path().join(id);
			match std::fs::rename(src, dst) {
				Ok(()) => {
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
					)?;
				},
				Err(error)
					if matches!(error.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) => {},
				Err(source) => {
					return Err(tg::error!(!source, "failed to rename file"));
				},
			}
		}
		Ok(())
	}

	pub(super) async fn checkin_messenger_task(
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
				if object.data.is_none() || object.bytes.is_none() {
					continue;
				}
				let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
					children: object.data.as_ref().unwrap().children().collect(),
					id: object.id.clone(),
					size: object.bytes.as_ref().unwrap().len().to_u64().unwrap(),
					touched_at,
					cache_reference: None,
				});
				let message = serde_json::to_vec(&message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				messages.push(message.into());
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
							.map(|data| data.children().collect())
							.unwrap_or_default();
						let id = blob.id.clone().into();
						let size = blob.size;
						let cache_reference = if state.arg.destructive {
							root.clone()
						} else {
							object.id.clone().try_into().unwrap()
						};
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

		for graph in &state.graph_objects {
			let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
				cache_reference: None,
				children: graph.data.children().collect(),
				id: graph.id.clone().into(),
				size: graph.bytes.len().to_u64().unwrap(),
				touched_at,
			});
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			messages.push(message.into());
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
			let root_path = state.graph.nodes[*root].path.as_deref();
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
				if object.bytes.is_none() {
					continue;
				}
				objects.push((
					object.id.clone(),
					Some(object.bytes.as_ref().unwrap().clone()),
					None,
				));

				// If the file has a blob and it's on disk, add it too.
				let Some(root_path) = root_path else {
					continue;
				};
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
		let lockfile_path = state.graph.nodes[0]
			.path()
			.join(tg::package::LOCKFILE_FILE_NAME);

		// Skip creating a lockfile if this is a destructive checkin, the caller did not request a lockfile, or the root is not a directory.
		if state.arg.destructive
			|| !state.arg.lockfile
			|| !state.graph.nodes[0].metadata.as_ref().unwrap().is_dir()
		{
			return Ok(());
		}

		// Create a lockfile.
		let lockfile = Self::create_lockfile(state)?;

		// If this is a locked checkin, then verify the lockfile is unchanged.
		if state.arg.locked
			&& state
				.lockfile
				.as_ref()
				.is_some_and(|existing| existing.nodes != lockfile.nodes)
		{
			return Err(tg::error!("the lockfile is out of date"));
		}

		// Do nothing if the lockfile is empty.
		if lockfile.nodes.is_empty() {
			crate::util::fs::remove(&lockfile_path).await.ok();
			return Ok(());
		}

		// Serialize the lockfile.
		let contents = serde_json::to_vec_pretty(&lockfile)
			.map_err(|source| tg::error!(!source, "failed to serialize lockfile"))?;

		// Write to disk.
		tokio::fs::write(&lockfile_path, contents)
			.await
			.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;

		Ok(())
	}
}
