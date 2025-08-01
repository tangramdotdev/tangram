use super::{Blob, File, State, Variant};
use crate::{Server, temp::Temp};
use bytes::Bytes;
use num::ToPrimitive as _;
use std::{ops::Not, os::unix::fs::PermissionsExt as _, sync::Arc};
use tangram_client as tg;
use tangram_messenger::Messenger as _;

impl Server {
	pub(super) async fn checkin_cache_task(
		&self,
		state: Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		for (root, nodes) in &state.graph.roots {
			let server = self.clone();
			let state = state.clone();
			let root = *root;
			let nodes = nodes.clone();
			if state.arg.destructive {
				server
					.checkin_cache_task_destructive(state, touched_at, root)
					.await?;
			} else {
				tokio::task::spawn_blocking({
					let server = server.clone();
					let state = state.clone();
					move || server.checkin_cache_task_inner(&state, root, &nodes)
				})
				.await
				.unwrap()
				.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;
			}
		}
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
		let src = state.graph.nodes[root].path();
		let dst = &self.cache_path().join(id.to_string());
		if id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o755);
			std::fs::set_permissions(src, permissions).map_err(
				|source| tg::error!(!source, %path = src.display(), "failed to set permissions"),
			)?;
		}
		let done = match tokio::fs::rename(src, dst).await {
			Ok(()) => false,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists
						| std::io::ErrorKind::DirectoryNotEmpty
						| std::io::ErrorKind::PermissionDenied
				) =>
			{
				true
			},
			Err(source) => {
				return Err(tg::error!(!source, "failed to rename the root"));
			},
		};
		if !done && id.is_directory() {
			let permissions = std::fs::Permissions::from_mode(0o555);
			tokio::fs::set_permissions(dst, permissions).await.map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
			)?;
		}
		if !done {
			let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
			filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
			)?;
		}

		// Publish the cache entry message for the root.
		let id = id.clone().try_into().unwrap();
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
			let node = &state.graph.nodes[node];
			let Some(metadata) = node.metadata.as_ref() else {
				continue;
			};
			if !metadata.is_file() {
				continue;
			}
			let id = node.object.as_ref().unwrap().id.to_string();

			// Copy the file to a temp.
			let src = node.path();
			let temp = Temp::new(self);
			let dst = temp.path();
			std::fs::copy(src, dst)
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;

			// Set its permissions.
			let executable = metadata.permissions().mode() & 0o111 != 0;
			let mode = if executable { 0o555 } else { 0o444 };
			let permissions = std::fs::Permissions::from_mode(mode);
			std::fs::set_permissions(dst, permissions).map_err(
				|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
			)?;

			// Rename the temp to the cache directory.
			let src = temp.path();
			let dst = &self.cache_path().join(id);
			let done = match std::fs::rename(src, dst) {
				Ok(()) => false,
				Err(source) => {
					return Err(tg::error!(!source, "failed to rename the file"));
				},
			};

			// Set the file times.
			if !done {
				let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
				filetime::set_symlink_file_times(dst, epoch, epoch).map_err(
					|source| tg::error!(!source, %path = dst.display(), "failed to set the modified time"),
				)?;
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
					cache_reference: None,
					children: object.data.as_ref().unwrap().children().collect(),
					complete: false,
					count: None,
					depth: None,
					id: object.id.clone(),
					size: object.bytes.as_ref().unwrap().len().to_u64().unwrap(),
					touched_at,
					weight: None,
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
								complete: false,
								count: None,
								depth: None,
								id,
								size,
								touched_at,
								weight: None,
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
				complete: false,
				count: None,
				depth: None,
				id: graph.id.clone().into(),
				size: graph.bytes.len().to_u64().unwrap(),
				touched_at,
				weight: None,
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
						let data =
							tg::directory::data::Directory::Reference(tg::graph::data::Reference {
								graph: Some(graph.id.clone()),
								node,
							});
						let bytes = data.serialize()?;
						let id = tg::directory::Id::new(&bytes);
						(id.into(), bytes)
					},
					tg::artifact::Kind::File => {
						let data = tg::file::data::File::Reference(tg::graph::data::Reference {
							graph: Some(graph.id.clone()),
							node,
						});
						let bytes = data.serialize()?;
						let id = tg::file::Id::new(&bytes);
						(id.into(), bytes)
					},
					tg::artifact::Kind::Symlink => {
						let data =
							tg::symlink::data::Symlink::Reference(tg::graph::data::Reference {
								graph: Some(graph.id.clone()),
								node,
							});
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
						let (artifact, path) = if state.arg.destructive {
							let path = node.path().strip_prefix(root_path).unwrap().to_owned();
							let path = path.to_str().unwrap().is_empty().not().then_some(path);
							(root.clone(), path)
						} else {
							let artifact = object.id.clone().try_into().unwrap();
							(artifact, None)
						};
						let reference = crate::store::CacheReference {
							artifact,
							path,
							position: blob.position,
							length: blob.length,
						};
						objects.push((blob.id.clone().into(), blob.bytes.clone(), Some(reference)));
						stack.extend(&blob.children);
					}
				}
			}
		}

		// Store the objects.
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

	pub(super) async fn checkin_create_lock_task(&self, state: &State) -> tg::Result<()> {
		let lockfile_path = state.graph.nodes[0]
			.path()
			.join(tg::package::LOCKFILE_FILE_NAME);

		// Skip creating a lock if this is a destructive checkin, the caller did not request a lock, or the root is not a directory.
		if state.arg.destructive
			|| !state.arg.lock
			|| !state.graph.nodes[0].metadata.as_ref().unwrap().is_dir()
		{
			return Ok(());
		}

		// Create a lock.
		let lock = Self::checkin_create_lock(state)?;

		// If this is a locked checkin, then verify the lock is unchanged.
		if state.arg.locked
			&& state
				.lock
				.as_ref()
				.is_some_and(|existing| existing.nodes != lock.nodes)
		{
			return Err(tg::error!("the lock is out of date"));
		}

		// Do nothing if the lock is empty.
		if lock.nodes.is_empty() {
			crate::util::fs::remove(&lockfile_path).await.ok();
			return Ok(());
		}

		// Serialize the lock.
		let contents = serde_json::to_vec_pretty(&lock)
			.map_err(|source| tg::error!(!source, "failed to serialize lock"))?;

		// Write to disk.
		tokio::fs::write(&lockfile_path, contents)
			.await
			.map_err(|source| tg::error!(!source, "failed to write lock"))?;

		Ok(())
	}
}
