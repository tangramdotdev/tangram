use super::{Blob, File, State, Variant, set_permissions_and_times};
use crate::{Server, temp::Temp};
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
		// Extract the root data.
		let root_id = &state.graph.nodes[root].object.as_ref().unwrap().id;
		let root_path = state.graph.nodes[root].path();
		let root_metadata = state.graph.nodes[root].metadata.as_ref().unwrap();
		let root_dst = self.cache_path().join(root_id.to_string());

		// Attempt to rename if this is a destructive checkin.
		if state.arg.destructive {
			match std::fs::rename(root_path, &root_dst) {
				Ok(()) => {
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(&root_dst, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = root_dst.display(), "failed to set the modified time"),
					)?;
					return Ok(());
				},
				Err(ref error) if error.raw_os_error() == Some(libc::EEXIST | libc::ENOTEMPTY) => {
					return Ok(());
				},
				Err(ref error) if error.raw_os_error() == Some(libc::ENOSYS) => (),
				Err(source) => return Err(tg::error!(!source, "failed to rename root")),
			}
		}

		// Otherwise copy to a temp.
		let temp = Temp::new(self);
		if root_metadata.is_file() {
			// Copy the file.
			std::fs::copy(root_path, temp.path())
				.map_err(|source| tg::error!(!source, "failed to copy file"))?;

			// Update permissions.
			set_permissions_and_times(temp.path(), root_metadata)?;
		} else if root_metadata.is_symlink() {
			// Get the target.
			let target = &state.graph.nodes[root].variant.unwrap_symlink_ref().target;

			// Create the symlink.
			std::os::unix::fs::symlink(target, temp.path())
				.map_err(|source| tg::error!(!source, "failed to create link"))?;
		} else {
			// Walk the subgraph.
			std::fs::create_dir_all(temp.path())
				.map_err(|source| tg::error!(!source, "failed to create temp directory"))?;

			// Copy everything except the root.
			for node in nodes {
				let node = &state.graph.nodes[*node];
				let metadata = node.metadata.as_ref().unwrap();
				let src = node.path();
				let dst = temp.path().join(src.strip_prefix(root_path).unwrap());
				if metadata.is_dir() {
					std::fs::create_dir_all(&dst).map_err(
						|source| tg::error!(!source, %path = dst.display(), "failed to create directory"),
					)?;
				} else if metadata.is_file() {
					std::fs::create_dir_all(dst.parent().unwrap()).map_err(
						|source| tg::error!(!source, %path = dst.display(), "failed to create parent directory"),
					)?;
					std::fs::copy(src, &dst).map_err(
						|source| tg::error!(!source, %path = src.display(), "failed to copy file"),
					)?;
				} else if metadata.is_symlink() {
					let target = &node.variant.unwrap_symlink_ref().target;
					std::os::unix::fs::symlink(target, &dst)
						.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
				} else {
					unreachable!("well, fuck")
				}

				// Update permissions.
				set_permissions_and_times(&dst, metadata)?;
			}
		}

		// Rename the temp to the cache.
		match std::fs::rename(temp.path(), &root_dst) {
			Ok(()) => (),
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::DirectoryNotEmpty,
				) => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to rename the path to the cache directory"
				));
			},
		}

		// Set the file times to the epoch.
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(&root_dst, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = root_dst.display(), "failed to set the modified time"),
		)?;

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
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(root.clone()),
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
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: Some(root.clone()),
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
						let subpath = node.path().strip_prefix(root_path).unwrap().to_owned();
						let subpath = subpath
							.to_str()
							.unwrap()
							.is_empty()
							.not()
							.then_some(subpath);
						let reference = crate::store::CacheReference {
							artifact: root.clone(),
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
