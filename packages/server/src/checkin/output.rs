use {
	super::state::{State, Variant},
	crate::{Server, temp::Temp},
	bytes::Bytes,
	std::{collections::BTreeSet, os::unix::fs::PermissionsExt as _, sync::Arc},
	tangram_client as tg,
	tangram_either::Either,
	tangram_messenger::Messenger as _,
	tangram_store::prelude::*,
};

impl Server {
	pub(super) async fn checkin_cache(&self, state: Arc<State>) -> tg::Result<()> {
		if state.arg.options.destructive {
			self.checkin_cache_task_destructive(state).await?;
		} else {
			tokio::task::spawn_blocking({
				let server = self.clone();
				let state = state.clone();
				move || server.checkin_cache_task_inner(&state)
			})
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, "the checkin cache task failed"))?;
		}
		Ok(())
	}

	pub(super) async fn checkin_cache_task_destructive(&self, state: Arc<State>) -> tg::Result<()> {
		let node = &state.graph.nodes[0];
		// Only process if the root node is dirty.
		if !node.dirty {
			return Ok(());
		}
		let id = node.object_id.as_ref().unwrap();
		let src = node.path.as_ref().unwrap();
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
		Ok(())
	}

	fn checkin_cache_task_inner(&self, state: &State) -> tg::Result<()> {
		for node in &state.graph.nodes {
			// Only process dirty nodes.
			if !node.dirty {
				continue;
			}
			let Some(path) = node.path.as_ref() else {
				continue;
			};
			// Skip nodes whose paths were deleted.
			if !state.graph.paths.contains_key(path) {
				continue;
			}
			let metadata = node.path_metadata.as_ref().unwrap();
			if !metadata.is_file() {
				continue;
			}
			let id = node.object_id.as_ref().unwrap();

			// Copy the file to a temp.
			let src = path;
			let temp = Temp::new(self);
			let dst = temp.path();
			std::fs::copy(src, dst)
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;

			// Set its permissions.
			if !metadata.is_symlink() {
				let executable = metadata.permissions().mode() & 0o111 != 0;
				let mode = if executable { 0o555 } else { 0o444 };
				let permissions = std::fs::Permissions::from_mode(mode);
				std::fs::set_permissions(dst, permissions).map_err(
					|source| tg::error!(!source, %path = dst.display(), "failed to set permissions"),
				)?;
			}

			// Rename the temp to the cache directory.
			let src = temp.path();
			let dst = &self.cache_path().join(id.to_string());
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

	pub(super) async fn checkin_store(&self, state: &State, touched_at: i64) -> tg::Result<()> {
		// Collect object IDs and graph IDs from dirty nodes.
		let mut dirty_object_ids = std::collections::HashSet::new();
		for node in &state.graph.nodes {
			if node.dirty {
				if let Some(object_id) = &node.object_id {
					dirty_object_ids.insert(object_id.clone());
					// Also include graph IDs from dirty reference artifacts.
					if let Some(object) = state.objects.as_ref().unwrap().get(object_id) {
						if let Some(data) = &object.data {
							if let Some(graph_id) = match data {
								tg::object::Data::Directory(tg::directory::Data::Reference(reference))
								| tg::object::Data::File(tg::file::Data::Reference(reference))
								| tg::object::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
									reference.graph.as_ref()
								},
								_ => None,
							} {
								dirty_object_ids.insert(graph_id.clone().into());
							}
						}
					}
				}
				// Also include blob IDs from dirty file nodes.
				if let Variant::File(file) = &node.variant {
					if let Some(Either::Left(blob)) = &file.contents {
						dirty_object_ids.insert(blob.id.clone().into());
					}
				}
			}
		}

		// Only store objects that correspond to dirty nodes or dirty graphs.
		let args: Vec<_> = state
			.objects
			.as_ref()
			.unwrap()
			.iter()
			.filter(|(id, _)| dirty_object_ids.contains(id))
			.map(|(_, object)| crate::store::PutArg {
				bytes: object.bytes.clone(),
				cache_reference: object.cache_reference.clone(),
				id: object.id.clone(),
				touched_at,
			})
			.collect();
		self.store
			.put_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}

	pub(super) async fn checkin_index(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		// Collect object IDs and graph IDs from dirty nodes.
		let mut dirty_object_ids = std::collections::HashSet::new();
		for node in &state.graph.nodes {
			if node.dirty {
				if let Some(object_id) = &node.object_id {
					dirty_object_ids.insert(object_id.clone());
					// Also include graph IDs from dirty reference artifacts.
					if let Some(object) = state.objects.as_ref().unwrap().get(object_id) {
						if let Some(data) = &object.data {
							if let Some(graph_id) = match data {
								tg::object::Data::Directory(tg::directory::Data::Reference(reference))
								| tg::object::Data::File(tg::file::Data::Reference(reference))
								| tg::object::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
									reference.graph.as_ref()
								},
								_ => None,
							} {
								dirty_object_ids.insert(graph_id.clone().into());
							}
						}
					}
				}
				// Also include blob IDs from dirty file nodes.
				if let Variant::File(file) = &node.variant {
					if let Some(Either::Left(blob)) = &file.contents {
						dirty_object_ids.insert(blob.id.clone().into());
					}
				}
			}
		}

		let mut messages: Vec<Bytes> = Vec::new();

		// Create put cache entry messages.
		if state.arg.options.destructive {
			// For destructive mode, only index if the root node is dirty.
			if state.graph.nodes[0].dirty {
				let id = state.graph.nodes[0]
					.object_id
					.as_ref()
					.unwrap()
					.clone()
					.try_into()
					.unwrap();
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id,
						touched_at,
					});
				let message = message.serialize()?;
				let _published = self
					.messenger
					.stream_publish("index".to_owned(), message)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
			}
		} else {
			for node in &state.graph.nodes {
				// Only process dirty file nodes.
				if !node.dirty {
					continue;
				}
				let Variant::File(file) = &node.variant else {
					continue;
				};
				let Some(Either::Left(_)) = &file.contents else {
					continue;
				};
				let message =
					crate::index::Message::PutCacheEntry(crate::index::message::PutCacheEntry {
						id: node.object_id.as_ref().unwrap().clone().try_into().unwrap(),
						touched_at,
					});
				let message = message.serialize()?;
				messages.push(message);
			}
		}

		// Create put object messages only for objects from dirty nodes or dirty graphs.
		for object in state.objects.as_ref().unwrap().values() {
			if !dirty_object_ids.contains(&object.id) {
				continue;
			}
			let cache_entry = object
				.cache_reference
				.as_ref()
				.map(|cache_reference| cache_reference.artifact.clone());
			let mut children = BTreeSet::new();
			if let Some(data) = &object.data {
				data.children(&mut children);
			}
			let complete = object.complete;
			let metadata = object.metadata.clone().unwrap_or_default();
			let message = crate::index::Message::PutObject(crate::index::message::PutObject {
				cache_entry,
				children,
				complete,
				id: object.id.clone(),
				metadata,
				size: object.size,
				touched_at,
			});
			let message = message.serialize()?;
			messages.push(message);
		}

		self.messenger
			.stream_batch_publish("index".to_owned(), messages.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the messages"))?;

		Ok(())
	}
}
