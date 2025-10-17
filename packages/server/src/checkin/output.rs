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
			let Some(path) = node.path.as_ref() else {
				continue;
			};
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
		let args = state
			.objects
			.as_ref()
			.unwrap()
			.values()
			.map(|object| crate::store::PutArg {
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
		let mut messages: Vec<Bytes> = Vec::new();

		// Create put cache entry messages.
		if state.arg.options.destructive {
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
		} else {
			for node in &state.graph.nodes {
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

		// Create put object messages.
		for object in state.objects.as_ref().unwrap().values() {
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
