use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_server_core::{Server, context::Origin},
};

impl Component {
	pub(crate) async fn initialize_checkouts(&self, server: &Server) -> tg::Result<()> {
		let checkout_guard = self.lock.acquire().await;
		let store_path = server.store_path();
		let checkout_path = server.path.join("checkouts");
		let vfs_kind = match server.config.vfs.clone().unwrap_or_default().kind {
			tangram_server_core::config::VfsKind::Auto => {
				if cfg!(target_os = "macos")
					&& std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some()
				{
					tangram_server_core::vfs::Kind::Fskit
				} else if cfg!(target_os = "macos") {
					tangram_server_core::vfs::Kind::Nfs
				} else if cfg!(target_os = "linux") {
					tangram_server_core::vfs::Kind::Fuse
				} else {
					unreachable!()
				}
			},
			tangram_server_core::config::VfsKind::Fskit => tangram_server_core::vfs::Kind::Fskit,
			tangram_server_core::config::VfsKind::Fuse => tangram_server_core::vfs::Kind::Fuse,
			tangram_server_core::config::VfsKind::Nfs => tangram_server_core::vfs::Kind::Nfs,
		};
		let store_exists = match tokio::fs::try_exists(&store_path).await {
			Ok(exists) => exists,
			Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {
				server.components.vfs.unmount(vfs_kind, &store_path).await?;
				true
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to stat the path"));
			},
		};
		let checkout_exists = tokio::fs::try_exists(&checkout_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to stat the path"))?;
		if let Some(options) = server.config.vfs.clone() {
			if store_exists && !checkout_exists {
				tokio::fs::rename(&store_path, &checkout_path)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to move the store directory to the checkouts path"
						)
					})?;
			}
			tokio::fs::create_dir_all(&store_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the store directory"))?;
			tokio::fs::create_dir_all(&checkout_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the checkouts directory"))?;
			let arg = tangram_server_core::vfs::MountArg {
				kind: vfs_kind,
				options,
				origin: Origin::Host,
				path: &store_path,
				principal: Some(tg::Principal::Root),
				recvfd: None,
			};
			server
				.components
				.vfs
				.mount_store(server, arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to start the VFS"))?;
		} else {
			if checkout_exists {
				tokio::fs::rename(&checkout_path, &store_path)
					.await
					.map_err(|error| {
						tg::error!(
							!error,
							"failed to move the checkouts directory to the store path"
						)
					})?;

				// Remove named checkout entries before exposing the physical store.
				self.remove_all_named_checkout_entries_with_lock(server, &checkout_guard)
					.await?;
			}
			tokio::fs::create_dir_all(&store_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the store directory"))?;
		}
		drop(checkout_guard);

		Ok(())
	}
}
