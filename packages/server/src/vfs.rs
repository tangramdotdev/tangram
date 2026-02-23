use {provider::Provider, std::path::Path, tangram_client::prelude::*, tangram_vfs as vfs};

mod provider;

#[derive(Clone, Debug, Copy)]
pub enum Kind {
	Fuse,
	Nfs,
}

#[derive(Clone)]
pub enum Server {
	Fuse(vfs::fuse::Server<Provider>),
	Nfs(vfs::nfs::Server<Provider>),
}

impl Server {
	pub async fn start(
		server: &crate::Server,
		kind: Kind,
		path: &Path,
		options: crate::config::Vfs,
	) -> tg::Result<Self> {
		// Remove a file at the path if one exists.
		tokio::fs::remove_file(path).await.ok();

		// Create a directory at the path if necessary.
		tokio::fs::create_dir_all(path).await.ok();

		// Create the provider.
		let provider = Provider::new(server, options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the vfs provider"))?;

		let vfs = match kind {
			Kind::Fuse => {
				let options = vfs::fuse::Options {
					io: match options.io {
						crate::config::VfsIo::Auto => vfs::fuse::Io::Auto,
						crate::config::VfsIo::IoUring => vfs::fuse::Io::IoUring,
						crate::config::VfsIo::ReadWrite => vfs::fuse::Io::ReadWrite,
					},
					passthrough: match options.passthrough {
						crate::config::VfsPassthrough::Auto => vfs::fuse::Passthrough::Auto,
						crate::config::VfsPassthrough::Disabled => vfs::fuse::Passthrough::Disabled,
						crate::config::VfsPassthrough::Required => vfs::fuse::Passthrough::Required,
					},
				};
				let fuse = vfs::fuse::Server::start(provider, path, options)
					.await
					.map_err(|source| tg::error!(!source, "failed to start the FUSE server"))?;
				Server::Fuse(fuse)
			},
			Kind::Nfs => {
				let port = 8476;
				let host = if cfg!(target_os = "macos") {
					tokio::process::Command::new("dns-sd")
						.args([
							"-P",
							"Tangram",
							"_nfs._tcp",
							"local",
							&port.to_string(),
							"Tangram",
							"::1",
							"path=/",
						])
						.stdout(std::process::Stdio::null())
						.stderr(std::process::Stdio::null())
						.spawn()
						.map_err(|source| tg::error!(!source, "failed to spawn dns-sd"))?;
					"Tangram"
				} else {
					"localhost"
				};
				let nfs = vfs::nfs::Server::start(provider, path, host, port)
					.await
					.map_err(|source| tg::error!(!source, "failed to start the NFS server"))?;
				Self::Nfs(nfs)
			},
		};

		Ok(vfs)
	}

	pub async fn unmount(kind: Kind, path: &Path) -> tg::Result<()> {
		match kind {
			Kind::Fuse => vfs::fuse::Server::<Provider>::unmount(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to unmount"))?,
			Kind::Nfs => vfs::nfs::unmount(path)
				.await
				.map_err(|source| tg::error!(!source, "failed to unmount"))?,
		}
		Ok(())
	}

	pub fn stop(&self) {
		match self {
			Server::Fuse(server) => server.stop(),
			Server::Nfs(server) => server.stop(),
		}
	}

	pub async fn wait(self) {
		match self {
			Server::Fuse(server) => {
				server.wait().await;
			},
			Server::Nfs(server) => {
				server.wait().await;
			},
		}
	}
}
