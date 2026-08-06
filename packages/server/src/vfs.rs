use {
	provider::Provider,
	std::{
		os::fd::OwnedFd,
		path::Path,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_vfs as vfs,
};

#[cfg(target_os = "macos")]
mod fskit;

mod provider;

#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Fskit,
	Fuse,
	Nfs,
}

pub enum Server {
	#[cfg(target_os = "macos")]
	Fskit(fskit::Server),

	#[cfg(target_os = "linux")]
	Fuse(vfs::fuse::Server<Provider>),

	Nfs(vfs::nfs::Server<Provider>),

	#[cfg(target_os = "linux")]
	Virtiofs(vfs::virtiofs::Server),
}

impl Server {
	pub async fn start(
		server: &crate::Server,
		kind: Kind,
		path: &Path,
		options: crate::config::Vfs,
		origin: crate::Origin,
		principal: Arc<Mutex<Option<tg::Principal>>>,
		recvfd: Option<OwnedFd>,
	) -> tg::Result<Self> {
		// Remove a file at the path if one exists.
		tokio::fs::remove_file(path).await.ok();

		// Create a directory at the path if necessary.
		tokio::fs::create_dir_all(path).await.ok();

		// Create the provider.
		let provider = Provider::new(server, origin, principal.clone())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the vfs provider"))?;

		let vfs = match kind {
			Kind::Fskit => {
				#[cfg(target_os = "macos")]
				{
					let principal = principal
						.lock()
						.unwrap()
						.clone()
						.unwrap_or(tg::Principal::Anonymous);
					let fskit = fskit::Server::start(server, path, principal).await?;
					Server::Fskit(fskit)
				}
				#[cfg(not(target_os = "macos"))]
				{
					let _ = server;
					return Err(tg::error!("fskit is only supported on macos"));
				}
			},
			Kind::Fuse => {
				#[cfg(target_os = "linux")]
				{
					let options = vfs::fuse::Options {
						io: match options.io {
							crate::config::VfsIo::Auto => vfs::fuse::Io::Auto,
							crate::config::VfsIo::IoUring => vfs::fuse::Io::IoUring,
							crate::config::VfsIo::ReadWrite => vfs::fuse::Io::ReadWrite,
						},
						passthrough: match options.passthrough {
							crate::config::VfsPassthrough::Auto => vfs::fuse::Passthrough::Auto,
							crate::config::VfsPassthrough::Disabled => {
								vfs::fuse::Passthrough::Disabled
							},
							crate::config::VfsPassthrough::Required => {
								vfs::fuse::Passthrough::Required
							},
						},
					};
					// Without a sandbox socket, mount with the fusermount3 helper after unmounting a stale mount.
					let recvfd = match recvfd {
						None => {
							vfs::fuse::Server::<Provider>::unmount(path).await.ok();
							vfs::fuse::fusermount3(path).map_err(|error| {
								tg::error!(!error, "failed to start the FUSE mount helper")
							})?
						},
						Some(recvfd) => recvfd,
					};
					let fuse = vfs::fuse::Server::start(provider, path, options, recvfd)
						.await
						.map_err(|error| tg::error!(!error, "failed to start the FUSE server"))?;
					Server::Fuse(fuse)
				}
				#[cfg(not(target_os = "linux"))]
				{
					let _ = (options, recvfd);
					return Err(tg::error!("the FUSE VFS is only supported on Linux"));
				}
			},
			Kind::Nfs => {
				let port = 8476;
				let host = if cfg!(target_os = "macos") {
					"Tangram"
				} else {
					"localhost"
				};
				let nfs = vfs::nfs::Server::start(provider, path, host, port)
					.await
					.map_err(|error| tg::error!(!error, "failed to start the NFS server"))?;
				Self::Nfs(nfs)
			},
		};

		Ok(vfs)
	}

	#[cfg(target_os = "linux")]
	pub async fn start_virtiofs(
		server: &crate::Server,
		socket: &Path,
		dax: Option<u64>,
		origin: crate::Origin,
		principal: Arc<Mutex<Option<tg::Principal>>>,
	) -> tg::Result<Self> {
		let provider = Provider::new(server, origin, principal)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the vfs provider"))?;
		let dax_window_size = dax.unwrap_or(0);
		let server = vfs::virtiofs::Server::start(provider, socket, dax_window_size)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the virtiofsd server"))?;
		Ok(Self::Virtiofs(server))
	}

	pub async fn unmount(kind: Kind, path: &Path) -> tg::Result<()> {
		match kind {
			Kind::Fskit => {
				#[cfg(target_os = "macos")]
				{
					fskit::Server::unmount(path).await?;
				}
				#[cfg(not(target_os = "macos"))]
				{
					let _ = path;
					return Err(tg::error!("fskit is only supported on macos"));
				}
			},
			Kind::Fuse => {
				#[cfg(target_os = "linux")]
				{
					vfs::fuse::Server::<Provider>::unmount(path)
						.await
						.map_err(|error| tg::error!(!error, "failed to unmount"))?;
				}
				#[cfg(not(target_os = "linux"))]
				{
					let _ = path;
					return Err(tg::error!("fuse is only supported on linux"));
				}
			},
			Kind::Nfs => vfs::nfs::unmount(path)
				.await
				.map_err(|error| tg::error!(!error, "failed to unmount"))?,
		}
		Ok(())
	}

	pub fn stop(&self) {
		match self {
			#[cfg(target_os = "macos")]
			Server::Fskit(_) => {},
			#[cfg(target_os = "linux")]
			Server::Fuse(server) => server.stop(),
			Server::Nfs(server) => server.stop(),
			#[cfg(target_os = "linux")]
			Server::Virtiofs(server) => server.stop(),
		}
	}

	pub async fn wait(self) {
		match self {
			#[cfg(target_os = "macos")]
			Server::Fskit(server) => {
				if let Err(error) = fskit::Server::unmount(server.path()).await {
					tracing::error!(?error, "failed to unmount the fskit vfs");
				}
			},
			#[cfg(target_os = "linux")]
			Server::Fuse(server) => {
				server.wait().await;
			},
			Server::Nfs(server) => {
				server.wait().await;
			},
			#[cfg(target_os = "linux")]
			Server::Virtiofs(server) => {
				server.wait().await;
			},
		}
	}
}
