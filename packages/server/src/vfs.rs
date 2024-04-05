use std::path::Path;
use tangram_client as tg;

#[cfg(target_os = "linux")]
mod fuse;
#[cfg(target_os = "macos")]
mod nfs;

#[derive(Clone)]
pub enum Server {
	#[cfg(target_os = "linux")]
	Fuse(fuse::Server),
	#[cfg(target_os = "macos")]
	Nfs(nfs::Server),
}

impl Server {
	pub async fn start(server: &crate::Server, path: &Path) -> tg::Result<Self> {
		#[cfg(target_os = "linux")]
		{
			Ok(Self::Fuse(fuse::Server::start(server, path).await?))
		}
		#[cfg(target_os = "macos")]
		{
			Ok(Self::Nfs(nfs::Server::start(server, path, 8437).await?))
		}
	}

	pub fn stop(&self) {
		match self {
			#[cfg(target_os = "linux")]
			Server::Fuse(server) => server.stop(),
			#[cfg(target_os = "macos")]
			Server::Nfs(server) => server.stop(),
		}
	}

	pub async fn join(&self) -> tg::Result<()> {
		match self {
			#[cfg(target_os = "linux")]
			Server::Fuse(server) => server.join().await,
			#[cfg(target_os = "macos")]
			Server::Nfs(server) => server.join().await,
		}
	}
}

pub async fn unmount(path: &Path) -> tg::Result<()> {
	#[cfg(target_os = "linux")]
	fuse::Server::unmount(path).await?;
	#[cfg(target_os = "macos")]
	nfs::Server::unmount(path).await?;
	Ok(())
}
