use std::path::Path;
use tangram_client as tg;
use tangram_error::Result;

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
	pub async fn start(tg: &dyn tg::Handle, path: &Path) -> Result<Self> {
		#[cfg(target_os = "linux")]
		{
			Ok(Self::Fuse(fuse::Server::start(tg, path).await?))
		}
		#[cfg(target_os = "macos")]
		{
			Ok(Self::Nfs(nfs::Server::start(tg, path, 8437).await?))
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

	pub async fn join(&self) -> Result<()> {
		match self {
			#[cfg(target_os = "linux")]
			Server::Fuse(server) => server.join().await,
			#[cfg(target_os = "macos")]
			Server::Nfs(server) => server.join().await,
		}
	}
}

pub async fn unmount(path: &Path) -> Result<()> {
	#[cfg(target_os = "linux")]
	fuse::Server::unmount(path).await?;
	#[cfg(target_os = "macos")]
	nfs::Server::unmount(path).await?;
	Ok(())
}
