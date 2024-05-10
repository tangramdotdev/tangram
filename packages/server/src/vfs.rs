use std::path::Path;
use tangram_client as tg;

mod fuse;
mod nfs;

#[derive(Debug, Clone, Copy)]
pub enum Kind {
	Fuse,
	Nfs,
}

#[derive(Clone)]
pub enum Server {
	Fuse(fuse::Server),
	Nfs(nfs::Server),
}

impl Server {
	pub async fn start(server: &crate::Server, kind: Kind, path: &Path) -> tg::Result<Self> {
		match kind {
			Kind::Fuse => {
				let fuse = fuse::Server::start(server, path).await?;
				let server = Self::Fuse(fuse);
				Ok(server)
			},
			Kind::Nfs => {
				let nfs = nfs::Server::start(server, path, 8437).await?;
				let server = Self::Nfs(nfs);
				Ok(server)
			},
		}
	}

	pub fn stop(&self) {
		match self {
			Server::Fuse(server) => server.stop(),
			Server::Nfs(server) => server.stop(),
		}
	}

	pub async fn wait(&self) {
		match self {
			Server::Fuse(server) => server.wait().await,
			Server::Nfs(server) => server.wait().await,
		}
	}
}
