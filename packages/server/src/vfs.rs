use std::path::Path;
use tangram_client as tg;
use tangram_database as db;
use tangram_vfs as vfs;

mod provider;
use provider::Provider;

#[derive(Debug, Clone, Copy)]
pub enum Kind {
	Fuse,
	Nfs,
}

#[derive(Clone)]
pub enum Server {
	Fuse(vfs::fuse::Vfs<Provider>),
	Nfs(vfs::nfs::Vfs<Provider>),
}

impl Server {
	pub async fn start(server: &crate::Server, kind: Kind, path: &Path) -> tg::Result<Self> {
		// Remove a file at the path if one exists.
		std::fs::remove_file(path).ok();

		// Create a directory at the path if necessary.
		std::fs::create_dir_all(path).ok();

		let options = db::sqlite::Options {
			path: server.path.join("vfs"),
			connections: 8,
		};
		let provider = Provider::new(server, options).await?;

		match kind {
			Kind::Fuse => {
				let fuse = vfs::fuse::Vfs::start(provider, path)
					.await
					.map_err(|source| tg::error!(!source, "failed to start FUSE server"))?;
				Ok(Server::Fuse(fuse))
			},
			Kind::Nfs => {
				let port = 8476;
				let url = if cfg!(target_os = "macos") {
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
				let nfs = vfs::nfs::Vfs::start(provider, path, url.into(), port)
					.await
					.map_err(|source| tg::error!(!source, "failed to start NFS server"))?;
				Ok(Self::Nfs(nfs))
			},
		}
	}

	pub fn stop(&self) {
		match self {
			Server::Fuse(server) => server.stop(),
			Server::Nfs(server) => server.stop(),
		}
	}

	pub async fn wait(self) -> tg::Result<()> {
		match self {
			Server::Fuse(server) => server.wait().await,
			Server::Nfs(server) => server.wait().await,
		}
		Ok(())
	}
}
