use provider::Provider;
use std::path::Path;
use tangram_client as tg;
use tangram_vfs as vfs;

mod provider;

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
		tokio::fs::remove_file(path).await.ok();

		// Create a directory at the path if necessary.
		tokio::fs::create_dir_all(path).await.ok();

		// Create the provider.
		let options = provider::Options {
			cache_ttl: 10.0,
			cache_size: 2048,
			connections: 4,
		};
		let provider = Provider::new(server, options).await?;

		let vfs = match kind {
			Kind::Fuse => {
				let fuse = vfs::fuse::Vfs::start(provider, path)
					.await
					.map_err(|source| tg::error!(!source, "failed to start FUSE server"))?;
				Server::Fuse(fuse)
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
				Self::Nfs(nfs)
			},
		};

		Ok(vfs)
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
