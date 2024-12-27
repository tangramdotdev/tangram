use super::util::spawn_checksum_build;
use crate::Server;
use futures::FutureExt as _;
use tangram_client as tg;

mod archive;
mod bundle;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;

#[derive(Clone)]
pub struct Runtime {
	server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn build(&self, build: &tg::Build, remote: Option<String>) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the target.
		let target = build.target(server).await?;

		// Get the args.
		let args = target.args(server).await?;

		// Get the name.
		let name = args
			.first()
			.ok_or_else(|| tg::error!("expected at least one arg"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the first arg to be a string"))?;

		let output = match name.as_str() {
			"archive" => self.archive(build, remote).boxed(),
			"bundle" => self.bundle(build, remote).boxed(),
			"checksum" => self.checksum(build, remote).boxed(),
			"compress" => self.compress(build, remote).boxed(),
			"decompress" => self.decompress(build, remote).boxed(),
			"download" => self.download(build, remote).boxed(),
			"extract" => self.extract(build, remote).boxed(),
			_ => {
				return Err(tg::error!("unknown name"));
			},
		}
		.await?;

		// Create a child build to calculate the checksum.
		let checksum = target.checksum(server).await?.clone();
		if let Some(checksum) = checksum {
			spawn_checksum_build(server, build.id().clone(), &output, &checksum).await?;
		}

		Ok(output)
	}
}
