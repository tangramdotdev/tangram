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
mod util;

#[derive(Clone)]
pub struct Runtime {
	pub(super) server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		self.run_inner(process)
			.await
			.unwrap_or_else(|error| super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			})
	}

	async fn run_inner(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let server = &self.server;

		// Get the executable.
		let command = process.command(server).await?;
		let executable = command.executable(server).await?;
		let name = executable
			.try_unwrap_path_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the executable to be a path"))?
			.path
			.to_str()
			.ok_or_else(|| tg::error!("invalid executable"))?;

		let output = match name {
			"archive" => self.archive(process).boxed(),
			"bundle" => self.bundle(process).boxed(),
			"checksum" => self.checksum(process).boxed(),
			"compress" => self.compress(process).boxed(),
			"decompress" => self.decompress(process).boxed(),
			"download" => self.download(process).boxed(),
			"extract" => self.extract(process).boxed(),
			_ => {
				return Err(tg::error!("invalid executable"));
			},
		}
		.await?;

		Ok(output)
	}
}
