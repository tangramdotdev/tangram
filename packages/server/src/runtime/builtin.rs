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
	pub(super) server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		let (error, exit, output) = match self.run_inner(process).await {
			Ok(output) => (None, None, Some(output)),
			Err(error) => (Some(error), None, None),
		};
		super::Output {
			error,
			exit,
			output,
		}
	}

	async fn run_inner(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the executable.
		let command = process.command(server).await?;
		let executable = command.executable(server).await?;
		let name = executable
			.try_unwrap_path_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the path variant of executable"))?
			.to_str()
			.unwrap();

		let value = match name {
			"archive" => self.archive(process).boxed(),
			"bundle" => self.bundle(process).boxed(),
			"checksum" => self.checksum(process).boxed(),
			"compress" => self.compress(process).boxed(),
			"decompress" => self.decompress(process).boxed(),
			"download" => self.download(process).boxed(),
			"extract" => self.extract(process).boxed(),
			_ => {
				return Err(tg::error!("unknown name"));
			},
		}
		.await?;

		Ok(value)
	}
}
