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

		// Get the args.
		let args = process.command(server).await?.args(server).await?;

		// Get the name.
		let name = args
			.first()
			.ok_or_else(|| tg::error!("expected at least one arg"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the first arg to be a string"))?;

		let value = match name.as_str() {
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
