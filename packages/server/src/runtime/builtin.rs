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

	pub async fn run(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> super::Output {
		let (error, exit, value) = match self.run_inner(process, command, remote).await {
			Ok(value) => (None, None::<tg::process::Exit>, Some(value)),
			Err(error) => (Some(error), None, None),
		};
		super::Output { error, exit, value }
	}

	async fn run_inner(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the args.
		let args = command.args(server).await?;

		// Get the name.
		let name = args
			.first()
			.ok_or_else(|| tg::error!("expected at least one arg"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the first arg to be a string"))?;

		let value = match name.as_str() {
			"archive" => self.archive(process, command, remote).boxed(),
			"bundle" => self.bundle(process, command, remote).boxed(),
			"checksum" => self.checksum(process, command, remote).boxed(),
			"compress" => self.compress(process, command, remote).boxed(),
			"decompress" => self.decompress(process, command, remote).boxed(),
			"download" => self.download(process, command, remote).boxed(),
			"extract" => self.extract(process, command, remote).boxed(),
			_ => {
				return Err(tg::error!("unknown name"));
			},
		}
		.await?;

		Ok(value)
	}
}
