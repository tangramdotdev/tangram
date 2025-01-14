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

	pub async fn spawn(
		&self,
		process: &tg::process::Id,
		command: &tg::Command,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		let server = &self.server;

		// Get the args.
		let args = command.args(server).await?;

		// Get the checksum.
		let checksum = command.checksum(server).await?;

		// Try to reuse a build whose checksum is `None` or `Unsafe`.
		if let Ok(value) =
			super::util::try_reuse_process(server, process, &command, checksum.as_ref())
				.boxed()
				.await
		{
			return Ok(value);
		};

		// Get the name.
		let name = args
			.first()
			.ok_or_else(|| tg::error!("expected at least one arg"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the first arg to be a string"))?;

		let output = match name.as_str() {
			"archive" => self.archive(process, &command, remote).boxed(),
			"bundle" => self.bundle(process, &command, remote).boxed(),
			"checksum" => self.checksum(process, &command, remote).boxed(),
			"compress" => self.compress(process, &command, remote).boxed(),
			"decompress" => self.decompress(process, &command, remote).boxed(),
			"download" => self.download(process, &command, remote).boxed(),
			"extract" => self.extract(process, &command, remote).boxed(),
			_ => {
				return Err(tg::error!("unknown name"));
			},
		}
		.await?;

		// Checksum the output if necessary.
		if let Some(checksum) = checksum.as_ref() {
			super::util::checksum(server, process, &output, checksum)
				.boxed()
				.await?;
		}

		Ok(output)
	}
}
