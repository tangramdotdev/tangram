use {crate::Server, futures::FutureExt as _, tangram_client as tg};

mod archive;
mod bundle;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod util;

impl Server {
	pub(crate) async fn run_builtin(&self, process: &tg::Process) -> tg::Result<super::Output> {
		// Get the executable.
		let command = process.command(self).await?;
		let executable = command.executable(self).await?;
		let name = executable
			.try_unwrap_path_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected the executable to be a path"))?
			.path
			.to_str()
			.ok_or_else(|| tg::error!("invalid executable"))?;

		let output = match name {
			"archive" => self.run_builtin_archive(process).boxed(),
			"bundle" => self.run_builtin_bundle(process).boxed(),
			"checksum" => self.run_builtin_checksum(process).boxed(),
			"compress" => self.run_builtin_compress(process).boxed(),
			"decompress" => self.run_builtin_decompress(process).boxed(),
			"download" => self.run_builtin_download(process).boxed(),
			"extract" => self.run_builtin_extract(process).boxed(),
			_ => {
				return Err(tg::error!("invalid executable"));
			},
		}
		.await?;

		Ok(output)
	}
}
