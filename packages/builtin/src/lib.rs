use {
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client as tg,
};

mod archive;
mod bundle;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod util;

use self::{
	archive::archive, bundle::bundle, checksum::checksum, compress::compress,
	decompress::decompress, download::download, extract::extract,
};

pub type Logger = Arc<
	dyn Fn(tg::process::log::Stream, String) -> BoxFuture<'static, tg::Result<()>>
		+ Send
		+ Sync
		+ 'static,
>;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

pub async fn run<H>(
	handle: &H,
	process: &tg::Process,
	logger: Logger,
	temp_path: &std::path::Path,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	// Get the executable.
	let command = process.command(handle).await?;
	let executable = command.executable(handle).await?;
	let name = executable
		.try_unwrap_path_ref()
		.ok()
		.ok_or_else(|| tg::error!("expected the executable to be a path"))?
		.path
		.to_str()
		.ok_or_else(|| tg::error!("invalid executable"))?;

	let output = match name {
		"archive" => archive(handle, process, logger).boxed(),
		"bundle" => bundle(handle, process, logger).boxed(),
		"checksum" => checksum(handle, process, logger).boxed(),
		"compress" => compress(handle, process, logger).boxed(),
		"decompress" => decompress(handle, process, logger).boxed(),
		"download" => download(handle, process, logger, temp_path).boxed(),
		"extract" => extract(handle, process, logger, temp_path).boxed(),
		_ => {
			return Err(tg::error!("invalid executable"));
		},
	}
	.await?;

	Ok(output)
}

pub(crate) async fn log_progress_stream<T: Send + std::fmt::Debug>(
	logger: &Logger,
	stream: impl futures::Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
) -> tg::Result<()> {
	use futures::TryStreamExt as _;
	use std::pin::pin;

	let mut stream = pin!(stream);
	while let Some(event) = stream.try_next().await? {
		let (tg::progress::Event::Start(indicator)
		| tg::progress::Event::Finish(indicator)
		| tg::progress::Event::Update(indicator)) = event
		else {
			continue;
		};
		let message = format!("{indicator}\n");
		logger(tg::process::log::Stream::Stderr, message).await?;
	}
	Ok(())
}
