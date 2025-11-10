use {
	self::{
		archive::archive, bundle::bundle, checksum::checksum, compress::compress,
		decompress::decompress, download::download, extract::extract,
	},
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
};

mod archive;
mod bundle;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod util;

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

#[allow(clippy::too_many_arguments)]
pub async fn run<H>(
	handle: &H,
	process: Option<&tg::Process>,
	args: tg::value::data::Array,
	cwd: std::path::PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
	logger: Logger,
	temp_path: &std::path::Path,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	// Get the executable name.
	let name = match &executable {
		tg::command::data::Executable::Path(path_executable) => path_executable
			.path
			.to_str()
			.ok_or_else(|| tg::error!("invalid executable"))?,
		_ => return Err(tg::error!("expected the executable to be a path")),
	};

	let output = match name {
		"archive" => archive(handle, process, args, cwd, env, executable, logger).boxed(),
		"bundle" => bundle(handle, process, args, cwd, env, executable, logger).boxed(),
		"checksum" => checksum(handle, process, args, cwd, env, executable, logger).boxed(),
		"compress" => compress(handle, process, args, cwd, env, executable, logger).boxed(),
		"decompress" => decompress(handle, process, args, cwd, env, executable, logger).boxed(),
		"download" => download(
			handle, process, args, cwd, env, executable, logger, temp_path,
		)
		.boxed(),
		"extract" => extract(
			handle, process, args, cwd, env, executable, logger, temp_path,
		)
		.boxed(),
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
