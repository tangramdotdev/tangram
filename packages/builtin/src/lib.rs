use {
	self::{
		archive::archive, bundle::bundle, compress::compress, decompress::decompress,
		download::download, extract::extract,
	},
	futures::{TryStreamExt as _, future::BoxFuture},
	std::{path::Path, pin::pin, sync::Arc},
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
	dyn Fn(tg::process::stdio::Stream, Vec<u8>) -> BoxFuture<'static, tg::Result<()>>
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
	args: tg::value::data::Array,
	cwd: std::path::PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
	logger: Logger,
	temp_path: Option<&Path>,
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

	// Run the builtin. Only download produces a checksum.
	let mut checksum = None;
	let result = match name {
		"archive" => archive(handle, args, cwd, env, executable, logger).await,
		"bundle" => bundle(handle, args, cwd, env, executable, logger).await,
		"checksum" => self::checksum::checksum(handle, args, cwd, env, executable, logger).await,
		"compress" => compress(handle, args, cwd, env, executable, logger).await,
		"decompress" => decompress(handle, args, cwd, env, executable, logger).await,
		"download" => download(handle, args, cwd, env, executable, logger, temp_path)
			.await
			.map(|(value, download_checksum)| {
				checksum = download_checksum;
				value
			}),
		"extract" => extract(handle, args, cwd, env, executable, logger, temp_path).await,
		_ => {
			return Err(tg::error!("invalid executable"));
		},
	};

	// An error from a builtin means the build failed, not that the builtin could not run, so record it in the output.
	let output = match result {
		Ok(value) => Output {
			checksum,
			error: None,
			exit: 0,
			output: Some(value),
		},
		Err(error) => Output {
			checksum: None,
			error: Some(error),
			exit: 1,
			output: None,
		},
	};

	Ok(output)
}

pub(crate) async fn log_progress_stream<T: Send + std::fmt::Debug>(
	logger: &Logger,
	stream: impl futures::Stream<Item = tg::Result<tg::progress::Event<T>>> + Send + 'static,
) -> tg::Result<()> {
	let mut stream = pin!(stream);
	while let Some(event) = stream.try_next().await? {
		let tg::progress::Event::Indicators(indicators) = event else {
			continue;
		};
		for indicator in indicators {
			let message = format!("{indicator}\n");
			logger(tg::process::stdio::Stream::Stderr, message.into_bytes()).await?;
		}
	}
	Ok(())
}
