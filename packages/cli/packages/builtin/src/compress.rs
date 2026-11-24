use {
	std::path::PathBuf, tangram_client::prelude::*,
	tangram_futures::read::shared_position_reader::SharedPositionReader,
	tokio::io::AsyncWriteExt as _, usage_rs as usage,
};

#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long)]
	pub format: tg::CompressionFormat,

	#[usage(long = "input", conflicts("input-positional"))]
	pub input_named: Option<PathBuf>,

	#[usage(conflicts("--input"))]
	pub input_positional: Option<PathBuf>,

	#[usage(long = "output", conflicts("output-positional"))]
	pub output_named: Option<PathBuf>,

	#[usage(conflicts("--output"))]
	pub output_positional: Option<PathBuf>,
}

pub async fn run(args: Args, progress_interval: std::time::Duration) -> tg::Result<()> {
	let input = super::util::resolve_path(args.input_named, args.input_positional);
	let output = super::util::resolve_path(args.output_named, args.output_positional);
	let total = super::util::input_length(input.as_deref()).await;
	let input = super::util::open_input(input.as_deref()).await?;
	let input = SharedPositionReader::with_reader_and_position(input, 0)
		.await
		.map_err(|error| tg::error!(!error, "failed to track the input position"))?;
	let progress = super::progress::Progress::with_position(
		"compressing",
		total,
		input.shared_position(),
		progress_interval,
	)?;
	let input = tokio::io::BufReader::new(input);
	let mut input: Box<dyn tokio::io::AsyncRead + Send + Unpin> = match args.format {
		tg::CompressionFormat::Bz2 => {
			Box::new(async_compression::tokio::bufread::BzEncoder::new(input))
		},
		tg::CompressionFormat::Gz => {
			Box::new(async_compression::tokio::bufread::GzipEncoder::new(input))
		},
		tg::CompressionFormat::Xz => {
			Box::new(async_compression::tokio::bufread::XzEncoder::new(input))
		},
		tg::CompressionFormat::Zst => {
			Box::new(async_compression::tokio::bufread::ZstdEncoder::new(input))
		},
	};
	let mut output = super::util::open_output(output.as_deref()).await?;
	tokio::io::copy(&mut input, &mut output)
		.await
		.map_err(|error| tg::error!(!error, "failed to compress the input"))?;
	output
		.shutdown()
		.await
		.map_err(|error| tg::error!(!error, "failed to finish the output"))?;
	progress.finish("finished compressing")?;

	Ok(())
}
