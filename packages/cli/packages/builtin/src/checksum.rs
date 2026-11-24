use {
	std::path::PathBuf, tangram_client::prelude::*,
	tangram_futures::read::shared_position_reader::SharedPositionReader,
	tokio::io::AsyncWriteExt as _,
};

#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long)]
	pub algorithm: tg::checksum::Algorithm,

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
	if let Some(input) = &input {
		let metadata = tokio::fs::metadata(input).await.map_err(
			|error| tg::error!(!error, path = %input.display(), "failed to read the input metadata"),
		)?;
		if !metadata.is_file() {
			return Err(tg::error!(path = %input.display(), "the input is not a file"));
		}
	}
	let input = super::util::open_input(input.as_deref()).await?;
	let mut input = SharedPositionReader::with_reader_and_position(input, 0)
		.await
		.map_err(|error| tg::error!(!error, "failed to track the input position"))?;
	let progress = super::progress::Progress::with_position(
		"computing checksum",
		None,
		input.shared_position(),
		progress_interval,
	)?;
	let mut writer = tg::checksum::Writer::new(args.algorithm);
	tokio::io::copy(&mut input, &mut writer)
		.await
		.map_err(|error| tg::error!(!error, "failed to checksum the input"))?;
	let checksum = writer.finalize();
	let mut output = super::util::open_output(output.as_deref()).await?;
	output
		.write_all(checksum.to_string().as_bytes())
		.await
		.map_err(|error| tg::error!(!error, "failed to write the output"))?;
	output
		.shutdown()
		.await
		.map_err(|error| tg::error!(!error, "failed to finish the output"))?;
	progress.finish("finished computing checksum")?;

	Ok(())
}
