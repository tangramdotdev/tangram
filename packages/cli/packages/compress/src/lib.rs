use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Compress a blob or a file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[arg(long, short)]
	pub format: tg::CompressionFormat,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

pub async fn command_compress(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let input = tangram_cli_get::get_object(cli, &args.reference).await?;
	let (input, blob) = match tg::Object::with_referent(input) {
		tg::Object::Blob(blob) => (tg::Either::Left(blob), true),
		tg::Object::File(file) => (tg::Either::Right(file), false),
		_ => return Err(tg::error!("expected a blob or file")),
	};
	let format = args.format;
	let command = tg::builtin::compress_command(input, format);
	let command = command
		.store_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to store the command"))?;
	let reference = tg::Reference::with_object(command.into());
	let options = args.build;
	let transform = !options.detach && options.checkout.is_none();
	let args = tangram_cli_process::build::Args {
		options: options.clone(),
		reference: Some(reference),
		trailing: Vec::new(),
	};
	let output = tangram_cli_process::build::build(cli, args).await?;
	let output = if transform && blob && !output.is_null() {
		let file: tg::File = output.try_into()?;
		file.contents_with_instance(&client).await?.into()
	} else {
		output
	};
	tangram_cli_process::build::print_build_output(cli, &options, output).await
}
