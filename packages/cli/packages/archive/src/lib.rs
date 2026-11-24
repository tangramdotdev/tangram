use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Archive an artifact.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[usage(long)]
	pub compression: Option<tg::CompressionFormat>,

	#[usage(long)]
	pub format: tg::ArchiveFormat,

	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let artifact = tangram_cli_reference::get_artifact(cli, &args.reference).await?;
	let artifact = tg::Artifact::with_referent(artifact);
	tg::builtin::validate_archive_artifact_with_instance(&artifact, &client).await?;
	let format = args.format;
	let compression = args.compression;
	let command = tg::builtin::archive_command(&artifact, format, compression);
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
		trailing: tangram_cli_process::Trailing::default(),
	};
	let output = tangram_cli_process::build::build(cli, args).await?;
	let output = if transform && !output.is_null() {
		let file: tg::File = output.try_into()?;
		file.contents_with_instance(&client).await?.into()
	} else {
		output
	};
	tangram_cli_process::build::print_build_output(cli, &options, output).await
}
