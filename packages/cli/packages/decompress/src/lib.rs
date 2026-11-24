use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Decompress a blob or a file.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub build: tangram_cli_process::build::Options,

	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let input = tangram_cli_reference::get_object(cli, &args.reference).await?;
	let (input, blob) = match tg::Object::with_referent(input) {
		tg::Object::Blob(blob) => (tg::Either::Left(blob), true),
		tg::Object::File(file) => (tg::Either::Right(file), false),
		_ => return Err(tg::error!("expected a blob or file")),
	};
	let command = tg::builtin::decompress_command(input);
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
	let output = if transform && blob && !output.is_null() {
		let file: tg::File = output.try_into()?;
		file.contents_with_instance(&client).await?.into()
	} else {
		output
	};
	tangram_cli_process::build::print_build_output(cli, &options, output).await
}
