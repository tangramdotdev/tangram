use {futures::FutureExt as _, tangram_cli_core::Cli, tangram_client::prelude::*};

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let blob = tangram_cli_get::get_object(cli, &args.reference).await?;
	let blob = tg::Object::with_referent(blob)
		.try_unwrap_blob()
		.map_err(|_| tg::error!("expected a blob"))?;
	let command = tg::builtin::extract_command(&blob);
	let command = command
		.store_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, "failed to store the command"))?;
	let reference = tg::Reference::with_object(command.into());
	let args = tangram_cli_process::build::Args {
		options: args.build,
		reference: Some(reference),
		trailing: Vec::new(),
	};
	tangram_cli_process::build::run(cli, args).boxed().await?;
	Ok(())
}
