use {crate::Cli, tangram_client::prelude::*};

/// Get a process's storage status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_process_stored(&mut self, args: Args) -> tg::Result<()> {
		let process = self
			.resolve_process_with_locations(&args.process, &args.options.locations)
			.await?;
		self.command_process_stored_inner(process, args.options)
			.await
	}

	pub(crate) async fn command_process_stored_inner(
		&mut self,
		process: tg::Referent<tg::process::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = process.options.location.clone().map(Into::into);
		let id = process.node;
		let arg = tg::process::stored::Arg {
			location,
			tokens: process.options.tokens,
		};
		let output = client
			.try_get_process_stored(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process's storage status"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process's storage status"))?;
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
