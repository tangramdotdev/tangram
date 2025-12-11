use {crate::Cli, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the process.
		let arg = tg::process::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes.clone(),
		};
		let output = handle
			.try_get_process(&args.process, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))?;

		// Get the output.
		let output = output
			.data
			.output
			.map(TryInto::try_into)
			.transpose()?
			.unwrap_or(tg::Value::Null);

		// Print the output.
		let arg = tg::object::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		self.print_value(&output, args.print, arg).await?;

		Ok(())
	}
}
