use {
	crate::Cli,
	tangram_client::{self as tg, handle::Process},
};

/// List processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub pretty: Option<bool>,

	#[allow(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_process_list(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::process::list::Arg::default();
		let output = handle
			.list_processes(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the processes"))?;
		Self::print_json(&output, args.pretty).await?;
		Ok(())
	}
}
