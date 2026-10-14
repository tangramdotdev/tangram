use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	#[arg(long)]
	pub config: PathBuf,
}

impl Cli {
	pub fn command_sandbox_setup(args: Args) -> tg::Result<std::process::ExitCode> {
		#[cfg(target_os = "linux")]
		{
			let contents = std::fs::read(args.config).map_err(|error| {
				tg::error!(!error, "failed to read the sandbox setup configuration")
			})?;
			let arg = serde_json::from_slice(&contents).map_err(|error| {
				tg::error!(!error, "failed to parse the sandbox setup configuration")
			})?;
			tangram_sandbox::run::linux::run(arg)
		}
		#[cfg(not(target_os = "linux"))]
		{
			let _ = args;
			Err(tg::error!("Linux sandbox setup is not available"))
		}
	}
}
