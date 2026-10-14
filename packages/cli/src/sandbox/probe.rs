use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub path: PathBuf,
}

impl Cli {
	pub fn command_sandbox_probe_id_mapped_mount(
		args: &Args,
	) -> tg::Result<std::process::ExitCode> {
		tangram_sandbox::host::probe_id_mapped_mount(&args.path)?;
		Ok(std::process::ExitCode::SUCCESS)
	}
}
