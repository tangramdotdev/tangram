use {crate::Cli, tangram_client as tg};

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_outdated(&mut self, _args: Args) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
	}
}
