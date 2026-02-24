use {crate::Cli, tangram_client::prelude::*};

/// List shell directories.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_shell_directory_list(&mut self, args: Args) -> tg::Result<()> {
		let config = self.read_config()?;
		let directories = config
			.shell
			.map_or_else(Default::default, |shell| shell.directories);
		self.print_serde(directories, args.print).await?;
		Ok(())
	}
}
