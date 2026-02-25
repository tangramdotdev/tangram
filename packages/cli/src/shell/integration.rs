use {
	crate::{Cli, shell::Kind},
	tangram_client::prelude::*,
};

const BASH: &str = include_str!("integration.bash");
const FISH: &str = include_str!("integration.fish");
const NU: &str = include_str!("integration.nu");
const ZSH: &str = include_str!("integration.zsh");

/// Generate shell integration.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, value_enum)]
	shell: Kind,
}

impl Cli {
	pub async fn command_shell_integration(&mut self, args: Args) -> tg::Result<()> {
		match args.shell {
			Kind::Bash => {
				print!("{BASH}");
			},
			Kind::Fish => {
				print!("{FISH}");
			},
			Kind::Nu => {
				print!("{NU}");
			},
			Kind::Zsh => {
				print!("{ZSH}");
			},
		}

		Ok(())
	}
}
