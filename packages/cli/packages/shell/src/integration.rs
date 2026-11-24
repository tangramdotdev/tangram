use {crate::Kind, tangram_client::prelude::*};

const BASH: &str = include_str!("integration.bash");
const FISH: &str = include_str!("integration.fish");
const NU: &str = include_str!("integration.nu");
const ZSH: &str = include_str!("integration.zsh");

/// Generate shell integration.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(value_enum)]
	shell: Kind,
}

pub async fn run(args: Args) -> tg::Result<()> {
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
