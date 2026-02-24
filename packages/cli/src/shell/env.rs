use {
	crate::{Cli, shell::Kind},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

/// Apply a shell environment.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub shell: Kind,
}

impl Cli {
	pub async fn command_shell_env(&mut self, args: Args) -> tg::Result<()> {
		let current = std::env::vars().collect::<BTreeMap<_, _>>();
		let target = self.resolve_shell_environment(&args.reference).await?;
		let (mutations, _, _) = Self::create_shell_mutations(&current, &target);
		let output = Self::create_shell_code(args.shell, &mutations);
		print!("{output}");
		Ok(())
	}
}
