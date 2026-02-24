use {
	crate::{
		Cli,
		shell::{Kind, common::State},
	},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

/// Activate a shell environment.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub shell: Kind,
}

impl Cli {
	pub async fn command_shell_activate(&mut self, args: Args) -> tg::Result<()> {
		let mut output = String::new();

		let mut current = std::env::vars().collect::<BTreeMap<_, _>>();

		let (deactivate, preserved, deactivated) = Self::deactivate_shell()?;
		if deactivated.is_some() {
			let code = Self::create_shell_code(args.shell, &deactivate);
			output.push_str(&code);
			Self::apply_shell_environment_mutations(&mut current, &deactivate);
			Self::print_shell_preserved_environment_messages(&preserved);
		}

		let target = self.resolve_shell_environment(&args.reference).await?;
		let (activate, previous, current) = Self::create_shell_mutations(&current, &target);
		let code = Self::create_shell_code(args.shell, &activate);
		output.push_str(&code);

		let path = Self::shell_state_path()?;
		let state = State {
			current,
			directory: None,
			previous,
		};
		Self::write_shell_state(&path, &state)?;
		output.push_str(&Self::create_shell_state_code(args.shell, Some(&path)));

		print!("{output}");

		Ok(())
	}
}
