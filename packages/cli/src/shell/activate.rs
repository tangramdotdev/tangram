use {
	crate::{
		Cli,
		shell::{Kind, util::State},
	},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

/// Activate a shell environment.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub permanent: bool,

	#[arg(index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub shell: Kind,
}

impl Cli {
	pub async fn command_shell_activate(&mut self, args: Args) -> tg::Result<()> {
		let mut output = String::new();

		let mut current = std::env::vars().collect::<BTreeMap<_, _>>();

		if let Some(deactivate) = Self::deactivate_shell()? {
			let code = Self::create_shell_code(args.shell, &deactivate.mutations);
			output.push_str(&code);
			Self::apply_shell_mutations(&mut current, &deactivate.mutations);
			Self::print_shell_preserved_variable_messages(&deactivate.preserved);
		}

		let path = self.build_shell_executable(&args.reference).await?;
		let env = self.run_shell_executable(&path).await?;
		let (activate, previous, current) = Self::create_shell_mutations(&current, &env);
		let code = Self::create_shell_code(args.shell, &activate);
		output.push_str(&code);

		if args.permanent {
			let code = Self::create_shell_state_code(args.shell, None);
			output.push_str(&code);
		} else {
			let path = Self::shell_state_path()?;
			let state = State {
				current,
				directory: false,
				previous,
				reference: args.reference.clone(),
			};
			Self::write_shell_state(&path, &state)?;
			let code = Self::create_shell_state_code(args.shell, Some(&path));
			output.push_str(&code);
		}

		print!("{output}");

		Ok(())
	}
}
