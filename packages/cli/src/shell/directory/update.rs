use {
	crate::{
		Cli,
		shell::{Kind, util::State},
	},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

/// Update the shell environment from the current directory configuration.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub shell: Kind,
}

impl Cli {
	pub async fn command_shell_directory_update(&mut self, args: Args) -> tg::Result<()> {
		let path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
		let path = std::fs::canonicalize(path).map_err(|source| {
			tg::error!(!source, "failed to canonicalize the current directory")
		})?;
		let directory = self.get_shell_directory(&path)?;
		let shell_state_path = std::env::var("TANGRAM_SHELL_STATE").ok();
		let state = shell_state_path
			.as_deref()
			.map(Self::load_shell_state)
			.transpose()?
			.flatten();
		let mut current = std::env::vars().collect::<BTreeMap<_, _>>();
		let mut output = String::new();

		let Some((directory_path, desired)) = directory else {
			if state.as_ref().is_some_and(|state| state.directory) {
				let deactivate = Self::deactivate_shell()?
					.ok_or_else(|| tg::error!("expected an active shell environment"))?;
				let code = Self::create_shell_code(args.shell, &deactivate.mutations);
				output.push_str(&code);
				let code = Self::create_shell_state_code(args.shell, None);
				output.push_str(&code);
				print!("{output}");
				self.print_info_message(&format!("deactivated {}", deactivate.reference));
				Self::print_shell_preserved_variable_messages(&deactivate.preserved);
			}
			return Ok(());
		};
		let directory = directory_path
			.to_str()
			.ok_or_else(|| tg::error!("the directory path is not valid UTF-8"))?
			.to_owned();
		let reference = format!("{directory}#{}", desired.export)
			.parse::<tg::Reference>()
			.map_err(|source| {
				tg::error!(!source, "failed to parse the shell directory reference")
			})?;

		if state
			.as_ref()
			.is_some_and(|state| state.directory && state.reference == reference)
		{
			return Ok(());
		}

		if state.is_some() {
			let deactivate = Self::deactivate_shell()?
				.ok_or_else(|| tg::error!("expected an active shell environment"))?;
			let code = Self::create_shell_code(args.shell, &deactivate.mutations);
			output.push_str(&code);
			Self::apply_shell_mutations(&mut current, &deactivate.mutations);
			Self::print_shell_preserved_variable_messages(&deactivate.preserved);
		}

		let path = self.build_shell_executable(&reference).await?;
		let env = self.run_shell_executable(&path).await?;
		let (activate, previous, current) = Self::create_shell_mutations(&current, &env);
		let code = Self::create_shell_code(args.shell, &activate);
		output.push_str(&code);
		let state_path = Self::shell_state_path()?;
		let state = State {
			current,
			directory: true,
			previous,
			reference: reference.clone(),
		};
		Self::write_shell_state(&state_path, &state)?;
		let code = Self::create_shell_state_code(args.shell, Some(&state_path));
		output.push_str(&code);
		print!("{output}");
		self.print_info_message(&format!("activated {directory}"));

		Ok(())
	}
}
