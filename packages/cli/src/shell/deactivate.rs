use {
	crate::{Cli, shell::Kind},
	tangram_client::prelude::*,
};

/// Deactivate the shell environment.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub shell: Kind,
}

impl Cli {
	pub async fn command_shell_deactivate(&mut self, args: Args) -> tg::Result<()> {
		let path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the current directory"))?;
		let path = std::fs::canonicalize(path).map_err(|source| {
			tg::error!(!source, "failed to canonicalize the current directory")
		})?;
		let directory = self.get_shell_directory(&path)?;
		if directory.is_some() {
			let error = tg::error!("cannot deactivate the current directory's shell environment");
			return Err(error);
		}

		let shell_state_path = std::env::var("TANGRAM_SHELL_STATE").ok();
		let has_state_env = shell_state_path.is_some();
		let state = shell_state_path
			.as_deref()
			.map(Self::load_shell_state)
			.transpose()?
			.flatten();
		let Some(state) = state else {
			if has_state_env {
				let code = Self::create_shell_state_code(args.shell, None);
				print!("{code}");
			}
			return Ok(());
		};
		if state.directory {
			let error = tg::error!("cannot deactivate the current directory's shell environment");
			return Err(error);
		}

		let deactivate_output = Self::deactivate_shell()?;
		let Some(deactivate_output) = deactivate_output else {
			if has_state_env {
				let code = Self::create_shell_state_code(args.shell, None);
				print!("{code}");
			}
			return Ok(());
		};
		let mut output = String::new();
		let code = Self::create_shell_code(args.shell, &deactivate_output.mutations);
		output.push_str(&code);
		let code = Self::create_shell_state_code(args.shell, None);
		output.push_str(&code);
		print!("{output}");

		Self::print_info_message(&format!("deactivated {}", deactivate_output.reference));
		Self::print_shell_preserved_variable_messages(&deactivate_output.preserved);

		Ok(())
	}
}
