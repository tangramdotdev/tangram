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
		let directory = self.get_current_shell_directory()?;
		if directory.is_some() {
			return Err(tg::error!(
				"cannot deactivate while the current directory is configured for an automatic shell environment"
			));
		}

		let has_state_env = std::env::var_os("TANGRAM_SHELL_STATE").is_some();
		let state = Self::load_shell_state()?;
		let Some((_, state)) = state else {
			if has_state_env {
				let code = Self::create_shell_state_code(args.shell, None);
				print!("{code}");
			}
			return Ok(());
		};
		if state.directory.is_some() {
			return Err(tg::error!(
				"cannot deactivate a directory shell environment; remove the directory configuration or change directories"
			));
		}

		let (mutations, preserved, _) = Self::deactivate_shell()?;
		let mut output = Self::create_shell_code(args.shell, &mutations);
		output.push_str(&Self::create_shell_state_code(args.shell, None));
		print!("{output}");

		Self::print_shell_preserved_environment_messages(&preserved);

		Ok(())
	}
}
