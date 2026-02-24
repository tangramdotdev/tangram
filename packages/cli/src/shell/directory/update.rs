use {
	crate::{
		Cli,
		shell::{
			Kind,
			common::{Directory, State},
		},
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
		let directory = self.get_current_shell_directory()?;
		let state = Self::load_shell_state()?;
		let mut current = std::env::vars().collect::<BTreeMap<_, _>>();
		let mut output = String::new();

		match (directory, state) {
			(None, Some((_, state))) if state.directory.is_some() => {
				let (mutations, preserved, _) = Self::deactivate_shell()?;
				let code = Self::create_shell_code(args.shell, &mutations);
				output.push_str(&code);
				let code = Self::create_shell_state_code(args.shell, None);
				output.push_str(&code);
				print!("{output}");
				Self::print_info_message("deactivated the directory environment.");
				Self::print_shell_preserved_environment_messages(&preserved);
			},

			(Some((directory, desired)), Some((_, state))) => {
				let directory_path = directory;
				let directory = directory_path
					.to_str()
					.ok_or_else(|| tg::error!("the directory path is not valid UTF-8"))?
					.to_owned();
				let reference = format!("{directory}#{}", desired.export)
					.parse::<tg::Reference>()
					.map_err(|source| {
						tg::error!(!source, "failed to parse the shell directory reference")
					})?;
				if state.directory.as_ref().is_some_and(|state| {
					state.export == desired.export && state.path == directory_path
				}) {
					return Ok(());
				}
				Self::print_warning_message(
					"replacing the active shell environment with the directory environment.",
				);
				let (mutations, preserved, _) = Self::deactivate_shell()?;
				let code = Self::create_shell_code(args.shell, &mutations);
				output.push_str(&code);
				Self::apply_shell_environment_mutations(&mut current, &mutations);
				Self::print_shell_preserved_environment_messages(&preserved);

				let target = self.resolve_shell_environment(&reference).await?;
				let (activate, previous, current) = Self::create_shell_mutations(&current, &target);
				let code = Self::create_shell_code(args.shell, &activate);
				output.push_str(&code);

				let path = Self::shell_state_path()?;
				let state = State {
					current,
					directory: Some(Directory {
						export: desired.export.clone(),
						path: directory_path,
					}),
					previous,
				};
				Self::write_shell_state(&path, &state)?;
				output.push_str(&Self::create_shell_state_code(args.shell, Some(&path)));
				print!("{output}");

				Self::print_info_message(&format!(
					"activated the directory environment for {directory}."
				));
			},

			(Some((path, config)), None) => {
				let directory = path
					.to_str()
					.ok_or_else(|| tg::error!("the directory path is not valid UTF-8"))?
					.to_owned();
				let reference = format!("{directory}#{}", config.export)
					.parse::<tg::Reference>()
					.map_err(|source| {
						tg::error!(!source, "failed to parse the shell directory reference")
					})?;
				let target = self.resolve_shell_environment(&reference).await?;
				let (activate, previous, current) = Self::create_shell_mutations(&current, &target);
				output.push_str(&Self::create_shell_code(args.shell, &activate));
				let state_path = Self::shell_state_path()?;
				let state = State {
					current,
					directory: Some(Directory {
						export: config.export.clone(),
						path,
					}),
					previous,
				};
				Self::write_shell_state(&state_path, &state)?;
				let code = Self::create_shell_state_code(args.shell, Some(&state_path));
				output.push_str(&code);
				print!("{output}");
				Self::print_info_message(&format!(
					"activated the directory environment for {directory}."
				));
			},

			(None, None | Some(_)) => (),
		}

		Ok(())
	}
}
