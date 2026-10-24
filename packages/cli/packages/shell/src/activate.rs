use {
	crate::{Kind, util::State},
	std::collections::BTreeMap,
	tangram_cli_core::Cli,
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

pub async fn command_shell_activate(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut output = String::new();

	let mut current = std::env::vars().collect::<BTreeMap<_, _>>();

	if let Some(deactivate) = crate::util::deactivate_shell()? {
		let code = crate::util::create_shell_code(args.shell, &deactivate.mutations);
		output.push_str(&code);
		crate::util::apply_shell_mutations(&mut current, &deactivate.mutations);
		crate::util::print_shell_preserved_variable_messages(&deactivate.preserved);
	}

	let path = crate::util::build_shell_executable(cli, &args.reference).await?;
	let env = crate::util::run_shell_executable(&path).await?;
	let (activate, previous, current) = crate::util::create_shell_mutations(&current, &env);
	let code = crate::util::create_shell_code(args.shell, &activate);
	output.push_str(&code);

	if args.permanent {
		let code = crate::util::create_shell_state_code(args.shell, None);
		output.push_str(&code);
	} else {
		let path = crate::util::shell_state_path()?;
		let state = State {
			current,
			directory: false,
			previous,
			reference: args.reference.clone(),
		};
		crate::util::write_shell_state(&path, &state)?;
		let code = crate::util::create_shell_state_code(args.shell, Some(&path));
		output.push_str(&code);
	}

	print!("{output}");

	Ok(())
}
