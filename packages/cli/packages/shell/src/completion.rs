use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Generate shell completions.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The shell to generate completions for.
	#[arg(value_enum)]
	shell: Shell,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum Shell {
	Bash,
	Fish,
	Zsh,
	Nu,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let mut command = cli.command.clone();
	let mut stdout = std::io::stdout();

	match args.shell {
		Shell::Bash => {
			let shell = clap_complete::shells::Bash;
			clap_complete::generate(shell, &mut command, "tangram", &mut stdout);
			println!("complete -F _tangram tg");
		},
		Shell::Fish => {
			let shell = clap_complete::shells::Fish;
			clap_complete::generate(shell, &mut command, "tangram", &mut stdout);
			println!("complete -c tg -w tangram");
		},
		Shell::Nu => {
			let shell = clap_complete_nushell::Nushell;
			clap_complete::generate(shell, &mut command, "tangram", &mut stdout);
			println!("export alias tg = tangram");
		},
		Shell::Zsh => {
			let shell = clap_complete::shells::Zsh;
			clap_complete::generate(shell, &mut command, "tangram", &mut stdout);
			println!("compdef _tangram tg");
		},
	}

	Ok(())
}
