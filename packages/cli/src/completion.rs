use {crate::Cli, clap::CommandFactory as _, tangram_client::prelude::*};

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

impl Cli {
	pub async fn command_completion(&mut self, args: Args) -> tg::Result<()> {
		let mut command = crate::Args::command();
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
}
