use crate::Cli;
use indoc::formatdoc;
use tangram_error::{error, Error, Result};

/// Print the shell hook.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub shell: Shell,
}

#[derive(Debug, Clone)]
pub enum Shell {
	Bash,
	Zsh,
}

impl Cli {
	pub async fn command_hook(&self, args: Args) -> Result<()> {
		let hook = match args.shell {
			Shell::Bash => formatdoc!(
				r#"
					export PATH="$HOME/.tangram/bin:$PATH"
					function tangram_chpwd {{}}
					export PROMPT_COMMAND="tangram_chpwd${{PROMPT_COMMAND:+;$PROMPT_COMMAND}}"
				"#,
			),
			Shell::Zsh => formatdoc!(
				r#"
					export PATH="$HOME/.tangram/bin:$PATH"
					function tangram_chpwd {{}}
					autoload -Uz add-zsh-hook
					add-zsh-hook chpwd tangram_chpwd
				"#,
			),
		};
		println!("{hook}");
		Ok(())
	}
}

impl std::str::FromStr for Shell {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"bash" => Ok(Shell::Bash),
			"zsh" => Ok(Shell::Zsh),
			_ => Err(error!("invalid shell")),
		}
	}
}
