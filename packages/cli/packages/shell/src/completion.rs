use usage_rs as usage;

/// Generate shell completions.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The shell to generate completions for.
	#[usage(value_enum)]
	pub shell: Shell,
}

#[derive(Clone, Copy, Debug, usage::ValueEnum)]
pub enum Shell {
	Bash,
	Fish,
	Zsh,
	Nu,
}
