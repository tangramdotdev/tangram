/// Generate shell completions.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The shell to generate completions for.
	#[usage(value_enum)]
	pub shell: Shell,
}

#[derive(Clone, Copy, Debug, usage_rs::ValueEnum)]
pub enum Shell {
	Bash,
	Fish,
	Zsh,
	Nu,
}
