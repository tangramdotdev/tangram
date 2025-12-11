#[derive(Clone, Debug, Default, clap::Args)]
pub struct Local {
	/// Use the local server.
	#[arg(
		long,
		default_missing_value = "true",
		num_args = 0..=1,
		require_equals = true,
	)]
	pub local: Option<bool>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Remotes {
	/// The remotes to use. Use --remote for the default remote, --remote=<name> for a specific one. Can be specified multiple times or comma-separated.
	#[arg(
		long = "remotes",
		default_missing_value = "default",
		num_args = 0..=1,
		require_equals = true,
		value_delimiter = ',',
		visible_alias = "remote",
	)]
	pub remotes: Option<Vec<String>>,
}
