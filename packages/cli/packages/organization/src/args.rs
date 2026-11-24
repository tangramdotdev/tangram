use usage_rs as usage;

/// Manage organizations.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Billing(crate::billing::Args),

	#[usage(alias = "add")]
	Create(crate::create::Args),

	#[usage(alias = "remove", alias = "rm")]
	Delete(crate::delete::Args),

	Get(crate::get::Args),

	Members(crate::members::Args),

	Usage(crate::usage::Args),
}
