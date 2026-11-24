use usage_rs as usage;

/// Manage the user.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Billing(crate::billing::Args),
	Get(crate::get::Args),
	Login(crate::login::Args),
	Logout(crate::logout::Args),
	Token(crate::token::Args),
	Usage(crate::usage::Args),
	Whoami(crate::whoami::Args),
}
