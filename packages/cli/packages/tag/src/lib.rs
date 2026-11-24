use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Tag {
	/// Create the tag's missing ancestors after pulling.
	#[usage(
		default_missing = "true",
		long = "create-tag-ancestors",
		overrides("--no-create-tag-ancestors"),
		require_equals = true,
		requires("--tag"),
		value_name = "BOOL",
		visible_alias = "create-tag-parents"
	)]
	create: Option<bool>,

	/// Replace a conflicting named node and its descendants when setting the tag.
	#[usage(long = "force-tag", requires("--tag"))]
	pub force: bool,

	/// Do not create the tag's missing ancestors.
	#[usage(
		default_missing = "true",
		long = "no-create-tag-ancestors",
		overrides("--create-tag-ancestors"),
		require_equals = true,
		requires("--tag"),
		value_name = "BOOL",
		visible_alias = "no-create-tag-parents"
	)]
	no_create: Option<bool>,

	/// Do not pull the tag's ancestors.
	#[usage(
		default_missing = "true",
		long = "no-pull-tag-ancestors",
		overrides("--pull-tag-ancestors"),
		require_equals = true,
		requires("--tag"),
		value_name = "BOOL",
		visible_alias = "no-pull-tag-parents"
	)]
	no_pull: Option<bool>,

	/// Pull the tag's ancestors using the specified policy: always, missing, or never.
	#[usage(
		default_missing = "always",
		long = "pull-tag-ancestors",
		overrides("--no-pull-tag-ancestors"),
		require_equals = true,
		requires("--tag"),
		value_name = "POLICY",
		visible_alias = "pull-tag-parents"
	)]
	pull: Option<tg::node::AncestorsPull>,

	/// Tag the result.
	#[usage(long = "tag")]
	pub specifier: Option<tg::Specifier>,
}

/// Manage tags.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
#[usage(
	args_conflicts_with_subcommands = true,
	subcommand_negates_reqs = true,
	subcommand_precedence_over_arg = true
)]
pub struct Args {
	#[usage(flatten)]
	pub args: crate::put::Args,

	#[usage(subcommand)]
	pub command: Option<Command>,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	#[usage(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	Get(self::get::Args),
	#[usage(alias = "add")]
	Put(self::put::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command.unwrap_or(Command::Put(args.args)) {
		Command::Delete(args) => {
			crate::delete::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::run(cli, args).await?;
		},
	}
	Ok(())
}

impl Tag {
	#[must_use]
	pub fn ancestors(&self) -> tg::node::Ancestors {
		let create = self
			.create
			.or(self.no_create.map(|value| !value))
			.unwrap_or(false);
		let pull = self
			.pull
			.or(self.no_pull.map(|value| {
				if value {
					tg::node::AncestorsPull::Never
				} else {
					tg::node::AncestorsPull::Missing
				}
			}))
			.unwrap_or_default();

		tg::node::Ancestors { create, pull }
	}
}
