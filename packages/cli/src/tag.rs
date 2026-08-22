use {crate::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod get;
pub mod put;

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Tag {
	/// Create the tag's missing ancestors after pulling.
	#[arg(
		default_missing_value = "true",
		id = "tag.create_tag_ancestors",
		long = "create-tag-ancestors",
		num_args = 0..=1,
		overrides_with = "tag.no_create_tag_ancestors",
		require_equals = true,
		requires = "tag.specifier",
		value_name = "BOOL",
		visible_alias = "create-tag-parents",
	)]
	create: Option<bool>,

	/// Replace a conflicting named node and its descendants when setting the tag.
	#[arg(id = "tag.force", long = "force-tag", requires = "tag.specifier")]
	pub force: bool,

	/// Do not create the tag's missing ancestors.
	#[arg(
		default_missing_value = "true",
		id = "tag.no_create_tag_ancestors",
		long = "no-create-tag-ancestors",
		num_args = 0..=1,
		overrides_with = "tag.create_tag_ancestors",
		require_equals = true,
		requires = "tag.specifier",
		value_name = "BOOL",
		visible_alias = "no-create-tag-parents",
	)]
	no_create: Option<bool>,

	/// Do not pull the tag's ancestors.
	#[arg(
		default_missing_value = "true",
		id = "tag.no_pull_tag_ancestors",
		long = "no-pull-tag-ancestors",
		num_args = 0..=1,
		overrides_with = "tag.pull_tag_ancestors",
		require_equals = true,
		requires = "tag.specifier",
		value_name = "BOOL",
		visible_alias = "no-pull-tag-parents",
	)]
	no_pull: Option<bool>,

	/// Pull the tag's ancestors using the specified policy: always, missing, or never.
	#[arg(
		default_missing_value = "always",
		id = "tag.pull_tag_ancestors",
		long = "pull-tag-ancestors",
		num_args = 0..=1,
		overrides_with = "tag.no_pull_tag_ancestors",
		require_equals = true,
		requires = "tag.specifier",
		value_name = "POLICY",
		visible_alias = "pull-tag-parents",
	)]
	pull: Option<tg::node::AncestorsPull>,

	/// Tag the result.
	#[arg(id = "tag.specifier", long = "tag")]
	pub specifier: Option<tg::Specifier>,
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

/// Manage tags.
#[derive(Clone, Debug, clap::Args)]
#[command(
	args_conflicts_with_subcommands = true,
	subcommand_negates_reqs = true,
	subcommand_precedence_over_arg = true
)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub args: crate::tag::put::Args,

	#[command(subcommand)]
	pub command: Option<Command>,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	Get(self::get::Args),
	#[command(alias = "add")]
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_tag(&mut self, args: Args) -> tg::Result<()> {
		match args.command.unwrap_or(Command::Put(args.args)) {
			Command::Delete(args) => {
				self.command_tag_delete(args).await?;
			},
			Command::Get(args) => {
				self.command_tag_get(args).await?;
			},
			Command::Put(args) => {
				self.command_tag_put(args).await?;
			},
		}
		Ok(())
	}
}
