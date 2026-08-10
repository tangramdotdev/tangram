use tangram_client::prelude::*;

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ancestors {
	/// Create missing ancestors after pulling.
	#[arg(
		default_missing_value = "true",
		id = "ancestors.create_ancestors",
		long = "create-ancestors",
		num_args = 0..=1,
		overrides_with = "ancestors.no_create_ancestors",
		require_equals = true,
		short = 'p',
		value_name = "BOOL",
		visible_alias = "create-parents",
	)]
	create: Option<bool>,

	/// Do not create missing ancestors.
	#[arg(
		default_missing_value = "true",
		id = "ancestors.no_create_ancestors",
		long = "no-create-ancestors",
		num_args = 0..=1,
		overrides_with = "ancestors.create_ancestors",
		require_equals = true,
		value_name = "BOOL",
		visible_alias = "no-create-parents",
	)]
	no_create: Option<bool>,

	/// Do not pull ancestors.
	#[arg(
		default_missing_value = "true",
		id = "ancestors.no_pull_ancestors",
		long = "no-pull-ancestors",
		num_args = 0..=1,
		overrides_with = "ancestors.pull_ancestors",
		require_equals = true,
		value_name = "BOOL",
		visible_alias = "no-pull-parents",
	)]
	no_pull: Option<bool>,

	/// Pull ancestors using the specified policy: always, missing, or never.
	#[arg(
		default_missing_value = "always",
		id = "ancestors.pull_ancestors",
		long = "pull-ancestors",
		num_args = 0..=1,
		overrides_with = "ancestors.no_pull_ancestors",
		require_equals = true,
		value_name = "POLICY",
		visible_alias = "pull-parents",
	)]
	pull: Option<tg::node::AncestorsPull>,
}

impl Ancestors {
	#[must_use]
	pub fn get(&self) -> tg::node::Ancestors {
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

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Options {
	/// Transfer ancestors using the specified policy: always, missing, or never.
	#[arg(
		default_missing_value = "always",
		id = "ancestor_options.ancestors",
		long = "ancestors",
		num_args = 0..=1,
		overrides_with = "ancestor_options.no_ancestors",
		require_equals = true,
		value_name = "POLICY",
	)]
	ancestors: Option<tg::node::AncestorsPull>,

	/// Do not transfer ancestors.
	#[arg(
		default_missing_value = "true",
		id = "ancestor_options.no_ancestors",
		long = "no-ancestors",
		num_args = 0..=1,
		overrides_with = "ancestor_options.ancestors",
		require_equals = true,
		value_name = "BOOL",
	)]
	no_ancestors: Option<bool>,
}

impl Options {
	#[must_use]
	pub fn get(&self) -> tg::node::AncestorsPull {
		self.ancestors
			.or(self.no_ancestors.map(|value| {
				if value {
					tg::node::AncestorsPull::Never
				} else {
					tg::node::AncestorsPull::Missing
				}
			}))
			.unwrap_or_default()
	}
}
