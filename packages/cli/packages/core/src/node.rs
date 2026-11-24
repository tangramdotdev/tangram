use {tangram_client::prelude::*, usage_rs as usage};

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Ancestors {
	/// Create missing ancestors after pulling.
	#[usage(
		default_missing = "true",
		long = "create-ancestors",
		overrides("--no-create-ancestors"),
		require_equals = true,
		short = 'p',
		value_name = "BOOL",
		visible_alias = "create-parents"
	)]
	create: Option<bool>,

	/// Do not create missing ancestors.
	#[usage(
		default_missing = "true",
		long = "no-create-ancestors",
		overrides("--create-ancestors"),
		require_equals = true,
		value_name = "BOOL",
		visible_alias = "no-create-parents"
	)]
	no_create: Option<bool>,

	/// Do not pull ancestors.
	#[usage(
		default_missing = "true",
		long = "no-pull-ancestors",
		overrides("--pull-ancestors"),
		require_equals = true,
		value_name = "BOOL",
		visible_alias = "no-pull-parents"
	)]
	no_pull: Option<bool>,

	/// Pull ancestors using the specified policy: always, missing, or never.
	#[usage(
		default_missing = "always",
		long = "pull-ancestors",
		overrides("--no-pull-ancestors"),
		require_equals = true,
		value_name = "POLICY",
		visible_alias = "pull-parents"
	)]
	pull: Option<tg::node::AncestorsPull>,
}

#[derive(Clone, Debug, Default, usage::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	/// Transfer ancestors using the specified policy: always, missing, or never.
	#[usage(
		default_missing = "always",
		long = "ancestors",
		overrides("--no-ancestors"),
		require_equals = true,
		value_name = "POLICY"
	)]
	ancestors: Option<tg::node::AncestorsPull>,

	/// Do not transfer ancestors.
	#[usage(
		default_missing = "true",
		long = "no-ancestors",
		overrides("--ancestors"),
		require_equals = true,
		value_name = "BOOL"
	)]
	no_ancestors: Option<bool>,
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
