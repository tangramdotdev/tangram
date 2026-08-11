use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// List nodes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub entries: Entries,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub parent: Option<tg::grant::Resource>,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub recursive: bool,

	#[arg(long)]
	pub reverse: bool,

	#[command(flatten)]
	pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Entries {
	#[arg(
		default_missing_value = "true",
		id = "list.entries.groups",
		long = "groups",
		num_args = 0..=1,
		overrides_with = "list.entries.no_groups",
		require_equals = true,
	)]
	groups: Option<bool>,

	#[arg(
		id = "list.entries.no_groups",
		long = "no-groups",
		overrides_with = "list.entries.groups"
	)]
	no_groups: bool,

	#[arg(
		default_missing_value = "true",
		id = "list.entries.organizations",
		long = "organizations",
		num_args = 0..=1,
		overrides_with = "list.entries.no_organizations",
		require_equals = true,
	)]
	organizations: Option<bool>,

	#[arg(
		id = "list.entries.no_organizations",
		long = "no-organizations",
		overrides_with = "list.entries.organizations"
	)]
	no_organizations: bool,

	#[arg(
		default_missing_value = "true",
		id = "list.entries.tags",
		long = "tags",
		num_args = 0..=1,
		overrides_with = "list.entries.no_tags",
		require_equals = true,
	)]
	tags: Option<bool>,

	#[arg(
		id = "list.entries.no_tags",
		long = "no-tags",
		overrides_with = "list.entries.tags"
	)]
	no_tags: bool,

	#[arg(
		default_missing_value = "true",
		id = "list.entries.users",
		long = "users",
		num_args = 0..=1,
		overrides_with = "list.entries.no_users",
		require_equals = true,
	)]
	users: Option<bool>,

	#[arg(
		id = "list.entries.no_users",
		long = "no-users",
		overrides_with = "list.entries.users"
	)]
	no_users: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(id = "list.ttl.ttl", long = "ttl", overrides_with = "list.ttl.no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(
		id = "list.ttl.no_ttl",
		long = "no-ttl",
		overrides_with = "list.ttl.ttl"
	)]
	pub no_ttl: bool,
}

impl Entries {
	pub(crate) fn groups(&self) -> bool {
		if self.no_groups {
			false
		} else {
			self.groups.unwrap_or(true)
		}
	}

	pub(crate) fn organizations(&self) -> bool {
		if self.no_organizations {
			false
		} else {
			self.organizations.unwrap_or(true)
		}
	}

	pub(crate) fn tags(&self) -> bool {
		if self.no_tags {
			false
		} else {
			self.tags.unwrap_or(true)
		}
	}

	pub(crate) fn users(&self) -> bool {
		if self.no_users {
			false
		} else {
			self.users.unwrap_or(true)
		}
	}
}

impl Ttl {
	pub(crate) fn get(&self) -> tg::remote::cache::Ttl {
		if self.no_ttl {
			tg::remote::cache::Ttl::Infinite
		} else {
			self.ttl
				.map(tg::remote::cache::Ttl::Duration)
				.unwrap_or_default()
		}
	}
}

impl Cli {
	pub async fn command_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::list::Arg {
			cached: args.cached,
			length: None,
			location: args.locations.get(),
			groups: args.entries.groups(),
			organizations: args.entries.organizations(),
			parent: args.parent.clone(),
			recursive: args.recursive,
			reverse: args.reverse,
			tags: args.entries.tags(),
			ttl: args.ttl.get(),
			users: args.entries.users(),
		};
		let output = client
			.list(arg)
			.await
			.map_err(|error| tg::error!(!error, parent = ?args.parent, "failed to list entries"))?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
