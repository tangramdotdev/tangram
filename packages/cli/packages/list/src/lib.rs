use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List nodes.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Fetch all pages.
	#[usage(long)]
	pub all: bool,

	/// Only use cached remote results. Do not fetch from remotes.
	#[usage(long)]
	pub cached: bool,

	/// Continue from this cursor.
	#[usage(long)]
	pub cursor: Option<String>,

	#[usage(flatten)]
	pub entries: Entries,

	/// The maximum number of entries per page (default: 100, maximum: 1000).
	#[usage(long)]
	pub limit: Option<u64>,

	#[usage(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub output: tangram_cli_core::print::OutputOptions,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(long)]
	pub recursive: bool,

	pub reference: Option<tg::Reference>,

	#[usage(long)]
	pub reverse: bool,

	#[usage(flatten)]
	pub time_to_live: TimeToLive,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Entries {
	#[usage(
		default_missing = "true",
		long = "groups",
		overrides("--no-groups"),
		require_equals = true
	)]
	groups: Option<bool>,

	#[usage(long = "no-groups", overrides("--groups"))]
	no_groups: bool,

	#[usage(
		default_missing = "true",
		long = "organizations",
		overrides("--no-organizations"),
		require_equals = true
	)]
	organizations: Option<bool>,

	#[usage(long = "no-organizations", overrides("--organizations"))]
	no_organizations: bool,

	#[usage(
		default_missing = "true",
		long = "tags",
		overrides("--no-tags"),
		require_equals = true
	)]
	tags: Option<bool>,

	#[usage(long = "no-tags", overrides("--tags"))]
	no_tags: bool,

	#[usage(
		default_missing = "true",
		long = "users",
		overrides("--no-users"),
		require_equals = true
	)]
	users: Option<bool>,

	#[usage(long = "no-users", overrides("--users"))]
	no_users: bool,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TimeToLive {
	#[usage(
		alias = "no-ttl",
		long = "no-time-to-live",
		overrides("--time-to-live")
	)]
	pub no_time_to_live: bool,

	#[usage(alias = "ttl", long = "time-to-live", overrides("--no-time-to-live"))]
	pub time_to_live: Option<humantime::Duration>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the node.
	let node = if let Some(reference) = &args.reference {
		let reference = args.locations.apply_to_reference(reference);
		let arg = tg::get::Arg {
			cached: args.cached,
			time_to_live: args.time_to_live.get(),
			..tg::get::Arg::default()
		};
		let output = tangram_cli_reference::get_with_arg(cli, &reference, arg).await?;
		let node = output.referent.try_map(|node| match node {
			tg::get::Node::Id(id) => Ok(id),
			tg::get::Node::Pointer(_) => Err(tg::error!(%reference, "the list node must be an ID")),
		})?;
		Some(node)
	} else {
		None
	};

	// List the entries.
	let client = cli.client().await?;
	let location = node
		.as_ref()
		.and_then(|node| node.options.location.clone())
		.map(Into::into)
		.or_else(|| args.locations.get());
	let arg = tg::list::Arg {
		cached: args.cached,
		cursor: args.cursor,
		groups: args.entries.groups(),
		limit: args.limit,
		location,
		node,
		organizations: args.entries.organizations(),
		recursive: args.recursive,
		reverse: args.reverse,
		tags: args.entries.tags(),
		time_to_live: args.time_to_live.get(),
		users: args.entries.users(),
	};
	let output = if args.all {
		client.list_all(arg).await
	} else {
		client.list(arg).await
	}
	.map_err(|error| tg::error!(!error, reference = ?args.reference, "failed to list entries"))?;
	if args.output.verbose {
		cli.print_serde(output, args.print).await?;
	} else {
		cli.print_serde(output.data, args.print).await?;
		if let Some(cursor) = output.cursor {
			cli.print_info_message(&format!("Next cursor: {cursor}"));
		}
	}
	Ok(())
}

impl Entries {
	#[must_use]
	pub fn groups(&self) -> bool {
		if self.no_groups {
			false
		} else {
			self.groups.unwrap_or(true)
		}
	}

	#[must_use]
	pub fn organizations(&self) -> bool {
		if self.no_organizations {
			false
		} else {
			self.organizations.unwrap_or(true)
		}
	}

	#[must_use]
	pub fn tags(&self) -> bool {
		if self.no_tags {
			false
		} else {
			self.tags.unwrap_or(true)
		}
	}

	#[must_use]
	pub fn users(&self) -> bool {
		if self.no_users {
			false
		} else {
			self.users.unwrap_or(true)
		}
	}
}

impl TimeToLive {
	#[must_use]
	pub fn get(&self) -> tg::remote::cache::TimeToLive {
		if self.no_time_to_live {
			tg::remote::cache::TimeToLive::Infinite
		} else {
			self.time_to_live
				.map_or_default(|duration| tg::remote::cache::TimeToLive::Duration(duration.into()))
		}
	}
}
