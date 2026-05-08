use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// List namespaces and tags.
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

	#[arg(default_value = "*", index = 1)]
	pub pattern: tg::list::Pattern,

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
		long,
		num_args = 0..=1,
		overrides_with = "no_namespaces",
		require_equals = true,
	)]
	namespaces: Option<bool>,

	#[arg(long, overrides_with = "namespaces")]
	no_namespaces: bool,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_tags",
		require_equals = true,
	)]
	tags: Option<bool>,

	#[arg(long, overrides_with = "tags")]
	no_tags: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(long, overrides_with = "no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(long, overrides_with = "ttl")]
	pub no_ttl: bool,
}

impl Entries {
	fn namespaces(&self) -> bool {
		if self.no_namespaces {
			false
		} else {
			self.namespaces.unwrap_or(true)
		}
	}

	fn tags(&self) -> bool {
		if self.no_tags {
			false
		} else {
			self.tags.unwrap_or(true)
		}
	}
}

impl Ttl {
	fn get(&self) -> Option<Duration> {
		if self.no_ttl { None } else { self.ttl }
	}
}

impl Cli {
	pub async fn command_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::list::Arg {
			cached: args.cached,
			length: None,
			location: args.locations.get(),
			namespaces: args.entries.namespaces(),
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			reverse: args.reverse,
			tags: args.entries.tags(),
			ttl: args.ttl.get(),
		};
		let output = client.list(arg).await.map_err(
			|error| tg::error!(!error, pattern = %args.pattern, "failed to list entries"),
		)?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
