use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// List tags.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(default_value = "*", index = 1)]
	pub pattern: tg::tag::Pattern,

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
pub struct Ttl {
	#[arg(long, overrides_with = "no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(long, overrides_with = "ttl")]
	pub no_ttl: bool,
}

impl Ttl {
	fn get(&self) -> Option<Duration> {
		if self.no_ttl { None } else { self.ttl }
	}
}

impl Cli {
	pub async fn command_tag_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::list::Arg {
			cached: args.cached,
			length: None,
			location: args.locations.get(),
			pattern: args.pattern.clone(),
			recursive: args.recursive,
			reverse: args.reverse,
			ttl: args.ttl.get(),
		};
		let output = client.list_tags(arg).await.map_err(
			|error| tg::error!(!error, pattern = %args.pattern, "failed to list the tags"),
		)?;
		self.print_serde(output.data, args.print).await?;
		Ok(())
	}
}
