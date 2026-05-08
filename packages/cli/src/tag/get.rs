use {crate::Cli, std::time::Duration, tangram_client::prelude::*};

/// Get the latest tag matching a pattern.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub pattern: tg::tag::Pattern,

	#[command(flatten)]
	pub print: crate::print::Options,

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
	pub async fn command_tag_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::tag::list::Arg {
			cached: args.cached,
			length: Some(1),
			location: args.locations.get(),
			pattern: args.pattern.clone(),
			recursive: false,
			reverse: true,
			ttl: args.ttl.get(),
		};
		let output = client.list_tags(arg).await.map_err(
			|error| tg::error!(!error, pattern = %args.pattern, "failed to get the tag"),
		)?;
		let entry = output
			.data
			.into_iter()
			.next()
			.ok_or_else(|| tg::error!(pattern = %args.pattern, "no tag was found"))?;
		self.print_serde(entry, args.print).await?;
		Ok(())
	}
}
