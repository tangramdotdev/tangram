use {crate::Cli, tangram_client::prelude::*};

/// Resolve a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the metadata.
	#[arg(long)]
	pub metadata: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Get the storage status.
	#[arg(long)]
	pub stored: bool,

	#[command(flatten)]
	pub ttl: crate::get::Ttl,
}

impl Cli {
	pub async fn command_resolve(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::get::Args {
			bytes: args.bytes,
			cached: args.cached,
			locations: args.locations,
			metadata: args.metadata,
			print: args.print,
			reference: args.reference,
			stored: args.stored,
			ttl: args.ttl,
		};
		let reference = args.locations.apply_to_reference(&args.reference);
		let arg = tg::resolve::Arg {
			cached: args.cached,
			ttl: args.ttl.get(),
			..Default::default()
		};
		let referent = self.resolve_with_arg(&reference, arg).await?;

		self.print_get_output(args, referent).await
	}
}
