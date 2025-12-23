use {crate::Cli, tangram_client::prelude::*};

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_metadata(&mut self, args: Args) -> tg::Result<()> {
		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = referent
			.item
			.map_left(|object| object.id().clone())
			.map_right(|process| process.id().clone());

		match item {
			tg::Either::Left(object) => {
				let args = crate::object::metadata::Args {
					local: args.local,
					object,
					print: args.print,
					remotes: args.remotes,
				};
				self.command_object_metadata(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::metadata::Args {
					local: args.local,
					print: args.print,
					process,
					remotes: args.remotes,
				};
				self.command_process_metadata(args).await?;
			},
		}

		Ok(())
	}
}
