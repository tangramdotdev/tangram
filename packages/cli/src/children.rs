use {crate::Cli, tangram_client::prelude::*};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The object or process.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			tg::Either::Left(object) => {
				let args = crate::object::children::Args {
					local: args.local,
					object: object.id(),
					print: args.print,
					remotes: args.remotes,
				};
				self.command_object_children(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::children::Args {
					length: None,
					local: args.local,
					position: None,
					print: args.print,
					process: process.id().clone(),
					remotes: args.remotes,
					size: None,
				};
				self.command_process_children(args).await?;
			},
		}
		Ok(())
	}
}
