use {crate::Cli, tangram_client::prelude::*, tangram_either::Either};

/// Get the children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	/// The object or process.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_children(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		match referent.item {
			Either::Left(object) => {
				let args = crate::object::children::Args {
					object: object.id(),
					print: args.print,
				};
				self.command_object_children(args).await?;
			},
			Either::Right(process) => {
				let args = crate::process::children::Args {
					length: None,
					position: None,
					print: args.print,
					process: process.id().clone(),
					remote: None,
					size: None,
				};
				self.command_process_children(args).await?;
			},
		}
		Ok(())
	}
}
