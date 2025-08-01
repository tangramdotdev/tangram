use crate::Cli;
use anstream::eprintln;
use crossterm::style::Stylize as _;
use tangram_client as tg;
use tangram_either::Either;

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<crate::object::get::Format>,

	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(short, long, default_value = "1")]
	pub depth: crate::object::get::Depth,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		let referent = referent.map(|item| match item {
			Either::Left(process) => Either::Left(process.id().clone()),
			Either::Right(object) => Either::Right(object.id().clone()),
		});
		eprintln!("{} {referent}", "info".blue().bold());
		match referent.item {
			Either::Left(process) => {
				let args = crate::process::get::Args {
					process,
					pretty: args.pretty,
				};
				self.command_process_get(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::get::Args {
					object,
					depth: args.depth,
					format: args.format,
					pretty: args.pretty,
					blobs: false,
				};
				self.command_object_get(args).await?;
			},
		}
		Ok(())
	}
}
