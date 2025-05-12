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

	#[arg(long)]
	pub depth: Option<u64>,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let referent = self.get_reference(&args.reference).await?;
		let item = match &referent.item {
			Either::Left(process) => process.id().to_string(),
			Either::Right(item) => item.to_string(),
		};
		eprintln!("{} {item}", "info".blue().bold());
		if let Some(path) = &referent.path {
			let path = path.display();
			eprintln!("{} path {path}", "info".blue().bold());
		}
		if let Some(tag) = &referent.tag {
			eprintln!("{} tag {tag}", "info".blue().bold());
		}
		let item = match referent.item {
			Either::Left(process) => Either::Left(process.id().clone()),
			Either::Right(object) => Either::Right(object.id().clone()),
		};
		let Args {
			format,
			pretty,
			depth,
			..
		} = args;
		match item {
			Either::Left(process) => {
				self.command_process_get(crate::process::get::Args { pretty, process })
					.await?;
			},
			Either::Right(object) => {
				self.command_object_get(crate::object::get::Args {
					format,
					object,
					pretty,
					depth,
				})
				.await?;
			},
		}
		Ok(())
	}
}
