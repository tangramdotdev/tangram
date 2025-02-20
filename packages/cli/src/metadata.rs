use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Get process or object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_metadata(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(process) => Either::Left(process),
			Either::Right(object) => {
				let object = if let Some(subpath) = &referent.subpath {
					let directory = object
						.try_unwrap_directory()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?;
					directory.get(&handle, subpath).await?.into()
				} else {
					object
				};
				Either::Right(object)
			},
		};

		match item {
			Either::Left(process) => {
				let args = crate::process::metadata::Args {
					process,
					pretty: args.pretty,
				};
				self.command_process_metadata(args).await?;
			},
			Either::Right(object) => {
				let args = crate::object::metadata::Args {
					object,
					pretty: args.pretty,
				};
				self.command_object_metadata(args).await?;
			},
		}

		Ok(())
	}
}
