use crate::Cli;
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
	pub recursive: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let (referent, _) = self.get_reference(&args.reference).await?;
		eprintln!("{} item {}", "info".blue().bold(), referent.item);
		if let Some(path) = &referent.path {
			let path = path.display();
			eprintln!("{} path {path}", "info".blue().bold());
		}
		if let Some(subpath) = &referent.subpath {
			let path = subpath.display();
			eprintln!("{} subpath {path}", "info".blue().bold());
		}
		if let Some(tag) = &referent.tag {
			eprintln!("{} tag {tag}", "info".blue().bold());
		}
		let item = match referent.item.clone() {
			Either::Left(build) => Either::Left(build),
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
		let item = match item {
			Either::Left(build) => Either::Left(build.id().clone()),
			Either::Right(object) => Either::Right(object.id(&handle).await?.clone()),
		};
		let Args {
			format,
			pretty,
			recursive,
			..
		} = args;
		match item {
			Either::Left(build) => {
				self.command_build_get(crate::build::get::Args { build, pretty })
					.await?;
			},
			Either::Right(object) => {
				self.command_object_get(crate::object::get::Args {
					format,
					object,
					pretty,
					recursive,
				})
				.await?;
			},
		}
		Ok(())
	}
}
