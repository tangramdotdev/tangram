use crate::Cli;
use crossterm::style::Stylize as _;
use tangram_client as tg;
use tangram_either::Either;

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub reference: tg::Reference,
	#[arg(long)]
	pub format: Option<String>,
	#[arg(long, requires = "format", default_value = "false")]
	pub pretty: bool,
	#[arg(long, requires = "format", default_value = "false")]
	pub recursive: bool,
}

impl Cli {
	pub async fn command_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let referent = self.get_reference(&args.reference).await?;
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
		if args.format.as_deref() == Some("tgvn") {
			let pretty = args.pretty;
			let recursive = args.recursive;
			match item {
				Either::Left(build) => {
					tg::error!("expected an object")
				},
				Either::Right(object) => {
					let options = tg::value::print::Options {
						recursive: true,
						style: if pretty {
							tg::value::print::Style::Pretty { indentation: "  " }
						} else {
							tg::value::print::Style::Compact
						},
					};
					if recursive {
						object.load_recursive(&handle).await?;
					} else {
						object.load(&handle).await?;
					}
					let value = tg::Value::from(object);
					print!("{}", value.print(options));
				},
			}
			return Ok(());
		}
		let item = match item {
			Either::Left(build) => Either::Left(build.id().clone()),
			Either::Right(object) => Either::Right(object.id(&handle).await?.clone()),
		};
		eprintln!("{} id {item}", "info".blue().bold());
		eprintln!("{} item {}", "info".blue().bold(), referent.item);
		if let Some(path) = referent.path {
			let path = path.display();
			eprintln!("{} path {path}", "info".blue().bold());
		}
		if let Some(subpath) = referent.subpath {
			let path = subpath.display();
			eprintln!("{} subpath {path}", "info".blue().bold());
		}
		if let Some(tag) = referent.tag {
			eprintln!("{} tag {tag}", "info".blue().bold());
		}
		match item {
			Either::Left(build) => {
				self.command_build_get(crate::build::get::Args { build })
					.await?;
			},
			Either::Right(object) => {
				self.command_object_get(crate::object::get::Args { object })
					.await?;
			},
		}
		Ok(())
	}
}
