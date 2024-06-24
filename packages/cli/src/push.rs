use crate::Cli;
use either::Either;
use tangram_client as tg;

/// Push a build or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub targets: bool,
}

impl Cli {
	pub async fn command_push(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let item = args.reference.get(&client).await?;
		match item.clone() {
			Either::Left(build) => {
				self.command_build_push(crate::build::push::Args {
					build,
					logs: args.logs,
					recursive: args.recursive,
					remote: args.remote,
					targets: args.targets,
				})
				.await?;
			},
			Either::Right(object) => {
				self.command_object_push(crate::object::push::Args {
					object,
					remote: args.remote,
				})
				.await?;
			},
		}
		if let tg::Reference {
			path: tg::reference::Path::Tag(pattern),
			..
		} = args.reference
		{
			if let Ok(tag) = pattern.try_into() {
				let arg = tg::tag::put::Arg { force: false, item };
				client.put_tag(&tag, arg).await?;
			}
		}
		Ok(())
	}
}
