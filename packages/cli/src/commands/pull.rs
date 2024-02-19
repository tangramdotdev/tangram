use crate::{util::build_or_object_id, Cli};
use either::Either;
use tangram_client as tg;
use tangram_error::Result;

/// Pull a build or an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[clap(value_parser = build_or_object_id)]
	pub id: Either<tg::build::Id, tg::object::Id>,
}

impl Cli {
	pub async fn command_pull(&self, args: Args) -> Result<()> {
		match args.id {
			Either::Left(id) => {
				self.command_build_pull(super::build::PullArgs { id })
					.await?;
			},
			Either::Right(id) => {
				self.command_object_pull(super::object::PullArgs { id })
					.await?;
			},
		}
		Ok(())
	}
}
