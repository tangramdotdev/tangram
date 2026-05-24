use {crate::Cli, tangram_client::prelude::*};

/// Delete a tag grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub all: bool,

	#[arg(long)]
	pub group: Option<String>,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[arg(long)]
	pub tag: tg::Tag,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub async fn command_tag_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		client
			.delete_tag_grant(tg::tag::grants::delete::Arg {
				all: args.all,
				group: args.group,
				permission,
				tag: args.tag.clone(),
				user: args.user,
			})
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to delete the tag grant"))?
			.ok_or_else(|| tg::error!("failed to find the tag grant"))?;
		Ok(())
	}
}
