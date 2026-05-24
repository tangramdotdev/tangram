use {crate::Cli, tangram_client::prelude::*};

/// Delete a tag grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[command(flatten)]
	pub principal: crate::grant::Principal,

	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_grants_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		client
			.delete_tag_grant(tg::tag::grants::delete::Arg {
				all: args.principal.all,
				group: args.principal.group,
				location: args.location.get(),
				permission,
				tag: args.tag.clone(),
				user: args.principal.user,
			})
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to delete the tag grant"))?
			.ok_or_else(|| tg::error!("failed to find the tag grant"))?;
		Ok(())
	}
}
