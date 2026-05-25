use {crate::Cli, tangram_client::prelude::*};

/// Add a tag grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[command(flatten)]
	pub principal: crate::grant::Principal,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub tag: tg::Tag,
}

impl Cli {
	pub async fn command_tag_grants_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		let principal = args.principal.resolve(&client).await?;
		let grant = client
			.create_tag_grant(tg::tag::grants::create::Arg {
				location: args.location.get(),
				permission,
				principal,
				tag: args.tag.clone(),
			})
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to add the tag grant"))?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
