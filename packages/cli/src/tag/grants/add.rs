use {crate::Cli, tangram_client::prelude::*};

/// Add a tag grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub group: Option<String>,

	#[arg(index = 2)]
	pub permission: tg::Permission,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub public: bool,

	#[arg(index = 1)]
	pub tag: tg::Tag,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub async fn command_tag_grants_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let grant = client
			.create_tag_grant(tg::tag::grants::create::Arg {
				group: args.group,
				permission: args.permission,
				public: args.public,
				tag: args.tag.clone(),
				user: args.user,
			})
			.await
			.map_err(|error| tg::error!(!error, tag = %args.tag, "failed to add the tag grant"))?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
