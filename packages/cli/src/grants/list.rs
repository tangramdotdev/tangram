use {crate::Cli, tangram_client::prelude::*};

/// List grants.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	/// List the grants held by this principal.
	#[arg(conflicts_with = "resource", long)]
	pub principal: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// List the grants on this resource.
	#[arg(long)]
	pub resource: Option<tg::grant::Resource>,
}

impl Cli {
	pub async fn command_grants_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let principal = args.principal.is_some();
		let arg = tg::grant::list::Arg {
			location: args.location.get(),
			principal: args.principal,
			resource: args.resource,
		};
		let output = client
			.list_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the grants"))?
			.ok_or_else(|| {
				if principal {
					tg::error!("failed to find the principal")
				} else {
					tg::error!("failed to find the resource")
				}
			})?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
