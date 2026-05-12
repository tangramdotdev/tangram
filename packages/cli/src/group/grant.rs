use {crate::Cli, tangram_client::prelude::*};

/// Grant a group permission on a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: String,

	#[arg(index = 2)]
	pub namespace: tg::Namespace,

	#[arg(index = 3)]
	pub permission: tg::Permission,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_grant(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let grant = client
			.grant_group_namespace_permission(
				&args.group,
				tg::group::grant::Arg {
					namespace: args.namespace.clone(),
					permission: args.permission,
				},
			)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, namespace = %args.namespace, "failed to grant the namespace permission"))?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
