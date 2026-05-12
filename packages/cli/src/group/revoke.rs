use {crate::Cli, tangram_client::prelude::*};

/// Revoke a group permission on a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: String,

	#[arg(index = 2)]
	pub namespace: tg::Namespace,

	#[arg(index = 3)]
	pub permission: tg::Permission,
}

impl Cli {
	pub async fn command_group_revoke(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.revoke_group_namespace_permission(
				&args.group,
				tg::group::revoke::Arg {
					namespace: args.namespace.clone(),
					permission: args.permission,
				},
			)
			.await
			.map_err(|error| tg::error!(!error, group = %args.group, namespace = %args.namespace, "failed to revoke the namespace permission"))?
			.ok_or_else(|| tg::error!("failed to find the namespace grant"))?;
		Ok(())
	}
}
