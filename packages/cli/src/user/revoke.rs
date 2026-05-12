use {crate::Cli, tangram_client::prelude::*};

/// Revoke a user permission on a namespace.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub user: String,

	#[arg(index = 2)]
	pub namespace: tg::Namespace,

	#[arg(index = 3)]
	pub permission: tg::Permission,
}

impl Cli {
	pub async fn command_user_revoke(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		client
			.revoke_user_namespace_permission(
				&args.user,
				tg::user::revoke::Arg {
					namespace: args.namespace.clone(),
					permission: args.permission,
				},
			)
			.await
			.map_err(|error| tg::error!(!error, user = %args.user, namespace = %args.namespace, "failed to revoke the namespace permission"))?
			.ok_or_else(|| tg::error!("failed to find the namespace grant"))?;
		Ok(())
	}
}
