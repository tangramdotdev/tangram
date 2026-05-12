use {crate::Cli, tangram_client::prelude::*};

/// List effective namespace permissions for a user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub user: String,

	#[arg(index = 2)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_permissions(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_user_namespace_permissions(
				&args.user,
				tg::user::permissions::Arg {
					namespace: args.namespace.clone(),
				},
			)
			.await
			.map_err(
				|error| tg::error!(!error, user = %args.user, namespace = %args.namespace, "failed to list the namespace permissions"),
			)?
			.ok_or_else(|| tg::error!(user = %args.user, "failed to find the user"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
