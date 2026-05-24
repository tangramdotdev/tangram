use {crate::Cli, tangram_client::prelude::*};

/// Add a namespace grant.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub all: bool,

	#[arg(long)]
	pub group: Option<String>,

	#[arg(long)]
	pub namespace: tg::Namespace,

	#[command(flatten)]
	pub permission: crate::grant::Permission,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(long)]
	pub user: Option<String>,
}

impl Cli {
	pub async fn command_namespace_grants_add(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let permission = args.permission.get()?;
		let grant = client
			.create_namespace_grant(tg::namespace::grants::create::Arg {
				all: args.all,
				group: args.group,
				namespace: args.namespace.clone(),
				permission,
				user: args.user,
			})
			.await
			.map_err(
				|error| tg::error!(!error, namespace = %args.namespace, "failed to add the namespace grant"),
			)?;
		self.print_serde(grant, args.print).await?;
		Ok(())
	}
}
