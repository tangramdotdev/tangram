use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Revoke permissions from the grant to a subject on a resource.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub subject: tg::authorization::subject::Selector,

	pub permissions: tg::Either<tg::authorization::permission::Set, String>,

	pub resource: tg::Referent<tg::Selector<tg::Id>>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::grant::revoke::Arg {
		permissions: args.permissions.clone(),
		resource: args.resource.clone(),
		subject: args.subject.clone(),
	};
	client.revoke(arg).await.map_err(
		|error| tg::error!(!error, resource = %args.resource, subject = %args.subject, "failed to revoke the grant"),
	)?;

	Ok(())
}
