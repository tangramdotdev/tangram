use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Grant permissions to a subject on a resource.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Create a new grant instead of adding to the creator's existing grant.
	#[usage(long)]
	pub create: bool,

	pub subject: tg::authorization::subject::Selector,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	pub permissions: tg::Either<tg::authorization::permission::Set, String>,

	pub resource: tg::Referent<tg::Selector<tg::Id>>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::grant::grant::Arg {
		create: args.create,
		permissions: args.permissions.clone(),
		resource: args.resource.clone(),
		subject: args.subject.clone(),
	};
	let output = client
		.grant(arg)
		.await
		.map_err(
			|error| tg::error!(!error, resource = %args.resource, subject = %args.subject, "failed to create the grant"),
		)?;
	cli.print_serde(output.data, args.print).await?;
	Ok(())
}
