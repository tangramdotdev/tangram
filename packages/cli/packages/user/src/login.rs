use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Log in.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long)]
	pub email: Option<String>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(hide = true, long, short)]
	pub name: Option<tg::Specifier>,

	/// Print the token and user.
	#[usage(long)]
	pub verbose: bool,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	let location = args.location.to_location()?;

	// Start the login.
	let login = client
		.create_login(tg::user::login::create::Arg {
			email: args.email,
			location: location.clone().map(Into::into),
			name: args.name.clone(),
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to start the login"))?;

	// Open the URL.
	if let Some(url) = &login.url {
		cli.open_url(url);
	}

	// Await the login.
	let output = client
		.wait_login(tg::user::login::wait::Arg {
			code: login.code,
			location: location.clone().map(Into::into),
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to complete the login"))?;

	// Write the token if necessary.
	if !location.as_ref().is_some_and(tg::Location::is_remote)
		&& !output
			.location
			.as_ref()
			.is_some_and(tg::Location::is_remote)
	{
		cli.write_token(output.token.clone())?;
	}

	// Print the output.
	if args.verbose {
		cli.print_serde(output, args.print).await?;
	} else {
		cli.print_serde(output.user, args.print).await?;
	}

	Ok(())
}
