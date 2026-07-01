use {crate::Cli, tangram_client::prelude::*};

/// Log in.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub email: Option<String>,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub name: Option<tg::Specifier>,

	/// Print the token and user.
	#[arg(long)]
	pub verbose: bool,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_login(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

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
			self.print_info_message(&format!("open {url}"));
			let result = webbrowser::open(url);
			if let Err(error) = result {
				tracing::debug!(%error, "failed to open the browser");
			}
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
				.user
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			self.write_token(output.token.clone())?;
		}

		// Print the output.
		if args.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.user, args.print).await?;
		}

		Ok(())
	}
}
