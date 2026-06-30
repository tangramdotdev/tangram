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
		let location = args.location.to_location()?;
		let client = self.client().await?;
		let output = if self
			.config
			.as_ref()
			.and_then(|config| config.server.authentication.as_ref())
			.is_some()
		{
			let client_id = "tangram_cli".to_owned();
			let session = client
				.create_oauth_device_session(tg::oauth::device_session::Arg {
					client_id: client_id.clone(),
					name: args.name.clone(),
					scope: None,
				})
				.await
				.map_err(|error| tg::error!(!error, "failed to start the login"))?;
			if let Err(error) = webbrowser::open(&session.verification_uri_complete) {
				tracing::debug!(%error, "failed to open the browser");
			}
			eprintln!(
				"Open {} and enter code {}.",
				session.verification_uri, session.user_code
			);
			let token = tg::oauth::poll_device_access_token(
				&client,
				session.device_code,
				client_id,
				std::time::Duration::from_secs(session.interval),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to complete the login"))?;
			self.write_token(token.access_token.clone())?;
			self.client = None;
			let client = self.client().await?;
			tg::user::login::Output {
				token: token.access_token,
				user: client
					.get_current_user(tg::user::current::Arg {
						location: location.clone().map(Into::into),
					})
					.await?
					.ok_or_else(|| tg::error!("failed to get the current user"))?,
			}
		} else {
			let name = args
				.name
				.ok_or_else(|| tg::error!("invalid user specifier"))?;
			let arg = tg::user::login::Arg {
				name: Some(name),
				email: args.email,
				location: location.clone().map(Into::into),
				provider: None,
			};
			client
				.login(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to log in"))?
		};
		if !location.as_ref().is_some_and(tg::Location::is_remote)
			&& !output
				.user
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			self.write_token(output.token.clone())?;
		}
		if args.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.user, args.print).await?;
		}
		Ok(())
	}
}
