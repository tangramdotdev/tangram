use crate::Cli;
use std::time::{Duration, Instant};
use tangram_error::{error, Result};

/// Log in to Tangram.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_login(&self, _args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Create a login.
		let login = client
			.create_login()
			.await
			.map_err(|error| error!(source = error, "failed to create the login"))?;

		// Open the browser to the login URL.
		webbrowser::open(login.url.as_ref()).map_err(|error| {
			error!(
				source = error,
				"failed to open the browser to the login URL"
			)
		})?;
		eprintln!("To log in, please open your browser to:\n\n{}\n", login.url);

		// Poll.
		let start_instant = Instant::now();
		let poll_interval = Duration::from_secs(1);
		let poll_duration = Duration::from_secs(300);
		let token = loop {
			if start_instant.elapsed() >= poll_duration {
				return Err(error!("Login timed out. Please try again."));
			}
			let login = client
				.get_login(&login.id)
				.await
				.map_err(|error| error!(source = error, "failed to get the login"))?
				.ok_or_else(|| error!("expected the login to exist"))?;
			if let Some(token) = login.token {
				break token;
			}
			tokio::time::sleep(poll_interval).await;
		};

		// Get the user.
		let user = client
			.get_user_for_token(&token)
			.await?
			.ok_or_else(|| error!("expected the user to exist"))?;

		// Save the user.
		tokio::task::spawn_blocking(move || Self::write_user(&user, None))
			.await
			.unwrap()?;

		eprintln!("You have successfully logged in.");

		Ok(())
	}
}
