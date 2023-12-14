use crate::Cli;
use std::time::{Duration, Instant};
use tangram_error::{return_error, Result, WrapErr};

/// Log in to Tangram.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	pub async fn command_login(&self, _args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Create a login.
		let login = tg
			.create_login()
			.await
			.wrap_err("Failed to create the login.")?;

		// Open the browser to the login URL.
		webbrowser::open(login.url.as_ref())
			.wrap_err("Failed to open the browser to the login URL.")?;
		eprintln!("To log in, please open your browser to:\n\n{}\n", login.url);

		// Poll.
		let start_instant = Instant::now();
		let poll_interval = Duration::from_secs(1);
		let poll_duration = Duration::from_secs(300);
		let token = loop {
			if start_instant.elapsed() >= poll_duration {
				return_error!("Login timed out. Please try again.");
			}
			let login = tg
				.get_login(&login.id)
				.await
				.wrap_err("Failed to get the login.")?
				.wrap_err("Expected the login to exist.")?;
			if let Some(token) = login.token {
				break token;
			}
			tokio::time::sleep(poll_interval).await;
		};

		// Get the user.
		let user = tg
			.get_user_for_token(&token)
			.await?
			.wrap_err("Expected the user to exist.")?;

		// Save the user.
		self.save_user(user).await?;

		eprintln!("You have successfully logged in.");

		Ok(())
	}
}
