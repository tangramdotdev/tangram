use {
	crate::{Cli, Remotes},
	std::{
		io::Write as _,
		os::fd::{FromRawFd as _, RawFd},
		path::PathBuf,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

/// Run the server in the foreground.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path to the config file.
	#[arg(long, short)]
	pub config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[arg(long, short)]
	pub directory: Option<PathBuf>,

	#[arg(hide = true, long)]
	pub ready_fd: Option<RawFd>,

	#[command(flatten)]
	pub remotes: Remotes,

	/// The token.
	pub token: Option<String>,

	/// Override the tracing filter.
	#[arg(long)]
	pub tracing: Option<String>,

	/// Override the HTTP listener URL in the config.
	#[arg(long, short)]
	pub url: Option<Uri>,
}

impl Cli {
	pub async fn command_server_run(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		if let Some(ready_fd) = args.ready_fd {
			let ready_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(ready_fd) };
			let mut ready = std::fs::File::from(ready_fd);
			ready
				.write_all(&[0x00])
				.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
			ready
				.flush()
				.map_err(|source| tg::error!(!source, "failed to flush the ready signal"))?;
		}

		// Get the server.
		let server = handle.as_ref().unwrap_right();

		// Spawn a task to stop the server on the first interrupt signal and exit the process on the second.
		tokio::spawn({
			let server = server.clone();
			async move {
				tokio::signal::ctrl_c().await.unwrap();
				server.stop();
				drop(server);
				tokio::signal::ctrl_c().await.unwrap();
				std::process::exit(130);
			}
		});

		// Wait for the server.
		server.wait().await.unwrap();

		Ok(())
	}
}
