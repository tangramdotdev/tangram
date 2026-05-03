use {
	crate::{Cli, Remotes},
	futures::FutureExt as _,
	std::{
		io::Write as _,
		os::fd::{FromRawFd as _, RawFd},
		path::PathBuf,
	},
	tangram_client::prelude::*,
	tangram_server::Shared as Server,
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
		let tokio_single_threaded = self
			.config
			.as_ref()
			.is_some_and(|config| config.tokio_single_threaded);

		// Get the server config.
		let mut config = self
			.config
			.as_ref()
			.map(|config| config.server.clone())
			.unwrap_or_default();

		// Get the directory.
		let directory = self.directory_path();

		// Set the directory and version.
		config.directory = Some(directory);
		config.version = Some(crate::version());

		// Set the URL.
		if let Some(url) = &self.args.url {
			config.http = Some(tangram_server::config::Http {
				listeners: vec![tangram_server::config::HttpListener {
					url: url.clone(),
					tls: None,
				}],
			});
		}

		// Set the remotes.
		if let Some(remotes) = self.args.remotes.get() {
			config.remotes = Some(
				remotes
					.iter()
					.filter_map(|remote_str| {
						let (name, url_str) = remote_str.split_once('=')?;
						let url = url_str.parse().ok()?;
						Some(tangram_server::config::Remote {
							name: name.to_owned(),
							url,
							reconnect: None,
							retry: None,
							token: None,
						})
					})
					.collect(),
			);
		}

		let ready_fd = args.ready_fd;

		let future = async move {
			// Start the server.
			let server: Server = tangram_server::Server::start(config)
				.await
				.map_err(|source| tg::error!(!source, "failed to start the server"))?
				.into();

			if let Some(ready_fd) = ready_fd {
				let ready_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(ready_fd) };
				let mut ready = std::fs::File::from(ready_fd);
				ready
					.write_all(&[0x00])
					.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
				ready
					.flush()
					.map_err(|source| tg::error!(!source, "failed to flush the ready signal"))?;
			}

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

			Ok::<_, tg::Error>(())
		};

		if tokio_single_threaded {
			future.boxed().await?;
		} else {
			let runtime = tokio::runtime::Builder::new_multi_thread()
				.enable_all()
				.build()
				.map_err(|source| tg::error!(!source, "failed to create the tokio runtime"))?;
			let task: tokio::task::JoinHandle<tg::Result<()>> = runtime.spawn(future);
			let result = task.await;
			runtime.shutdown_background();
			result.map_err(|source| tg::error!(!source, "the server runtime task panicked"))??;
		}

		Ok(())
	}
}
