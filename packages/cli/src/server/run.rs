use {
	crate::{Cli, Remotes},
	futures::FutureExt as _,
	std::{
		io::Write as _,
		os::fd::{FromRawFd as _, RawFd},
		path::PathBuf,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_server::Owned as Server,
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
			let listener = tangram_server::config::HttpListener {
				url: url.clone(),
				tls: None,
			};
			let http = config.http.get_or_insert_default();
			http.listeners = vec![listener];
		}

		// Set the remotes.
		if let Some(remotes) = self.args.remotes.get() {
			config.remotes = Some(
				remotes
					.iter()
					.filter_map(|remote_str| {
						let (name, url_str) = remote_str.split_once('=')?;
						let url = url_str.parse().ok()?;
						Some((
							name.to_owned(),
							tangram_server::config::Remote { url, token: None },
						))
					})
					.collect(),
			);
		}

		let ready_fd = args.ready_fd;

		let future = async move {
			let mut interrupt =
				tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
			let mut terminate =
				tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

			// Start the server.
			let server: Server = tokio::select! {
				result = tangram_server::Server::start(config).boxed() => {
					result.map_err(|error| tg::error!(!error, "failed to start the server"))?
				},
				_ = interrupt.recv() => {
					return Ok::<_, tg::Error>(());
				},
				_ = terminate.recv() => {
					return Ok::<_, tg::Error>(());
				},
			};

			if let Some(ready_fd) = ready_fd {
				let ready_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(ready_fd) };
				let mut ready = std::fs::File::from(ready_fd);
				ready
					.write_all(&[0x00])
					.map_err(|error| tg::error!(!error, "failed to write the ready signal"))?;
				ready
					.flush()
					.map_err(|error| tg::error!(!error, "failed to flush the ready signal"))?;
			}

			let shutdown = tokio::select! {
				result = server.wait() => {
					result.unwrap();
					false
				},
				_ = interrupt.recv() => {
					server.stop();
					true
				},
				_ = terminate.recv() => {
					server.stop();
					true
				},
			};

			if shutdown {
				tokio::select! {
					result = server.wait() => {
						result.unwrap();
					},
					_ = interrupt.recv() => {
						std::process::exit(130);
					},
					_ = terminate.recv() => {
						std::process::exit(130);
					}
				}
			}
			drop(server);

			Ok::<_, tg::Error>(())
		};

		if tokio_single_threaded {
			future.boxed().await?;
		} else {
			tokio::task::spawn_blocking(move || {
				let runtime = tokio::runtime::Builder::new_multi_thread()
					.enable_all()
					.build()
					.map_err(|error| tg::error!(!error, "failed to create the tokio runtime"))?;
				let result = runtime.block_on(future);
				drop(runtime);
				result
			})
			.await
			.map_err(|error| tg::error!(!error, "the server runtime task panicked"))??;
		}

		Ok(())
	}
}
