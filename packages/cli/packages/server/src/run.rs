use {
	futures::FutureExt as _,
	std::{collections::BTreeMap, ffi::OsString, path::PathBuf},
	tangram_cli_core::{Cli, RemoteOptions},
	tangram_client::prelude::*,
	tangram_server::{Owned as Server, Shutdown},
	tangram_uri::Uri,
};

/// Run the server in the foreground.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Override the `server.access` key in the config, which says who may use the local socket, the lock file, and checkouts: `user` for only the server's user, or `group` for the members of the server's group too.
	#[arg(long)]
	pub access: Option<tg::server::Access>,

	/// The path to the config file.
	#[arg(long, short)]
	pub config: Option<PathBuf>,

	/// Override the `server.directory` and `directory` keys in the config.
	#[arg(long, short)]
	pub directory: Option<PathBuf>,

	/// Override the `manager` key in the config, which names the manager that starts and stops the server: `app`, `cli`, `openrc`, or `systemd`. A server run without a manager can only be stopped by stopping its process.
	#[arg(long)]
	pub manager: Option<tg::server::Manager>,

	/// The service manager's notify socket, which is taken from the `NOTIFY_SOCKET` environment variable.
	#[arg(skip)]
	pub notify_socket: Option<OsString>,

	#[command(flatten)]
	pub remotes: RemoteOptions,

	/// The token.
	pub token: Option<String>,

	/// Override the tracing filter.
	#[arg(long)]
	pub tracing: Option<String>,

	/// Override the HTTP listener URL in the config.
	#[arg(long, short)]
	pub url: Option<Uri>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let tokio_single_threaded = cli
		.config
		.as_ref()
		.is_some_and(|config| config.tokio_single_threaded.unwrap_or_default());

	// Get the server config.
	let mut config = crate::config::resolve_config(cli)?;

	// Get the directory.
	let directory = cli.server_directory_path()?;

	// Set the access, directory, manager, and version.
	config.access = args.access.unwrap_or(cli.server_config().access);
	config.directory = Some(directory);
	config.manager = args.manager.or_else(|| cli.declared_server_manager());
	config.version = Some(tangram_cli_core::version());

	// Set the URL.
	if let Some(url) = &cli.args.url {
		let url = Cli::absolute_url(url)?;
		let listener = tangram_server::config::HttpListener { url, tls: None };
		config.http.listeners = vec![listener];
	}

	// Set the remotes.
	if let Some(remotes) = cli.args.remotes.values() {
		let mut output = BTreeMap::new();
		for remote in remotes {
			let (name, url) = remote
				.split_once('=')
				.ok_or_else(|| tg::error!(%remote, "invalid remote"))?;
			if name.is_empty() {
				return Err(tg::error!(%remote, "invalid remote"));
			}
			let url = url
				.parse()
				.map_err(|error| tg::error!(!error, %remote, "invalid remote URL"))?;
			let entry = tangram_server::config::Remote {
				token: None,
				trusted: false,
				url,
			};
			output.insert(name.to_owned(), entry);
		}
		config.remotes = Some(output);
	}

	// Set the trusted remotes.
	if let Some(trusted) = cli.args.remotes.trusted() {
		let remotes = config
			.remotes
			.as_mut()
			.ok_or_else(|| tg::error!("failed to find the trusted remotes"))?;
		for remote in remotes.values_mut() {
			remote.trusted = false;
		}
		for name in trusted {
			let remote = remotes
				.get_mut(name)
				.ok_or_else(|| tg::error!(%name, "failed to find the trusted remote"))?;
			remote.trusted = true;
		}
	}

	let notify_socket = args.notify_socket;

	let future = async move {
		let mut interrupt =
			tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
		let mut terminate =
			tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

		// Start the server.
		let server: Server = tokio::select! {
			result = tangram_server::start(config) => {
				result.map_err(|error| tg::error!(!error, "failed to start the server"))?
			},
			_ = interrupt.recv() => {
				return Ok::<_, tg::Error>(());
			},
			_ = terminate.recv() => {
				return Ok::<_, tg::Error>(());
			},
		};

		// Notify the service manager that the server is ready.
		if let Some(notify_socket) = notify_socket {
			tangram_util::notify::ready(&notify_socket).map_err(|error| {
				tg::error!(
					!error,
					"failed to notify the service manager that the server is ready"
				)
			})?;
		}

		let shutdown = tokio::select! {
			result = server.wait() => {
				result.unwrap();
				None
			},
			_ = interrupt.recv() => {
				Some(Shutdown::Interrupt)
			},
			_ = terminate.recv() => {
				Some(Shutdown::Terminate)
			},
		};

		if let Some(shutdown) = shutdown {
			server.shutdown(shutdown);
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
