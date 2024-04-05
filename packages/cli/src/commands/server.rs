use crate::{default_path, Cli, API_URL};
use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

/// Manage the server.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Health(HealthArgs),
	Run(RunArgs),
	Start(StartArgs),
	Stop(StopArgs),
}

/// Get the server's health.
#[derive(Debug, clap::Args)]
pub struct HealthArgs {}

/// Run a server.
#[derive(Debug, clap::Args)]
pub struct RunArgs {
	/// The url to listen on.
	#[clap(long)]
	pub url: Option<Url>,

	/// The path to the config file.
	#[clap(long)]
	pub config: Option<PathBuf>,

	/// The path where the server should store its data. The default is `$HOME/.tangram`.
	#[clap(long)]
	pub path: Option<PathBuf>,
}

/// Start the server.
#[derive(Debug, clap::Args)]
pub struct StartArgs {}

/// Stop the server.
#[derive(Debug, clap::Args)]
pub struct StopArgs {}

impl Cli {
	pub async fn command_server(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Health(_) => {
				let client = self.client().await?;
				let health = client.health().await?;
				let health = serde_json::to_string_pretty(&health).unwrap();
				println!("{health}");
			},
			Command::Start(_) => {
				self.start_server().await?;
			},
			Command::Stop(_) => {
				let client = self.client().await?;
				client.stop().await?;
			},
			Command::Run(args) => {
				self.command_server_run(args).await?;
			},
		}
		Ok(())
	}

	async fn command_server_run(&self, args: RunArgs) -> tg::Result<()> {
		// Get the config.
		let config = tokio::task::spawn_blocking({
			let config = args.config.clone();
			|| Self::read_config(config)
		})
		.await
		.unwrap()?;

		// Get the path.
		let path = args
			.path
			.or(config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or(default_path());

		// Get the url.
		let url = args
			.url
			.or(config.as_ref().and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				format!("unix:{}", path.join("socket").display())
					.parse()
					.unwrap()
			});

		// Get the file descriptor semaphore size.
		let file_descriptor_semaphore_size = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_semaphore_size)
			.unwrap_or(1024);

		// Create the build options.
		let enable = config
			.as_ref()
			.and_then(|config| config.build.as_ref())
			.and_then(|build| build.enable)
			.unwrap_or(true);
		let max_concurrency = config
			.as_ref()
			.and_then(|config| config.build.as_ref())
			.and_then(|build| build.max_concurrency)
			.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
		let build = tangram_server::options::Build {
			enable,
			max_concurrency,
		};

		// Create the database options.
		let database = config
			.as_ref()
			.and_then(|config| config.database.as_ref())
			.map_or_else(
				|| {
					tangram_server::options::Database::Sqlite(
						tangram_server::options::SqliteDatabase {
							max_connections: std::thread::available_parallelism().unwrap().get(),
						},
					)
				},
				|database| match database {
					crate::config::Database::Sqlite(sqlite) => {
						let max_connections = sqlite
							.max_connections
							.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
						tangram_server::options::Database::Sqlite(
							tangram_server::options::SqliteDatabase { max_connections },
						)
					},
					crate::config::Database::Postgres(postgres) => {
						let url = postgres.url.clone();
						let max_connections = postgres
							.max_connections
							.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
						tangram_server::options::Database::Postgres(
							tangram_server::options::PostgresDatabase {
								url,
								max_connections,
							},
						)
					},
				},
			);

		// Create the messenger options.
		let messenger = config
			.as_ref()
			.and_then(|config| config.messenger.as_ref())
			.map_or_else(
				|| tangram_server::options::Messenger::Channel,
				|messenger| match messenger {
					crate::config::Messenger::Channel => {
						tangram_server::options::Messenger::Channel
					},
					crate::config::Messenger::Nats(nats) => {
						let url = nats.url.clone();
						tangram_server::options::Messenger::Nats(
							tangram_server::options::NatsMessenger { url },
						)
					},
				},
			);

		// Create the oauth options.
		let oauth = config
			.as_ref()
			.and_then(|config| config.oauth.as_ref())
			.map(|oauth| {
				let github =
					oauth
						.github
						.as_ref()
						.map(|client| tangram_server::options::OauthClient {
							auth_url: client.auth_url.clone(),
							client_id: client.client_id.clone(),
							client_secret: client.client_secret.clone(),
							redirect_url: client.redirect_url.clone(),
							token_url: client.token_url.clone(),
						});
				tangram_server::options::Oauth { github }
			})
			.unwrap_or_default();

		// Create the remote options.
		let remotes = config
			.as_ref()
			.and_then(|config| config.remotes.as_ref())
			.map(|remotes| {
				remotes
					.iter()
					.map(|remote| {
						let url = remote.url.clone();
						let client = tg::Builder::new(url).build();
						let build_ = remote.build.clone();
						let enable = build_
							.as_ref()
							.and_then(|build| build.enable)
							.unwrap_or(false);
						let build_ = tangram_server::options::RemoteBuild { enable };
						let remote = tangram_server::options::Remote {
							client,
							build: build_,
						};
						Ok::<_, tg::Error>(remote)
					})
					.collect()
			})
			.transpose()?;
		let remotes = if let Some(remotes) = remotes {
			remotes
		} else {
			let build = tangram_server::options::RemoteBuild { enable: false };
			let url = Url::parse(API_URL).unwrap();
			let client = tg::Builder::new(url).build();
			let remote = tangram_server::options::Remote { build, client };
			vec![remote]
		};

		// Get the version.
		let version = self.version.clone();

		// Create the vfs options.
		let vfs = config
			.as_ref()
			.and_then(|config| config.vfs.as_ref())
			.map_or_else(
				|| tangram_server::options::Vfs { enable: true },
				|vfs| tangram_server::options::Vfs {
					enable: vfs.enable.unwrap_or(true),
				},
			);

		// Get the server's advanced options.
		let preserve_temp_directories = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.preserve_temp_directories)
			.unwrap_or(false);
		let error_trace_options = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let write_build_logs_to_stderr = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_stderr)
			.unwrap_or(false);
		let advanced = tangram_server::options::Advanced {
			error_trace_options,
			file_descriptor_semaphore_size,
			preserve_temp_directories,
			write_build_logs_to_stderr,
		};

		// Create the options.
		let options = tangram_server::Options {
			advanced,
			build,
			database,
			messenger,
			oauth,
			path,
			remotes,
			url,
			version,
			vfs,
		};

		// Start the server.
		let server = tangram_server::Server::start(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the server"))?;

		// Stop the server if an an interrupt signal is received.
		tokio::spawn({
			let server = server.clone();
			async move {
				tokio::signal::ctrl_c().await.ok();
				server.stop();
				tokio::signal::ctrl_c().await.ok();
				std::process::exit(130);
			}
		});

		// Join the server.
		server
			.join()
			.await
			.map_err(|source| tg::error!(!source, "failed to join the server"))?;

		Ok(())
	}
}
