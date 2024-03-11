use crate::{default_path, Cli, API_URL};
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use url::Url;

/// Manage the server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[command(verbatim_doc_comment)]
pub enum Command {
	Health(HealthArgs),
	Run(RunArgs),
	Start(StartArgs),
	Stop(StopArgs),
}

/// Get the server's health.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct HealthArgs {}

/// Run a server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct RunArgs {
	/// The address to bind to.
	#[arg(long)]
	pub address: Option<tg::Address>,

	/// The path to the config file.
	#[arg(long)]
	pub config: Option<PathBuf>,

	/// The path where the server should store its data. The default is `$HOME/.tangram`.
	#[arg(long)]
	pub path: Option<PathBuf>,

	/// The URL of the remote server.
	#[arg(long)]
	pub remote: Option<Url>,

	/// Disable the remote server.
	#[arg(long, default_value = "false")]
	pub no_remote: bool,

	/// Disable the VFS.
	#[arg(long, default_value = "false")]
	pub no_vfs: bool,
}

/// Start the server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct StartArgs {}

/// Stop the server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct StopArgs {}

impl Cli {
	pub async fn command_server(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Health(_) => {
				let client = tg::Builder::new(self.address.clone()).build();
				let health = client.health().await?;
				let health = serde_json::to_string_pretty(&health).unwrap();
				println!("{health}");
			},
			Command::Start(_) => {
				self.start_server().await?;
			},
			Command::Stop(_) => {
				let client = tg::Builder::new(self.address.clone()).build();
				client.stop().await?;
			},
			Command::Run(args) => {
				self.command_server_run(args).await?;
			},
		}
		Ok(())
	}

	async fn command_server_run(&self, args: RunArgs) -> Result<()> {
		// Get the config.
		let config = tokio::task::spawn_blocking({
			let config = args.config.clone();
			|| Self::read_config(config)
		})
		.await
		.unwrap()?;

		// Get the address.
		let address = args
			.address
			.or(config.as_ref().and_then(|config| config.address.clone()))
			.unwrap_or(tg::Address::Unix(default_path().join("socket")));

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
				|| tangram_server::options::Messenger::Local,
				|messenger| match messenger {
					crate::config::Messenger::Local => tangram_server::options::Messenger::Local,
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
							client_id: client.client_id.clone(),
							client_secret: client.client_secret.clone(),
							auth_url: client.auth_url.clone(),
							token_url: client.token_url.clone(),
						});
				tangram_server::options::Oauth { github }
			})
			.unwrap_or_default();

		// Get the path.
		let path = args
			.path
			.or(config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or(default_path());

		// Create the remote options.
		let url = args
			.remote
			.or(config
				.as_ref()
				.and_then(|config| config.remote.as_ref())
				.and_then(|remote| remote.url.clone()))
			.unwrap_or_else(|| API_URL.parse().unwrap());
		let tls = url.scheme() == "https";
		let client = tg::Builder::new(url.try_into()?).tls(tls).build();
		let host = tg::Triple::host()?;
		let build_ = config
			.as_ref()
			.and_then(|config| config.remote.as_ref())
			.and_then(|remote| remote.build.clone());
		let enable = build_
			.as_ref()
			.and_then(|build| build.enable)
			.unwrap_or(false);
		let hosts = build_
			.and_then(|build| build.hosts)
			.unwrap_or_else(|| vec![tg::Triple::js(), host.clone()]);
		let build_ = tangram_server::options::RemoteBuild { enable, hosts };
		let remote = tangram_server::options::Remote {
			tg: Box::new(client),
			build: build_,
		};
		let remote = if args.no_remote { None } else { Some(remote) };

		// Get the URL.
		let url = config.as_ref().and_then(|config| config.url.clone());

		let version = self.version.clone();

		// Create the vfs options.
		let mut vfs = config
			.as_ref()
			.and_then(|config| config.vfs.as_ref())
			.map_or_else(
				|| tangram_server::options::Vfs { enable: true },
				|vfs| tangram_server::options::Vfs {
					enable: vfs.enable.unwrap_or(true),
				},
			);
		if args.no_vfs {
			vfs.enable = false;
		};

		// Get the WWW URL.
		let www = config.as_ref().and_then(|config| config.www.clone());

		// Get the server's advanced options.
		let preserve_temp_directories = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.preserve_temp_directories)
			.unwrap_or(false);

		let write_build_logs_to_stderr = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_stderr)
			.unwrap_or(false);

		// Create the options.
		let options = tangram_server::Options {
			address,
			build,
			database,
			file_descriptor_semaphore_size,
			messenger,
			oauth,
			path,
			preserve_temp_directories,
			remote,
			url,
			version,
			vfs,
			write_build_logs_to_stderr,
			www,
		};

		// Start the server.
		let server = tangram_server::Server::start(options)
			.await
			.wrap_err("Failed to create the server.")?;

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
		server.join().await.wrap_err("Failed to join the server.")?;

		Ok(())
	}
}
