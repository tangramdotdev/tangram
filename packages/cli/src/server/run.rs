use crate::Cli;
use clap::CommandFactory as _;
use either::Either;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client as tg;
use url::Url;

/// Run the server in the foreground.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_server_run(&self, _args: Args) -> tg::Result<()> {
		// Get the path.
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self.config.as_ref().and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the advanced options.
		let build_dequeue_timeout = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.build_dequeue_timeout);
		let error_trace_options = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let file_descriptor_semaphore_size = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_semaphore_size)
			.unwrap_or(1024);
		let preserve_temp_directories = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.preserve_temp_directories)
			.unwrap_or(false);
		let write_build_logs_to_file = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_database)
			.unwrap_or(false);
		let write_build_logs_to_stderr = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.duplicate_build_logs_to_stderr)
			.unwrap_or(false);
		let advanced = tangram_server::options::Advanced {
			build_dequeue_timeout,
			error_trace_options,
			file_descriptor_semaphore_size,
			preserve_temp_directories,
			write_build_logs_to_database: write_build_logs_to_file,
			write_build_logs_to_stderr,
		};

		// Create the authentication options.
		let authentication =
			self.config
				.as_ref()
				.and_then(|config| config.authentication.as_ref())
				.map(|authentication| {
					let providers = authentication.providers.as_ref().map(|providers| {
						let github = providers.github.as_ref().map(|client| {
							tangram_server::options::Oauth {
								auth_url: client.auth_url.clone(),
								client_id: client.client_id.clone(),
								client_secret: client.client_secret.clone(),
								redirect_url: client.redirect_url.clone(),
								token_url: client.token_url.clone(),
							}
						});
						tangram_server::options::AuthenticationProviders { github }
					});
					tangram_server::options::Authentication { providers }
				})
				.unwrap_or_default();

		// Create the build options.
		let build = match self.config.as_ref().and_then(|config| config.build.clone()) {
			Some(Either::Left(false)) => None,
			None | Some(Either::Left(true)) => Some(crate::config::Build::default()),
			Some(Either::Right(value)) => Some(value),
		};
		let build = build.map(|build| {
			let concurrency = build
				.concurrency
				.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
			let heartbeat_interval = build.heartbeat_interval.map_or(
				std::time::Duration::from_secs(1),
				std::time::Duration::from_secs_f64,
			);
			tangram_server::options::Build {
				concurrency,
				heartbeat_interval,
			}
		});

		// Create the build heartbeat monitor options.
		let build_heartbeat_monitor = self
			.config
			.as_ref()
			.and_then(|config| config.build_heartbeat_monitor.clone());
		let build_heartbeat_monitor = match build_heartbeat_monitor {
			Some(Either::Left(false)) => None,
			None | Some(Either::Left(true)) => {
				Some(crate::config::BuildHeartbeatMonitor::default())
			},
			Some(Either::Right(config)) => Some(config),
		};
		let build_heartbeat_monitor = build_heartbeat_monitor.map(|config| {
			let interval = config.interval.unwrap_or(1);
			let limit = config.limit.unwrap_or(100);
			let timeout = config.timeout.unwrap_or(60);
			let interval = std::time::Duration::from_secs(interval);
			let timeout = std::time::Duration::from_secs(timeout);
			tangram_server::options::BuildHeartbeatMonitor {
				interval,
				limit,
				timeout,
			}
		});

		// Create the build indexer options.
		let build_indexer = self
			.config
			.as_ref()
			.and_then(|config| config.build_indexer.clone());
		let build_indexer = match build_indexer {
			Some(Either::Left(false)) => None,
			None | Some(Either::Left(true)) => Some(crate::config::BuildIndexer::default()),
			Some(Either::Right(config)) => Some(config),
		};
		let build_indexer = build_indexer.map(|_| tangram_server::options::BuildIndexer {});

		// Create the database options.
		let database = self
			.config
			.as_ref()
			.and_then(|config| config.database.as_ref())
			.map_or_else(
				|| {
					tangram_server::options::Database::Sqlite(
						tangram_server::options::SqliteDatabase {
							connections: std::thread::available_parallelism().unwrap().get(),
						},
					)
				},
				|database| match database {
					crate::config::Database::Sqlite(sqlite) => {
						let connections = sqlite
							.connections
							.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
						tangram_server::options::Database::Sqlite(
							tangram_server::options::SqliteDatabase { connections },
						)
					},
					crate::config::Database::Postgres(postgres) => {
						let url = postgres.url.clone();
						let connections = postgres
							.connections
							.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
						tangram_server::options::Database::Postgres(
							tangram_server::options::PostgresDatabase { url, connections },
						)
					},
				},
			);

		// Create the messenger options.
		let messenger = self
			.config
			.as_ref()
			.and_then(|config| config.messenger.as_ref())
			.map_or_else(
				|| tangram_server::options::Messenger::Memory,
				|messenger| match messenger {
					crate::config::Messenger::Memory => tangram_server::options::Messenger::Memory,
					crate::config::Messenger::Nats(nats) => {
						let url = nats.url.clone();
						tangram_server::options::Messenger::Nats(
							tangram_server::options::NatsMessenger { url },
						)
					},
				},
			);

		// Create the object indexer options.
		let object_indexer = self
			.config
			.as_ref()
			.and_then(|config| config.object_indexer.clone());
		let object_indexer = match object_indexer {
			Some(Either::Left(false)) => None,
			None | Some(Either::Left(true)) => Some(crate::config::ObjectIndexer::default()),
			Some(Either::Right(config)) => Some(config),
		};
		let object_indexer = object_indexer.map(|_| tangram_server::options::ObjectIndexer {});

		// Create the registry option.
		let registry = match self
			.config
			.as_ref()
			.and_then(|config| config.registry.as_ref())
		{
			Some(Either::Left(_)) => None,
			Some(Either::Right(registry)) => Some(registry.clone()),
			None => Some("default".to_owned()),
		};

		// Create the remote options.
		let mut remotes = BTreeMap::default();
		let name = "default".to_owned();
		let remote = tangram_server::options::Remote {
			build: false,
			client: tg::Client::new(Url::parse("https://api.tangram.dev").unwrap()),
		};
		remotes.insert(name, remote);
		if let Some(either) = self
			.config
			.as_ref()
			.and_then(|config| config.remotes.as_ref())
		{
			match either {
				Either::Left(false) => remotes.clear(),
				Either::Left(true) => (),
				Either::Right(remotes_) => {
					for (name, remote) in remotes_ {
						match remote {
							Either::Left(false) => {
								remotes.remove(name);
							},
							Either::Left(true) => {
								return Err(tg::error!("invalid remote value"));
							},
							Either::Right(remote) => {
								let name = name.clone();
								let build = remote.build.unwrap_or_default();
								let url = remote.url.clone();
								let client = tg::Client::new(url);
								let remote = tangram_server::options::Remote { build, client };
								remotes.insert(name, remote);
							},
						}
					}
				},
			}
		}

		// Get the version.
		let version = Some(crate::Args::command().get_version().unwrap().to_owned());

		// Create the vfs options.
		let vfs = self.config.as_ref().and_then(|config| config.vfs.clone());
		let vfs = match vfs {
			None => {
				if cfg!(target_os = "macos") {
					None
				} else {
					Some(crate::config::Vfs::default())
				}
			},
			Some(Either::Left(false)) => None,
			Some(Either::Left(true)) => Some(crate::config::Vfs::default()),
			Some(Either::Right(config)) => Some(config),
		};
		let vfs = vfs.map(|config| {
			let cache_ttl = config.cache_ttl.unwrap_or(10.0);
			let cache_size = config.cache_size.unwrap_or(4096);
			let database_connections = config.database_connections.unwrap_or(4);
			tangram_server::options::Vfs {
				cache_ttl,
				cache_size,
				database_connections,
			}
		});

		// Create the options.
		let options = tangram_server::Options {
			advanced,
			authentication,
			build,
			build_heartbeat_monitor,
			build_indexer,
			database,
			messenger,
			object_indexer,
			path,
			registry,
			remotes,
			url,
			version,
			vfs,
		};

		// Start the server.
		let server = tangram_server::Server::start(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the server"))?;

		// Spawn a task to stop the server on the first interrupt signal and exit the process on the second.
		tokio::spawn({
			let server = server.clone();
			async move {
				tokio::signal::ctrl_c().await.unwrap();
				server.stop();
				tokio::signal::ctrl_c().await.unwrap();
				std::process::exit(130);
			}
		});

		// Wait for the server.
		server.wait().await?;

		Ok(())
	}
}
