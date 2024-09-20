use self::config::{Config, DEFAULT_FILE_DESCRIPTOR_SEMAPHORE_SIZE};
use clap::{CommandFactory as _, Parser as _};
use crossterm::{style::Stylize as _, tty::IsTty as _};
use futures::FutureExt as _;
use num::ToPrimitive as _;
use std::{collections::BTreeMap, fmt::Write as _, path::PathBuf, sync::Mutex};
use tangram_client::{self as tg, Client};
use tangram_either::Either;
use tangram_server::Server;
use tokio::io::AsyncWriteExt as _;
use tracing_subscriber::prelude::*;
use url::Url;

mod artifact;
mod blob;
mod build;
mod cat;
mod checksum;
mod config;
mod get;
mod lsp;
mod object;
mod package;
mod progress;
mod pull;
mod push;
mod remote;
mod server;
mod tag;
mod target;
mod tree;
mod upgrade;
mod view;

struct Cli {
	args: Args,
	config: Option<Config>,
	handle: Mutex<Option<Either<Client, Server>>>,
	mode: Mode,
}

#[derive(Clone, Debug, clap::Parser)]
#[command(
	about = "Tangram is a build system and package manager.",
	arg_required_else_help = true,
	before_help = before_help(),
	disable_help_subcommand = true,
	name = "Tangram",
	version = version(),
)]
struct Args {
	#[command(subcommand)]
	command: Command,

	/// The path to the config file.
	#[arg(long)]
	config: Option<PathBuf>,

	/// The mode.
	#[arg(short, long)]
	mode: Option<Mode>,

	/// Override the `path` key in the config.
	#[arg(short, long)]
	path: Option<PathBuf>,

	/// Override the `url` key in the config.
	#[arg(short, long)]
	url: Option<Url>,
}

fn before_help() -> String {
	let version = version();
	let logo = include_str!("tangram.ascii").trim_end();
	format!("Tangram {version}\n\n{logo}")
}

fn version() -> String {
	let mut version = env!("CARGO_PKG_VERSION").to_owned();
	if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
		version.push('+');
		version.push_str(commit);
	}
	version
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
	#[default]
	Auto,
	Client,
	Server,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Artifact(self::artifact::Args),
	Blob(self::blob::Args),
	Build(self::build::Args),
	Cat(self::cat::Args),
	Check(self::package::check::Args),
	Checkin(self::artifact::checkin::Args),
	Checkout(self::artifact::checkout::Args),
	Checksum(self::checksum::Args),
	Clean(self::server::clean::Args),
	Doc(self::package::document::Args),
	Download(self::blob::download::Args),
	Format(self::package::format::Args),
	Get(self::get::Args),
	Init(self::package::init::Args),
	List(self::tag::list::Args),
	Log(self::build::log::Args),
	Lsp(self::lsp::Args),
	New(self::package::new::Args),
	Object(self::object::Args),
	Outdated(self::package::outdated::Args),
	Package(self::package::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::object::put::Args),
	Remote(self::remote::Args),
	Run(self::target::run::Args),
	Serve(self::server::run::Args),
	Server(self::server::Args),
	Tag(self::tag::Args),
	Target(self::target::Args),
	Tree(self::tree::Args),
	Update(self::package::update::Args),
	Upgrade(self::upgrade::Args),
	View(self::view::Args),
}

fn main() -> std::process::ExitCode {
	// Initialize V8.
	Cli::initialize_v8();

	// Parse the args.
	let args = Args::parse();

	// Read the config.
	let config = match Cli::read_config(args.config.clone()) {
		Ok(config) => config,
		Err(error) => {
			eprintln!("{} failed to read the config", "error".red().bold());
			futures::executor::block_on(Cli::print_error(&error, None));
			return 1.into();
		},
	};

	// Initialize tracing.
	Cli::initialize_tracing(config.as_ref());

	// Set the file descriptor limit.
	Cli::set_file_descriptor_limit(config.as_ref())
		.inspect_err(|_| {
			eprintln!(
				"{} failed to set the file descriptor limit",
				"warning".yellow().bold(),
			);
		})
		.ok();

	// Create the handle.
	let handle = Mutex::new(None);

	// Get the mode.
	let mode = match &args {
		// If the command is `tg serve` or `tg server run`, then set the mode to `server`.
		Args {
			command:
				Command::Serve(_)
				| Command::Server(self::server::Args {
					command: self::server::Command::Run(_),
					..
				}),
			..
		} => Mode::Server,

		// If the command is anything else under `tg server`, then set the mode to `client.`
		Args {
			command: Command::Server(_),
			..
		} => Mode::Client,

		_ => args.mode.unwrap_or_default(),
	};

	// Create the CLI.
	let cli = Cli {
		args,
		config,
		handle,
		mode,
	};

	// Create the future.
	let future = async move {
		match cli.command(cli.args.command.clone()).await {
			Ok(()) => Ok(()),
			Err(error) => {
				eprintln!("{} failed to run the command", "error".red().bold());
				Cli::print_error(&error, cli.config.as_ref()).await;
				Err(1)
			},
		}
	};

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	let runtime = builder.build().unwrap();
	let result = runtime.block_on(future);
	runtime.shutdown_background();

	// Handle the result.
	match result {
		Ok(()) => 0.into(),
		Err(code) => code.into(),
	}
}

impl Cli {
	async fn handle(&self) -> tg::Result<Either<Client, Server>> {
		// If the handle has already been created, then return it.
		if let Some(client) = self.handle.lock().unwrap().clone() {
			return Ok(client);
		}

		// Create the handle.
		let handle = match self.mode {
			Mode::Auto => Either::Left(self.auto().await?),
			Mode::Client => Either::Left(self.client().await?),
			Mode::Server => Either::Right(self.server().await?),
		};

		// Set the handle.
		self.handle.lock().unwrap().replace(handle.clone());

		Ok(handle)
	}

	async fn auto(&self) -> tg::Result<Client> {
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

		// Create the client.
		let client = tg::Client::new(url);

		// Attempt to connect to the server.
		client.connect().await.ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == "http+unix"
			|| matches!(client.url().host_str(), Some("localhost" | "0.0.0.0"));
		if !client.connected().await && local {
			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second.
			for _ in 0..10 {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}

			// If the client is not connected, then return an error.
			if !client.connected().await {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		// If the URL is local and the server's version is different from the client, then disconnect and restart the server.
		'a: {
			if !local {
				break 'a;
			}

			let Some(server_version) = &client.health().await?.version else {
				break 'a;
			};

			if Args::command().get_version().unwrap() == server_version {
				break 'a;
			};

			// Disconnect.
			client.disconnect().await?;

			// Stop the server.
			self.stop_server().await?;

			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second.
			for _ in 0..10 {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}

			// If the client is not connected, then return an error.
			if !client.connected().await {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		Ok(client)
	}

	async fn client(&self) -> tg::Result<Client> {
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

		// Create the client.
		let client = tg::Client::new(url);

		// Attempt to connect to the server.
		client.connect().await.ok();

		Ok(client)
	}

	async fn server(&self) -> tg::Result<Server> {
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
			.unwrap_or(DEFAULT_FILE_DESCRIPTOR_SEMAPHORE_SIZE);
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
			None => Some(crate::config::Build::default()),
			Some(None) => None,
			Some(Some(config)) => Some(config),
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
			None => Some(crate::config::BuildHeartbeatMonitor::default()),
			Some(None) => None,
			Some(Some(config)) => Some(config),
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
			None => Some(crate::config::BuildIndexer::default()),
			Some(None) => None,
			Some(Some(config)) => Some(config),
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
			None => Some(crate::config::ObjectIndexer::default()),
			Some(None) => None,
			Some(Some(config)) => Some(config),
		};
		let object_indexer =
			object_indexer.map(|object_indexer| tangram_server::options::ObjectIndexer {
				batch_size: object_indexer.batch_size.unwrap_or(128),
				timeout: object_indexer.timeout.unwrap_or(60.0),
			});

		// Create the remote options.
		let mut remotes = BTreeMap::new();
		let name = "default".to_owned();
		let remote = tangram_server::options::Remote {
			build: false,
			client: tg::Client::new(Url::parse("https://api.tangram.dev").unwrap()),
		};
		remotes.insert(name, remote);
		match self
			.config
			.as_ref()
			.and_then(|config| config.remotes.as_ref())
		{
			None => (),
			Some(None) => remotes.clear(),
			Some(Some(remotes_)) => {
				for (name, remote) in remotes_ {
					match remote {
						None => {
							remotes.remove(name);
						},
						Some(remote) => {
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
			Some(None) => None,
			Some(Some(config)) => Some(config),
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
			remotes,
			url,
			version,
			vfs,
		};

		// Start the server.
		let server = tangram_server::Server::start(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the server"))?;

		Ok(server)
	}

	/// Start the server.
	async fn start_server(&self) -> tg::Result<()> {
		// Ensure the path exists.
		let home = PathBuf::from(std::env::var("HOME").unwrap());
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| home.join(".tangram"));
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Get the log file path.
		let log_path = path.join("log");

		// Get the path to the current executable.
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;

		// Spawn the server.
		let stdout = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;
		let stderr = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.truncate(true)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(home)
			.stdin(std::process::Stdio::null())
			.stdout(stdout)
			.stderr(stderr)
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the server"))?;

		Ok(())
	}

	/// Stop the server.
	async fn stop_server(&self) -> tg::Result<()> {
		// Get the lock file path.
		let path = self
			.args
			.path
			.clone()
			.or(self.config.as_ref().and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		let lock_path = path.join("lock");

		// Read the PID from the lock file.
		let pid = tokio::fs::read_to_string(&lock_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the pid from the lock file"))?
			.parse::<u32>()
			.map_err(|source| tg::error!(!source, "invalid lock file"))?;

		// Send SIGINT to the server.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGINT to the server"));
		}

		// Wait up to five seconds for the server to exit.
		for _ in 0..50 {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			let duration = std::time::Duration::from_millis(100);
			tokio::time::sleep(duration).await;
		}

		// If the server has still not exited, then send SIGTERM.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGTERM to the server"));
		}

		// Wait up to one second for the server to exit.
		for _ in 0..10 {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			let duration = std::time::Duration::from_millis(100);
			tokio::time::sleep(duration).await;
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	// Run the command
	async fn command(&self, command: Command) -> tg::Result<()> {
		match command {
			Command::Artifact(args) => self.command_artifact(args).boxed(),
			Command::Blob(args) => self.command_blob(args).boxed(),
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Cat(args) => self.command_cat(args).boxed(),
			Command::Check(args) => self.command_package_check(args).boxed(),
			Command::Checkin(args) => self.command_artifact_checkin(args).boxed(),
			Command::Checkout(args) => self.command_artifact_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Clean(args) => self.command_server_clean(args).boxed(),
			Command::Doc(args) => self.command_package_doc(args).boxed(),
			Command::Download(args) => self.command_blob_download(args).boxed(),
			Command::Format(args) => self.command_package_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Init(args) => self.command_package_init(args).boxed(),
			Command::List(args) => self.command_tag_list(args).boxed(),
			Command::Log(args) => self.command_build_log(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::New(args) => self.command_package_new(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Outdated(args) => self.command_package_outdated(args).boxed(),
			Command::Package(args) => self.command_package(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Put(args) => self.command_object_put(args).boxed(),
			Command::Remote(args) => self.command_remote(args).boxed(),
			Command::Run(args) => self.command_target_run(args).boxed(),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Tag(args) => self.command_tag(args).boxed(),
			Command::Target(args) => self.command_target(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_package_update(args).boxed(),
			Command::Upgrade(args) => self.command_upgrade(args).boxed(),
			Command::View(args) => self.command_view(args).boxed(),
		}
		.await
	}

	fn read_config(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => {
				return Err(
					tg::error!(!source, %path = path.display(), "failed to read the config file"),
				)
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	fn _write_config(config: &Config, path: Option<PathBuf>) -> tg::Result<()> {
		let path = path.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = serde_json::to_string_pretty(&config)
			.map_err(|source| tg::error!(!source, "failed to serialize the config"))?;
		std::fs::write(path, config)
			.map_err(|source| tg::error!(!source, "failed to save the config"))?;
		Ok(())
	}

	async fn print_error(error: &tg::Error, config: Option<&Config>) {
		let options = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let trace = error.trace(&options);
		let mut errors = vec![trace.error];
		while let Some(next) = errors.last().unwrap().source.as_ref() {
			errors.push(next);
		}
		if !trace.options.reverse {
			errors.reverse();
		}
		for error in errors {
			let message = error.message.as_deref().unwrap_or("an error occurred");
			eprintln!("{} {message}", "->".red());
			if let Some(location) = &error.location {
				if !location.source.is_internal() || trace.options.internal {
					let mut string = String::new();
					write!(string, "{location}").unwrap();
					eprintln!("   {}", string.yellow());
				}
			}
			for (name, value) in &error.values {
				let name = name.as_str().blue();
				let value = value.as_str().green();
				eprintln!("   {name} = {value}");
			}
			let mut stack = error.stack.iter().flatten().collect::<Vec<_>>();
			if !trace.options.reverse {
				stack.reverse();
			}
			for location in stack {
				if !location.source.is_internal() || trace.options.internal {
					let location = location.to_string().yellow();
					eprintln!("   {location}");
				}
			}
		}
	}

	async fn print_diagnostic(&self, diagnostic: &tg::Diagnostic) {
		let title = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		eprintln!("{title}: {}", diagnostic.message);
		let mut string = String::new();
		if let Some(location) = &diagnostic.location {
			match &location.module.object {
				Some(Either::Left(object)) => {
					write!(string, "{object}").unwrap();
				},
				Some(Either::Right(path)) => {
					write!(string, "{}", path.display()).unwrap();
				},
				None => {},
			}
			if let Some(path) = &location.module.path {
				write!(string, ":{path}").unwrap();
			}
			let mut string = if string.is_empty() {
				"<unknown>".to_owned()
			} else {
				string
			};
			let line = location.range.start.line + 1;
			let character = location.range.start.character + 1;
			write!(string, ":{line}:{character}").unwrap();
			eprint!("   {}", string.yellow());
		}
		eprintln!();
	}

	async fn output_json<T>(output: &T) -> tg::Result<()>
	where
		T: serde::Serialize,
	{
		let mut stdout = tokio::io::stdout();
		if stdout.is_tty() {
			let output = serde_json::to_string_pretty(output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			stdout
				.write_all(output.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
			stdout
				.write_all(b"\n")
				.await
				.map_err(|source| tg::error!(!source, "failed to write"))?;
		} else {
			let output = serde_json::to_string(&output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
			stdout
				.write_all(output.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		}
		Ok(())
	}

	async fn get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Either<tg::Build, tg::Object>> {
		let handle = self.handle().await?;

		// If the reference has a path, then canonicalize it.
		let reference = if let tg::reference::Path::Path(path) = reference.path() {
			let path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			let path = tg::Path::try_from(path)?;
			let uri = reference.uri().to_builder().path(path).build().unwrap();
			tg::Reference::with_uri(uri)?
		} else {
			reference.clone()
		};

		reference.get(&handle).await
	}

	/// Initialize V8.
	fn initialize_v8() {
		// Set the ICU data.
		v8::icu::set_common_data_73(deno_core_icudata::ICU_DATA).unwrap();

		// Initialize the platform.
		let platform = v8::new_default_platform(0, true);
		v8::V8::initialize_platform(platform.make_shared());

		// Set flags.
		v8::V8::set_flags_from_string("--harmony-import-attributes");

		// Initialize V8.
		v8::V8::initialize();
	}

	/// Initialize tracing.
	fn initialize_tracing(config: Option<&Config>) {
		let console_layer = if config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.map_or(false, |advanced| advanced.tokio_console)
		{
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let output_layer = config
			.as_ref()
			.and_then(|config| config.tracing.as_ref())
			.map(|tracing| {
				let filter =
					tracing_subscriber::filter::EnvFilter::try_new(&tracing.filter).unwrap();
				let format = tracing
					.format
					.unwrap_or(self::config::TracingFormat::Pretty);
				let output_layer = match format {
					self::config::TracingFormat::Compact
					| self::config::TracingFormat::Json
					| self::config::TracingFormat::Pretty => {
						let layer = tracing_subscriber::fmt::layer()
							.with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
							.with_writer(std::io::stderr);
						let layer = match format {
							self::config::TracingFormat::Compact => layer.compact().boxed(),
							self::config::TracingFormat::Hierarchical => unreachable!(),
							self::config::TracingFormat::Json => layer.json().boxed(),
							self::config::TracingFormat::Pretty => layer.pretty().boxed(),
						};
						layer.boxed()
					},
					self::config::TracingFormat::Hierarchical => {
						tracing_tree::HierarchicalLayer::new(2)
							.with_bracketed_fields(true)
							.with_span_retrace(true)
							.with_targets(true)
							.boxed()
					},
				};
				output_layer.with_filter(filter)
			});
		tracing_subscriber::registry()
			.with(console_layer)
			.with(output_layer)
			.init();
	}

	fn set_file_descriptor_limit(config: Option<&Config>) -> tg::Result<()> {
		let file_descriptor_limit = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_limit);
		let semaphore_size = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_semaphore_size);

		let file_descriptor_limit = match (semaphore_size, file_descriptor_limit) {
			// If neither is provided, set to twice the default semaphore size.
			(None, None) => u64::try_from(DEFAULT_FILE_DESCRIPTOR_SEMAPHORE_SIZE).unwrap() * 2,

			// If just the semaphore size is set, use twice the limit.
			(Some(semaphore_size), None) => u64::try_from(semaphore_size).unwrap() * 2,

			(None, Some(file_descriptor_limit)) => {
				let default_semaphore_size =
					u64::try_from(DEFAULT_FILE_DESCRIPTOR_SEMAPHORE_SIZE).unwrap();
				if !Cli::is_at_most_half(default_semaphore_size, file_descriptor_limit) {
					tracing::warn!(?default_semaphore_size, file_descriptor_limit, "file descriptor size is less than twice the default file descriptor semaphore size");
				}
				file_descriptor_limit
			},

			(Some(semaphore_size), Some(file_descriptor_limit)) => {
				if !Cli::is_at_most_half(semaphore_size.try_into().unwrap(), file_descriptor_limit)
				{
					tracing::warn!(?semaphore_size, file_descriptor_limit, "file descriptor semaphore size is greater than 50% of the configured limit.");
				}
				file_descriptor_limit
			},
		};

		let new_fd_rlimit = libc::rlimit {
			rlim_cur: file_descriptor_limit,
			rlim_max: file_descriptor_limit,
		};
		let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_fd_rlimit) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the file descriptor limit"
			));
		}
		Ok(())
	}

	/// Determine whether value a is at most half of value b.
	fn is_at_most_half(a: u64, b: u64) -> bool {
		// A bitwise right shift will divide the number by 2.
		a <= (b >> 1)
	}

	// Get the host.
	fn host() -> &'static str {
		#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
		{
			"aarch64-darwin"
		}
		#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
		{
			"aarch64-linux"
		}
		#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
		{
			"x86_64-darwin"
		}
		#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
		{
			"x86_64-linux"
		}
	}
}
