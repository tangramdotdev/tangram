use self::config::Config;
use clap::Parser as _;
use crossterm::style::Stylize as _;
use either::Either;
use futures::FutureExt as _;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{fmt::Write as _, path::PathBuf};
use tangram_client as tg;
use tangram_server::Server;
use tg::Client;
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
mod pull;
mod push;
mod root;
mod server;
mod target;
mod tree;
mod tui;
mod upgrade;
mod view;

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Cli {
	args: Args,
	config: Option<Config>,
	handle: Either<Client, Server>,
}

#[derive(Clone, Debug, clap::Parser)]
#[command(
	about = env!("CARGO_PKG_DESCRIPTION"),
	disable_help_subcommand = true,
	long_version = env!("CARGO_PKG_VERSION"),
	name = env!("CARGO_CRATE_NAME"),
	version = env!("CARGO_PKG_VERSION"),
)]
struct Args {
	/// The path to the config file.
	#[arg(long)]
	config: Option<PathBuf>,

	/// Override the `mode` key in the config.
	#[arg(short, long)]
	mode: Option<Mode>,

	/// Override the `path` key in the config.
	#[arg(short, long)]
	path: Option<PathBuf>,

	/// Override the `url` key in the config.
	#[arg(short, long)]
	url: Option<Url>,

	#[command(subcommand)]
	command: Command,
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
	Doc(self::package::doc::Args),
	Download(self::blob::download::Args),
	Format(self::package::format::Args),
	Get(self::get::Args),
	Init(self::package::init::Args),
	Log(self::build::log::Args),
	Lsp(self::lsp::Args),
	New(self::package::new::Args),
	Object(self::object::Args),
	Outdated(self::package::outdated::Args),
	Package(self::package::Args),
	Publish(self::package::publish::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Root(self::root::Args),
	Run(self::target::run::Args),
	Search(self::package::search::Args),
	Serve(self::server::run::Args),
	Server(self::server::Args),
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
			eprintln!("{}: failed to read the config", "error".red().bold());
			futures::executor::block_on(Cli::print_error(None::<Client>, &error, None));
			return 1.into();
		},
	};

	// Initialize tracing.
	Cli::initialize_tracing(config.as_ref());

	// Set the file descriptor limit.
	Cli::set_file_descriptor_limit(config.as_ref())
		.inspect_err(|_| {
			eprintln!(
				"{}: failed to set the file descriptor limit",
				"warning".yellow().bold(),
			);
		})
		.ok();

	// Create the future.
	let future = async move {
		// Create the CLI.
		let cli = match Cli::new(args.clone(), config.clone()).await {
			Ok(cli) => cli,
			Err(error) => {
				eprintln!("{}: an error occurred", "error".red().bold());
				Cli::print_error(None::<Client>, &error, config.as_ref()).await;
				return Err(1);
			},
		};

		// Run the command.
		let result = match cli.command(args.command).await {
			Ok(()) => Ok(()),
			Err(error) => {
				eprintln!("{}: failed to run the command", "error".red().bold());
				Cli::print_error(Some(cli.handle.clone()), &error, config.as_ref()).await;
				Err(1)
			},
		};

		// Stop the server if necessary.
		if let Some(server) = cli.handle.right() {
			server.stop();
			let result = server.wait().await;
			match result {
				Ok(()) => (),
				Err(error) => {
					eprintln!("{}: an error occurred", "error".red().bold());
					Cli::print_error(None::<Client>, &error, config.as_ref()).await;
					return Err(1);
				},
			}
		}

		result
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
	async fn new(args: Args, config: Option<Config>) -> tg::Result<Self> {
		// Get the mode. If the command is `tg serve` or `tg server run`, then set the mode to `server`.
		let mode = matches!(
			args,
			Args {
				command: Command::Serve(_)
					| Command::Server(self::server::Args {
						command: self::server::Command::Run(_),
						..
					}),
				..
			}
		)
		.then_some(Mode::Server);
		let mode = mode
			.or(args.mode)
			.or(config.as_ref().and_then(|config| config.mode))
			.unwrap_or_default();

		// Create the handle.
		let handle = match mode {
			Mode::Auto => Either::Left(Self::auto(&args, config.as_ref()).await?),
			Mode::Client => Either::Left(Self::client(&args, config.as_ref()).await?),
			Mode::Server => Either::Right(Self::server(&args, config.as_ref()).await?),
		};

		// Create the CLI.
		let cli = Cli {
			args,
			config: config.clone(),
			handle,
		};

		Ok(cli)
	}

	async fn auto(args: &Args, config: Option<&Config>) -> tg::Result<tg::Client> {
		let client = Self::client(args, config).await?;

		// Attempt to connect to the server.
		client.connect().await.ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == "http+unix"
			|| matches!(client.url().host_str(), Some("localhost" | "0.0.0.0"));
		if !client.connected().await && local {
			// Start the server.
			Self::start_server(args, config).await?;

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

			if VERSION == server_version {
				break 'a;
			};

			// Disconnect.
			client.disconnect().await?;

			// Stop the server.
			Self::stop_server(args, config).await?;

			// Start the server.
			Self::start_server(args, config).await?;

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

	/// Start the server.
	async fn start_server(args: &Args, config: Option<&Config>) -> tg::Result<()> {
		// Ensure the path exists.
		let home = PathBuf::from(std::env::var("HOME").unwrap());
		let path = args
			.path
			.clone()
			.or(config.as_ref().and_then(|config| config.path.clone()))
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
	async fn stop_server(args: &Args, config: Option<&Config>) -> tg::Result<()> {
		// Get the lock file path.
		let path = args
			.path
			.clone()
			.or(config.as_ref().and_then(|config| config.path.clone()))
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

	async fn client(args: &Args, config: Option<&Config>) -> tg::Result<Client> {
		// Get the path.
		let path = args
			.path
			.clone()
			.or(config.and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = args
			.url
			.clone()
			.or(config.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the client.
		let client = tg::Client::new(url);

		Ok(client)
	}

	async fn server(args: &Args, config: Option<&Config>) -> tg::Result<Server> {
		// Get the path.
		let path = args
			.path
			.clone()
			.or(config.and_then(|config| config.path.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = args
			.url
			.clone()
			.or(config.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the advanced options.
		let preserve_temp_directories = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.preserve_temp_directories)
			.unwrap_or(false);
		let error_trace_options = config
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let file_descriptor_semaphore_size = config
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_semaphore_size)
			.unwrap_or(1024);
		let write_build_logs_to_file = config
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_file)
			.unwrap_or(false);
		let write_build_logs_to_stderr = config
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_stderr)
			.unwrap_or(false);
		let advanced = tangram_server::options::Advanced {
			error_trace_options,
			file_descriptor_semaphore_size,
			preserve_temp_directories,
			write_build_logs_to_file,
			write_build_logs_to_stderr,
		};

		// Create the authentication options.
		let authentication = config
			.and_then(|config| config.authentication.as_ref())
			.map(|authentication| {
				let providers =
					authentication.providers.as_ref().map(|providers| {
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
		let build = match config.and_then(|config| config.build.as_ref()) {
			Some(Either::Left(false)) => None,
			Some(Either::Left(true)) | None => Some(None),
			Some(Either::Right(value)) => Some(Some(value)),
		};
		let build = build.map(|build| {
			let concurrency = build
				.and_then(|build| build.concurrency)
				.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
			let heartbeat_interval = build.and_then(|build| build.heartbeat_interval).map_or(
				std::time::Duration::from_secs(1),
				std::time::Duration::from_secs_f64,
			);
			tangram_server::options::Build {
				concurrency,
				heartbeat_interval,
			}
		});

		// Create the build monitor options
		let build_monitor =
			config
				.and_then(|config| config.build_monitor.as_ref())
				.map(|build_monitor| {
					let dequeue_interval = build_monitor.dequeue_interval.unwrap_or(1);
					let dequeue_limit = build_monitor.dequeue_limit.unwrap_or(100);
					let dequeue_timeout = build_monitor.dequeue_timeout.unwrap_or(60);
					let heartbeat_interval = build_monitor.heartbeat_interval.unwrap_or(1);
					let heartbeat_limit = build_monitor.heartbeat_limit.unwrap_or(100);
					let heartbeat_timeout = build_monitor.heartbeat_timeout.unwrap_or(60);
					tangram_server::options::BuildMonitor {
						dequeue_interval: std::time::Duration::from_secs(dequeue_interval),
						dequeue_limit,
						dequeue_timeout: std::time::Duration::from_secs(dequeue_timeout),
						heartbeat_interval: std::time::Duration::from_secs(heartbeat_interval),
						heartbeat_limit,
						heartbeat_timeout: std::time::Duration::from_secs(heartbeat_timeout),
					}
				});

		// Create the database options.
		let database = config
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
		let messenger = config
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

		// Create the remote options.
		let remotes = config
			.and_then(|config| config.remotes.as_ref())
			.map(|remotes| {
				remotes
					.iter()
					.map(|remote| {
						let build = remote.build.unwrap_or_default();
						let url = remote.url.clone();
						let client = tg::Client::new(url);
						let remote = tangram_server::options::Remote { build, client };
						Ok::<_, tg::Error>(remote)
					})
					.collect()
			})
			.transpose()?;
		let remotes = if let Some(remotes) = remotes {
			remotes
		} else {
			let build = false;
			let url = Url::parse("https://api.tangram.dev").unwrap();
			let client = tg::Client::new(url);
			let remote = tangram_server::options::Remote { build, client };
			vec![remote]
		};

		// Get the version.
		let version = Some(VERSION.to_owned());

		// Create the vfs options.
		let vfs = config.and_then(|config| config.vfs).unwrap_or(true);

		// Create the options.
		let options = tangram_server::Options {
			advanced,
			authentication,
			build,
			build_monitor,
			database,
			messenger,
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
			Command::Log(args) => self.command_build_log(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::New(args) => self.command_package_new(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Outdated(args) => self.command_package_outdated(args).boxed(),
			Command::Package(args) => self.command_package(args).boxed(),
			Command::Publish(args) => self.command_package_publish(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Root(args) => self.command_root(args).boxed(),
			Command::Run(args) => self.command_run(args).boxed(),
			Command::Search(args) => self.command_package_search(args).boxed(),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
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

	async fn print_error<H>(handle: Option<H>, error: &tg::Error, config: Option<&Config>)
	where
		H: tg::Handle,
	{
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
			eprintln!("{} {}", "->".red(), error.message);
			if let Some(location) = &error.location {
				if !location.source.is_internal() || trace.options.internal {
					let source = match &location.source {
						tg::error::Source::Internal { path } => {
							path.components().iter().skip(1).join("/")
						},
						tg::error::Source::External { package, path } => {
							let source = if let Some(handle) = handle.as_ref() {
								Self::get_description_for_package(handle, package)
									.await
									.ok()
									.unwrap_or_else(|| package.to_string())
							} else {
								package.to_string()
							};
							if let Some(path) = path {
								let path = path.components().iter().skip(1).join("/");
								format!("{source}:{path}")
							} else {
								source.to_string()
							}
						},
					};
					let mut string = String::new();
					let line = location.line + 1;
					let column = location.column + 1;
					write!(string, "{source}:{line}:{column}").unwrap();
					if let Some(symbol) = &location.symbol {
						write!(string, " {symbol}").unwrap();
					}
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
			match &location.module {
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
				| tg::Module::Ts(tg::module::Js::PackageArtifact(package_artifact)) => {
					let id = package_artifact.artifact.clone();
					let artifact = tg::Artifact::with_id(id.clone());
					let metadata = tg::package::try_get_metadata(&self.handle, &artifact)
						.await
						.ok();
					if let Some(metadata) = metadata {
						let (name, version) = metadata
							.map(|metadata| (metadata.name, metadata.version))
							.unwrap_or_default();
						let name = name.as_deref().unwrap_or("<unknown>");
						let version = version.as_deref().unwrap_or("<unknown>");
						write!(string, "{name}@{version}").unwrap();
					} else {
						write!(string, "{id}").unwrap();
					}
				},

				tg::Module::Js(tg::module::Js::PackagePath(package_path))
				| tg::Module::Ts(tg::module::Js::PackagePath(package_path)) => {
					let path = package_path.package_path.join(&package_path.path);
					let path = path.display();
					write!(string, "{path}").unwrap();
				},

				tg::Module::Artifact(tg::module::Artifact::Path(path))
				| tg::Module::Directory(tg::module::Directory::Path(path))
				| tg::Module::File(tg::module::File::Path(path))
				| tg::Module::Symlink(tg::module::Symlink::Path(path)) => {
					let path = path.clone();
					write!(string, "{path}").unwrap();
				},

				_ => (),
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

	async fn get_description_for_package<H>(
		handle: &H,
		package: &tg::artifact::Id,
	) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let dependency = tg::Dependency::with_id(package.clone());
		let arg = tg::package::get::Arg {
			metadata: true,
			path: true,
			..Default::default()
		};
		let output = handle
			.get_package(&dependency, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package"))?;
		if let Some(path) = output.path {
			return Ok(path.components().iter().skip(1).join("/"));
		}
		let (name, version) = output
			.metadata
			.as_ref()
			.map(|metadata| (metadata.name.as_deref(), metadata.version.as_deref()))
			.unwrap_or_default();
		let name = name.unwrap_or("<unknown>");
		let version = version.unwrap_or("<unknown>");
		Ok(format!("{name}@{version}"))
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
		let format_layer = config
			.as_ref()
			.and_then(|config| config.tracing.as_ref())
			.map(|tracing| {
				let filter =
					tracing_subscriber::filter::EnvFilter::try_new(&tracing.filter).unwrap();
				let layer = tracing_subscriber::fmt::layer()
					.with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
					.with_writer(std::io::stderr);
				let layer = match tracing
					.format
					.unwrap_or(self::config::TracingFormat::Pretty)
				{
					self::config::TracingFormat::Compact => layer.compact().boxed(),
					self::config::TracingFormat::Json => layer.json().boxed(),
					self::config::TracingFormat::Pretty => layer.pretty().boxed(),
				};
				layer.with_filter(filter)
			});
		tracing_subscriber::registry()
			.with(console_layer)
			.with(format_layer)
			.init();
	}

	fn set_file_descriptor_limit(config: Option<&Config>) -> tg::Result<()> {
		if let Some(file_descriptor_limit) = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.file_descriptor_limit)
		{
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
		}
		Ok(())
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
