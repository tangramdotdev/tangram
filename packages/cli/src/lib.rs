use self::config::Config;
use clap::{CommandFactory as _, Parser as _};
use crossterm::{style::Stylize as _, tty::IsTty as _};
use futures::FutureExt as _;
use num::ToPrimitive as _;
use std::{fmt::Write as _, path::PathBuf, sync::Mutex, time::Duration};
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
mod clean;
mod config;
mod get;
mod health;
mod lsp;
mod object;
mod package;
mod progress;
mod pull;
mod push;
mod remote;
mod server;
mod tag;
mod tangram;
mod target;
mod tree;
mod view;

pub mod test;

pub struct Cli {
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
	name = "tangram",
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
	#[arg(short, long, env = "TANGRAM_URL")]
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

	#[command(alias = "b")]
	Build(self::build::Args),

	Cat(self::cat::Args),

	Check(self::package::check::Args),

	#[command(alias = "ci")]
	Checkin(self::artifact::checkin::Args),

	#[command(alias = "co")]
	Checkout(self::artifact::checkout::Args),

	Checksum(self::checksum::Args),

	Clean(self::clean::Args),

	#[command(alias = "doc")]
	Document(self::package::document::Args),

	Download(self::blob::download::Args),

	Export(self::object::export::Args),

	Format(self::package::format::Args),

	Get(self::get::Args),

	Health(self::health::Args),

	Import(self::object::import::Args),

	Init(self::package::init::Args),

	#[command(alias = "ls")]
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

	#[command(alias = "self")]
	Tangram(self::tangram::Args),

	Target(self::target::Args),

	Tree(self::tree::Args),

	Update(self::package::update::Args),

	View(self::view::Args),
}

impl Cli {
	#[must_use]
	pub fn main() -> std::process::ExitCode {
		// Parse the args.
		let args = Args::parse();

		// Read the config.
		let config = match Cli::read_config(args.config.clone()) {
			Ok(config) => config,
			Err(error) => {
				eprintln!("{} failed to read the config", "error".red().bold());
				Cli::print_error(&error, None);
				return 1.into();
			},
		};

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

			// If the command is anything else under `tg server`, then set the mode to `client`.
			Args {
				command: Command::Server(_),
				..
			} => Mode::Client,

			_ => args.mode.unwrap_or_default(),
		};

		// Handle server mode initialization.
		if matches!(mode, Mode::Server) {
			// Initialize V8.
			Cli::initialize_v8();

			// Set the file descriptor limit.
			Cli::set_file_descriptor_limit()
				.inspect_err(|_| {
					eprintln!(
						"{} failed to set the file descriptor limit",
						"warning".yellow().bold(),
					);
				})
				.ok();
		}

		// Initialize tracing.
		Cli::initialize_tracing(config.as_ref());

		// Create the CLI.
		let cli = Cli {
			args,
			config,
			handle,
			mode,
		};

		// Create the tokio runtime and block on the future.
		let mut builder = tokio::runtime::Builder::new_multi_thread();
		builder.enable_all();
		let runtime = builder.build().unwrap();

		// Run the command.
		let result = runtime.block_on(cli.command(cli.args.command.clone()));

		// Drop the handle.
		runtime.block_on(async {
			let handle = cli.handle.lock().unwrap().take();
			if let Some(Either::Right(server)) = handle {
				server.stop();
				server.wait().await;
			}
		});

		// Handle the result.
		let code = match result {
			Ok(()) => 0.into(),
			Err(error) => {
				eprintln!("{} failed to run the command", "error".red().bold());
				Cli::print_error(&error, cli.config.as_ref());
				1.into()
			},
		};

		code
	}

	async fn handle(&self) -> tg::Result<Either<Client, Server>> {
		// If the handle has already been created, then return it.
		if let Some(handle) = self.handle.lock().unwrap().clone() {
			return Ok(handle);
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

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
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

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				if client.connect().await.is_ok() {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
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

		// Try to connect for up to one second. If the client is still not connected, then return an error.
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			if client.connect().await.is_ok() {
				break;
			}
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}
		if !client.connected().await {
			return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
		}

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

		// Create the default config.
		let database = tangram_server::config::Database::Sqlite(
			tangram_server::config::SqliteDatabase::with_path(path.join("database")),
		);
		let remotes = [(
			"default".to_owned(),
			tangram_server::config::Remote {
				url: "https://api.tangram.dev".parse().unwrap(),
			},
		)]
		.into();
		let url = tangram_server::Config::default_url_for_path(&path);
		let vfs = if cfg!(target_os = "linux") {
			Some(tangram_server::config::Vfs::default())
		} else {
			None
		};
		let mut config = tangram_server::Config {
			advanced: tangram_server::config::Advanced::default(),
			authentication: None,
			build: Some(tangram_server::config::Build::default()),
			build_heartbeat_monitor: Some(tangram_server::config::BuildHeartbeatMonitor::default()),
			build_indexer: Some(tangram_server::config::BuildIndexer::default()),
			database,
			messenger: tangram_server::config::Messenger::default(),
			object_indexer: Some(tangram_server::config::ObjectIndexer::default()),
			path,
			remotes,
			url,
			version: None,
			vfs,
		};

		// Set the url.
		if let Some(url) = self
			.args
			.url
			.clone()
			.or(self.config.as_ref().and_then(|config| config.url.clone()))
		{
			config.url = url;
		}

		// Set the advanced options.
		if let Some(advanced) = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
		{
			if let Some(build_dequeue_timeout) = advanced.build_dequeue_timeout {
				config.advanced.build_dequeue_timeout = build_dequeue_timeout;
			}
			if let Some(error_trace_options) = advanced.error_trace_options.clone() {
				config.advanced.error_trace_options = error_trace_options;
			}
			if let Some(file_descriptor_semaphore_size) = advanced.file_descriptor_semaphore_size {
				config.advanced.file_descriptor_semaphore_size = file_descriptor_semaphore_size;
			}
			if let Some(preserve_temp_directories) = advanced.preserve_temp_directories {
				config.advanced.preserve_temp_directories = preserve_temp_directories;
			}
			if let Some(write_blobs_to_blobs_directory) = advanced.write_blobs_to_blobs_directory {
				config.advanced.write_blobs_to_blobs_directory = write_blobs_to_blobs_directory;
			}
			if let Some(write_build_logs_to_database) = advanced.write_build_logs_to_database {
				config.advanced.write_build_logs_to_database = write_build_logs_to_database;
			}
			if let Some(write_build_logs_to_stderr) = advanced.write_build_logs_to_stderr {
				config.advanced.write_build_logs_to_stderr = write_build_logs_to_stderr;
			}
		}

		// Set the authentication options.
		match self
			.config
			.as_ref()
			.and_then(|config| config.authentication.as_ref())
		{
			None => (),
			Some(None) => {
				config.authentication = None;
			},
			Some(Some(authentication)) => {
				let mut authentication_ = tangram_server::config::Authentication::default();
				if let Some(providers) = authentication.providers.as_ref() {
					if let Some(github) = providers.github.as_ref() {
						authentication_.providers.github = Some(tangram_server::config::Oauth {
							auth_url: github.auth_url.clone(),
							client_id: github.client_id.clone(),
							client_secret: github.client_secret.clone(),
							redirect_url: github.redirect_url.clone(),
							token_url: github.token_url.clone(),
						});
					}
				}
				config.authentication = Some(authentication_);
			},
		}

		// Set the build options.
		match self.config.as_ref().and_then(|config| config.build.clone()) {
			None => (),
			Some(None) => {
				config.build = None;
			},
			Some(Some(build)) => {
				let mut build_ = tangram_server::config::Build::default();
				if let Some(concurrency) = build.concurrency {
					build_.concurrency = concurrency;
				}
				if let Some(heartbeat_interval) = build.heartbeat_interval {
					build_.heartbeat_interval = heartbeat_interval;
				}
				if let Some(max_depth) = build.max_depth {
					build_.max_depth = max_depth;
				}
				if let Some(remotes) = build.remotes.clone() {
					build_.remotes = remotes;
				}
				config.build = Some(build_);
			},
		}

		// Set the build heartbeat monitor options.
		match self
			.config
			.as_ref()
			.and_then(|config| config.build_heartbeat_monitor.clone())
		{
			None => (),
			Some(None) => {
				config.build_heartbeat_monitor = None;
			},
			Some(Some(build_heartbeat_monitor)) => {
				let mut build_heartbeat_monitor_ =
					tangram_server::config::BuildHeartbeatMonitor::default();
				if let Some(interval) = build_heartbeat_monitor.interval {
					build_heartbeat_monitor_.interval = interval;
				}
				if let Some(limit) = build_heartbeat_monitor.limit {
					build_heartbeat_monitor_.limit = limit;
				}
				if let Some(timeout) = build_heartbeat_monitor.timeout {
					build_heartbeat_monitor_.timeout = timeout;
				}
				config.build_heartbeat_monitor = Some(build_heartbeat_monitor_);
			},
		}

		// Set the build indexer options.
		match self
			.config
			.as_ref()
			.and_then(|config| config.build_indexer.clone())
		{
			None => (),
			Some(None) => {
				config.build_indexer = None;
			},
			Some(Some(_build_indexer)) => {
				let build_indexer_ = tangram_server::config::BuildIndexer::default();
				config.build_indexer = Some(build_indexer_);
			},
		}

		// Set the database options.
		if let Some(database) = self
			.config
			.as_ref()
			.and_then(|config| config.database.clone())
		{
			config.database = match database {
				self::config::Database::Sqlite(database) => {
					let mut database_ =
						tangram_server::config::SqliteDatabase::with_path(config.path.clone());
					if let Some(connections) = database.connections {
						database_.connections = connections;
					}
					if let Some(path) = database.path {
						database_.path = path;
					}
					tangram_server::config::Database::Sqlite(database_)
				},
				self::config::Database::Postgres(database) => {
					let mut database_ = tangram_server::config::PostgresDatabase::default();
					if let Some(connections) = database.connections {
						database_.connections = connections;
					}
					if let Some(url) = database.url {
						database_.url = url;
					}
					tangram_server::config::Database::Postgres(database_)
				},
			};
		}

		// Set the messenger options.
		if let Some(messenger) = self
			.config
			.as_ref()
			.and_then(|config| config.messenger.clone())
		{
			config.messenger = match messenger {
				self::config::Messenger::Memory => tangram_server::config::Messenger::Memory,
				self::config::Messenger::Nats(messenger) => {
					let mut messenger_ = tangram_server::config::NatsMessenger::default();
					if let Some(url) = messenger.url {
						messenger_.url = url;
					}
					tangram_server::config::Messenger::Nats(messenger_)
				},
			}
		}

		// Set the object indexer options.
		match self
			.config
			.as_ref()
			.and_then(|config| config.object_indexer.clone())
		{
			None => (),
			Some(None) => {
				config.object_indexer = None;
			},
			Some(Some(object_indexer)) => {
				let mut object_indexer_ = tangram_server::config::ObjectIndexer::default();
				if let Some(batch_size) = object_indexer.batch_size {
					object_indexer_.batch_size = batch_size;
				}
				if let Some(timeout) = object_indexer.timeout {
					object_indexer_.timeout = timeout;
				}
				config.object_indexer = Some(object_indexer_);
			},
		}

		// Set the remote options.
		match self
			.config
			.as_ref()
			.and_then(|config| config.remotes.as_ref())
		{
			None => (),
			Some(None) => config.remotes.clear(),
			Some(Some(remotes)) => {
				for (name, remote) in remotes {
					match remote {
						None => {
							config.remotes.remove(name);
						},
						Some(remote) => {
							let name = name.clone();
							let url = remote.url.clone();
							let remote = tangram_server::config::Remote { url };
							config.remotes.insert(name, remote);
						},
					}
				}
			},
		}

		// Set the vfs options.
		match self.config.as_ref().and_then(|config| config.vfs.clone()) {
			None => (),
			Some(None) => {
				config.vfs = None;
			},
			Some(Some(vfs)) => {
				let mut vfs_ = tangram_server::config::Vfs::default();
				if let Some(cache_ttl) = vfs.cache_ttl {
					vfs_.cache_ttl = cache_ttl;
				}
				if let Some(cache_size) = vfs.cache_size {
					vfs_.cache_size = cache_size;
				}
				if let Some(database_connections) = vfs.database_connections {
					vfs_.database_connections = database_connections;
				}
				config.vfs = Some(vfs_);
			},
		}

		// Start the server.
		let server = tangram_server::Server::start(config)
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

		// Create files for stdout and stderr.
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

		// Get the path to the current executable.
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;

		// Spawn the server.
		tokio::process::Command::new(executable)
			.args(["serve"])
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

		// Wait up to one second for the server to exit.
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}

		// If the server has still not exited, then send SIGTERM to the server.
		let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
		if ret != 0 {
			return Err(tg::error!("failed to send SIGTERM to the server"));
		}

		// Wait up to one second for the server to exit.
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			// If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				return Ok(());
			}

			// Otherwise, sleep.
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	// Run the command.
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
			Command::Clean(args) => self.command_clean(args).boxed(),
			Command::Document(args) => self.command_package_document(args).boxed(),
			Command::Download(args) => self.command_blob_download(args).boxed(),
			Command::Export(args) => self.command_object_export(args).boxed(),
			Command::Format(args) => self.command_package_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Health(args) => self.command_health(args).boxed(),
			Command::Import(args) => self.command_object_import(args).boxed(),
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
			Command::Tangram(args) => self.command_tangram(args).boxed(),
			Command::Target(args) => self.command_target(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_package_update(args).boxed(),
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

	fn print_error(error: &tg::Error, config: Option<&Config>) {
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

	fn print_diagnostic(diagnostic: &tg::Diagnostic) {
		let title = match diagnostic.severity {
			tg::diagnostic::Severity::Error => "error".red().bold(),
			tg::diagnostic::Severity::Warning => "warning".yellow().bold(),
			tg::diagnostic::Severity::Info => "info".blue().bold(),
			tg::diagnostic::Severity::Hint => "hint".cyan().bold(),
		};
		eprintln!("{title}: {}", diagnostic.message);
		let mut string = String::new();
		if let Some(location) = &diagnostic.location {
			match &location.module.referent.item {
				tg::module::Item::Path(path) => {
					write!(string, "{}", path.display()).unwrap();
				},
				tg::module::Item::Object(object) => {
					write!(string, "{object}").unwrap();
				},
			}
			if let Some(path) = &location.module.referent.subpath {
				write!(string, ":{}", path.display()).unwrap();
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

	async fn output_json<T>(output: &T, pretty: Option<bool>) -> tg::Result<()>
	where
		T: serde::Serialize,
	{
		let mut stdout = tokio::io::stdout();
		let pretty = pretty.unwrap_or(stdout.is_tty());
		let json = if pretty {
			serde_json::to_string_pretty(output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
		} else {
			serde_json::to_string(output)
				.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
		};
		stdout
			.write_all(json.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write the output"))?;
		if pretty {
			stdout
				.write_all(b"\n")
				.await
				.map_err(|source| tg::error!(!source, "failed to write"))?;
		}
		Ok(())
	}

	async fn get_reference(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<Either<tg::Build, tg::Object>>> {
		let handle = self.handle().await?;
		let mut item = reference.item().clone();
		let mut options = reference.options().cloned();
		if let tg::reference::Item::Path(path) = &mut item {
			*path = std::path::absolute(&path)
				.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;
		}
		if let Some(path) = options.as_mut().and_then(|options| options.path.as_mut()) {
			*path = std::path::absolute(&path)
				.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;
		}
		let reference = tg::Reference::with_item_and_options(&item, options.as_ref());
		let referent = reference.get(&handle).await?;
		Ok(referent)
	}

	/// Initialize V8.
	fn initialize_v8() {
		// Set the ICU data.
		v8::icu::set_common_data_74(deno_core_icudata::ICU_DATA).unwrap();

		// Initialize the platform.
		let platform = v8::new_default_platform(0, true);
		v8::V8::initialize_platform(platform.make_shared());

		// Initialize V8.
		v8::V8::initialize();
	}

	/// Initialize tracing.
	fn initialize_tracing(config: Option<&Config>) {
		let console_layer = if config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.is_some_and(|advanced| advanced.tokio_console)
		{
			Some(console_subscriber::spawn())
		} else {
			None
		};
		let default = crate::config::Tracing {
	    filter: "tangram=info,tangram_client=info,tangram_database=info,tangram_server=info,tangram_vfs=info".to_owned(),
	    format: Some(crate::config::TracingFormat::Pretty),
		};
		let output_layer = config
			.as_ref()
			.and_then(|config| config.tracing.as_ref())
			.or(Some(&default))
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

	fn set_file_descriptor_limit() -> tg::Result<()> {
		let mut rlimit_nofile = libc::rlimit {
			rlim_cur: 0,
			rlim_max: 0,
		};
		let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the file descriptor limit"
			));
		}
		rlimit_nofile.rlim_cur = rlimit_nofile.rlim_max;
		let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the file descriptor limit"
			));
		}
		Ok(())
	}
}
