use anstream::eprintln;
use clap::{CommandFactory as _, Parser as _};
use crossterm::{style::Stylize as _, tty::IsTty as _};
use futures::FutureExt as _;
use num::ToPrimitive as _;
use std::{
	io::IsTerminal as _,
	path::{Path, PathBuf},
	time::Duration,
};
use tangram_client::{self as tg, Client, prelude::*};
use tangram_either::Either;
use tangram_server::Server;
use tokio::io::AsyncWriteExt as _;
use tracing_subscriber::prelude::*;
use url::Url;

mod archive;
mod blob;
mod build;
mod bundle;
mod cache;
mod cat;
mod check;
mod checkin;
mod checkout;
mod checksum;
mod children;
mod clean;
mod compress;
mod decompress;
mod document;
mod download;
mod error;
mod export;
mod extract;
mod format;
mod get;
mod health;
mod import;
mod index;
mod init;
mod lsp;
mod metadata;
mod new;
mod object;
mod outdated;
mod process;
mod progress;
mod pull;
mod push;
mod put;
mod remote;
mod run;
mod sandbox;
mod server;
mod tag;
mod tangram;
mod tree;
mod update;
mod util;
mod view;
mod viewer;

pub use self::config::Config;

pub mod config;
pub mod test;

pub struct Cli {
	args: Args,
	config: Option<Config>,
	exit: Option<u8>,
	handle: Option<Either<Client, Server>>,
	matches: clap::ArgMatches,
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
	#[arg(short, long)]
	config: Option<PathBuf>,

	/// Override the `directory` key in the config.
	#[arg(short, long)]
	directory: Option<PathBuf>,

	/// The mode.
	#[arg(short, long)]
	mode: Option<Mode>,

	/// Whether to show progress and other helpful information.
	#[arg(short, long)]
	quiet: bool,

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

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
	Archive(self::archive::Args),

	Blob(self::blob::Args),

	#[command(alias = "b")]
	Build(self::build::Args),

	Bundle(self::bundle::Args),

	Cache(self::cache::Args),

	Cancel(self::process::cancel::Args),

	Cat(self::cat::Args),

	Check(self::check::Args),

	#[command(alias = "ci")]
	Checkin(self::checkin::Args),

	#[command(alias = "co")]
	Checkout(self::checkout::Args),

	Checksum(self::checksum::Args),

	Children(self::children::Args),

	Clean(self::clean::Args),

	Compress(self::compress::Args),

	Decompress(self::decompress::Args),

	#[command(alias = "doc")]
	Document(self::document::Args),

	Download(self::download::Args),

	Export(self::export::Args),

	Extract(self::extract::Args),

	Format(self::format::Args),

	Get(self::get::Args),

	Health(self::health::Args),

	Import(self::import::Args),

	Index(self::index::Args),

	Init(self::init::Args),

	#[command(alias = "ls")]
	List(self::tag::list::Args),

	Log(self::process::log::Args),

	Lsp(self::lsp::Args),

	Metadata(self::metadata::Args),

	New(self::new::Args),

	Object(self::object::Args),

	Outdated(self::outdated::Args),

	Output(self::process::output::Args),

	Process(self::process::Args),

	Pull(self::pull::Args),

	Push(self::push::Args),

	Put(self::put::Args),

	Remote(self::remote::Args),

	#[command(alias = "r")]
	Run(self::run::Args),

	#[command(hide = true)]
	Sandbox(self::sandbox::Args),

	Serve(self::server::run::Args),

	Server(self::server::Args),

	#[command(alias = "kill")]
	Signal(self::process::signal::Args),

	Spawn(self::process::spawn::Args),

	Status(self::process::status::Args),

	Tag(self::tag::Args),

	#[command(name = "self")]
	Tangram(self::tangram::Args),

	#[command(hide = true)]
	Tree(self::tree::Args),

	Update(self::update::Args),

	View(self::view::Args),

	Wait(self::process::wait::Args),
}

impl Cli {
	#[must_use]
	pub fn main() -> std::process::ExitCode {
		// Parse the args.
		let args = Args::parse();
		let matches = Args::command().get_matches();

		// Handle the sandbox command.
		if let Command::Sandbox(args) = args.command {
			tangram_sandbox::main(args.command);
		}

		// Read the config.
		let config = match Cli::read_config(args.config.clone()) {
			Ok(config) => config,
			Err(error) => {
				eprintln!("{} failed to run the command", "error".red().bold());
				Self::print_error_basic(&error, None);
				return std::process::ExitCode::FAILURE;
			},
		};

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

		// Initialize FoundationDB.
		#[cfg(feature = "foundationdb")]
		let _fdb = if matches!(mode, Mode::Server) {
			Some(unsafe { foundationdb::boot() })
		} else {
			None
		};

		// Set the file descriptor limit.
		if matches!(mode, Mode::Server) {
			Cli::set_file_descriptor_limit()
				.inspect_err(|_| {
					if !args.quiet {
						eprintln!(
							"{} failed to set the file descriptor limit",
							"warning".yellow().bold(),
						);
					}
				})
				.ok();
		}

		// Initialize miette.
		Cli::initialize_miette();

		// Initialize tracing.
		Cli::initialize_tracing(config.as_ref());

		// Initialize V8.
		if matches!(mode, Mode::Server) {
			Cli::initialize_v8();
		}

		// Create the tokio runtime.
		let mut builder = tokio::runtime::Builder::new_multi_thread();
		builder.enable_all();
		let runtime = builder.build().unwrap();

		// Create the CLI.
		let mut cli = Cli {
			args,
			config,
			exit: None,
			handle: None,
			matches,
			mode,
		};

		// Run the command.
		let result = runtime.block_on(cli.command(cli.args.clone()).boxed());

		// Handle the result.
		let exit = match result {
			Ok(()) => cli.exit.unwrap_or_default().into(),
			Err(error) => {
				eprintln!("{} failed to run the command", "error".red().bold());
				runtime.block_on(async {
					cli.print_error(&error, None).await;
				});
				std::process::ExitCode::FAILURE
			},
		};

		// Drop the handle.
		runtime.block_on(async {
			let handle = cli.handle.take();
			match handle {
				Some(Either::Left(client)) => {
					client
						.disconnect()
						.await
						.inspect_err(|error| eprintln!("failed to disconnect: {error}"))
						.ok();
				},
				Some(Either::Right(server)) => {
					server.stop();
					server.wait().await;
				},
				None => (),
			}
		});

		// Drop the runtime.
		drop(runtime);

		exit
	}

	async fn handle(&mut self) -> tg::Result<Either<Client, Server>> {
		// If the handle has already been created, then return it.
		if let Some(handle) = self.handle.clone() {
			return Ok(handle);
		}

		// Create the handle.
		let handle = match self.mode {
			Mode::Auto => Either::Left(self.auto().await?),
			Mode::Client => Either::Left(self.client().await?),
			Mode::Server => Either::Right(self.server().await?),
		};

		// Set the handle.
		self.handle.replace(handle.clone());

		// Get the health and print diagnostics.
		let health = handle.health().await?;
		if !self.args.quiet {
			for diagnostic in &health.diagnostics {
				self.print_diagnostic(diagnostic).await;
			}
		}

		Ok(handle)
	}

	async fn auto(&self) -> tg::Result<Client> {
		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.http.as_ref())
				.and_then(|config| config.as_ref().right())
				.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = self
					.args
					.directory
					.clone()
					.or(self
						.config
						.as_ref()
						.and_then(|config| config.directory.clone()))
					.unwrap_or_else(|| {
						PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram")
					});
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the client.
		let client = tg::Client::new(url, Some(version()));

		// Attempt to connect to the server.
		let mut connected = client.connect().await.is_ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == "http+unix"
			|| matches!(client.url().host_str(), Some("localhost" | "0.0.0.0"));
		if !connected && local {
			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				connected = client.connect().await.is_ok();
				if connected {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
			if !connected {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		// If the URL is local and the server's version is different from the client, then disconnect and restart the server.
		'a: {
			if !local {
				break 'a;
			}

			let health = client.health().await?;
			let Some(server_version) = &health.version else {
				break 'a;
			};

			if &version() == server_version {
				break 'a;
			}

			// Disconnect.
			client.disconnect().await?;

			// Stop the server.
			self.stop_server().await?;

			// Start the server.
			self.start_server().await?;

			// Try to connect for up to one second. If the client is still not connected, then return an error.
			for duration in [10, 20, 30, 50, 100, 300, 500] {
				connected = client.connect().await.is_ok();
				if connected {
					break;
				}
				tokio::time::sleep(Duration::from_millis(duration)).await;
			}
			if !connected {
				return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
			}
		}

		Ok(client)
	}

	async fn client(&self) -> tg::Result<Client> {
		// Get the url.
		let url = self
			.args
			.url
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.http.as_ref())
				.and_then(|config| config.as_ref().right())
				.and_then(|config| config.url.clone()))
			.unwrap_or_else(|| {
				let path = self
					.args
					.directory
					.clone()
					.or(self
						.config
						.as_ref()
						.and_then(|config| config.directory.clone()))
					.unwrap_or_else(|| {
						PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram")
					});
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the client.
		let client = tg::Client::new(url, Some(version()));

		// Try to connect for up to one second. If the client is still not connected, then return an error.
		let mut connected = false;
		for duration in [10, 20, 30, 50, 100, 300, 500] {
			connected = client.connect().await.is_ok();
			if connected {
				break;
			}
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}
		if !connected {
			return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
		}

		Ok(client)
	}

	async fn server(&self) -> tg::Result<Server> {
		// Create the default config.
		let directory = self
			.args
			.directory
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.directory.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		let parallelism = std::thread::available_parallelism().unwrap().into();
		let advanced = tangram_server::config::Advanced::default();
		let authentication = None;
		let cleaner = None;
		let database =
			tangram_server::config::Database::Sqlite(tangram_server::config::SqliteDatabase {
				connections: parallelism,
				path: directory.join("database"),
			});
		let index = tangram_server::config::Index::Sqlite(tangram_server::config::SqliteIndex {
			connections: parallelism,
			path: directory.join("index"),
		});
		let http = Some(tangram_server::config::Http {
			url: self.args.url.clone(),
		});
		let indexer = Some(tangram_server::config::Indexer::default());
		let messenger = tangram_server::config::Messenger::default();
		let remotes = None;
		let runner = Some(tangram_server::config::Runner {
			concurrency: parallelism,
			heartbeat_interval: Duration::from_secs(1),
			remotes: Vec::new(),
		});

		// Create the runtimes.
		let runtimes = if cfg!(target_os = "linux") {
			let name = "linux".to_owned();
			let executable = std::env::current_exe()
				.map_err(|source| tg::error!(!source, "failed to get the executable path"))?;
			let args = vec!["sandbox".to_owned()];
			[(
				name,
				tangram_server::config::Runtime {
					kind: tangram_server::config::RuntimeKind::Tangram,
					executable,
					args,
				},
			)]
			.into_iter()
			.collect()
		} else if cfg!(target_os = "macos") {
			let name = "darwin".to_owned();
			let executable = std::env::current_exe()
				.map_err(|source| tg::error!(!source, "failed to get the executable path"))?;
			let args = vec!["sandbox".to_owned()];
			[(
				name,
				tangram_server::config::Runtime {
					kind: tangram_server::config::RuntimeKind::Tangram,
					executable,
					args,
				},
			)]
			.into_iter()
			.collect()
		} else {
			[].into_iter().collect()
		};

		let store = tangram_server::config::Store::Lmdb(tangram_server::config::LmdbStore {
			path: directory.join("store"),
		});
		let version = Some(version());
		let vfs = if cfg!(target_os = "linux") {
			Some(tangram_server::config::Vfs::default())
		} else {
			None
		};
		let watchdog = Some(tangram_server::config::Watchdog::default());
		let mut config = tangram_server::Config {
			advanced,
			authentication,
			cleaner,
			database,
			http,
			index,
			indexer,
			messenger,
			directory,
			remotes,
			runner,
			runtimes,
			store,
			version,
			vfs,
			watchdog,
		};

		// Set the advanced options.
		if let Some(advanced) = self
			.config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
		{
			if let Some(process_dequeue_timeout) = advanced.process_dequeue_timeout {
				config.advanced.process_dequeue_timeout = process_dequeue_timeout;
			}
			if let Some(preserve_temp_directories) = advanced.preserve_temp_directories {
				config.advanced.preserve_temp_directories = preserve_temp_directories;
			}
			if let Some(shared_directory) = advanced.shared_directory {
				config.advanced.shared_directory = shared_directory;
			}
			if let Some(write_process_logs_to_stderr) = advanced.write_process_logs_to_stderr {
				config.advanced.write_process_logs_to_stderr = write_process_logs_to_stderr;
			}
		}

		// Set the authentication config.
		match self
			.config
			.as_ref()
			.and_then(|config| config.authentication.as_ref())
		{
			None => (),
			Some(Either::Left(false)) => {
				config.authentication = None;
			},
			Some(Either::Left(true)) => {
				config.authentication = Some(tangram_server::config::Authentication::default());
			},
			Some(Either::Right(authentication)) => {
				let mut authentication_ = config.authentication.unwrap_or_default();
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

		// Set the cleaner config.
		match self
			.config
			.as_ref()
			.and_then(|config| config.cleaner.clone())
		{
			None => (),
			Some(Either::Left(false)) => {
				config.cleaner = None;
			},
			Some(Either::Left(true)) => {
				config.cleaner = Some(tangram_server::config::Cleaner::default());
			},
			Some(Either::Right(cleaner)) => {
				let mut cleaner_ = config.cleaner.unwrap_or_default();
				if let Some(batch_size) = cleaner.batch_size {
					cleaner_.batch_size = batch_size;
				}
				if let Some(ttl) = cleaner.ttl {
					cleaner_.ttl = ttl;
				}
				config.cleaner = Some(cleaner_);
			},
		}

		// Set the database config.
		if let Some(database) = self
			.config
			.as_ref()
			.and_then(|config| config.database.clone())
		{
			config.database = match database {
				self::config::Database::Sqlite(database) => {
					let mut database_ = tangram_server::config::SqliteDatabase {
						connections: parallelism,
						path: config.directory.clone(),
					};
					if let Some(connections) = database.connections {
						database_.connections = connections;
					}
					if let Some(path) = database.path {
						database_.path = path;
					}
					tangram_server::config::Database::Sqlite(database_)
				},
				self::config::Database::Postgres(database) => {
					let mut database_ = tangram_server::config::PostgresDatabase {
						connections: parallelism,
						url: "postgres://localhost:5432".parse().unwrap(),
					};
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

		// Set the http config.
		match self.config.as_ref().and_then(|config| config.http.as_ref()) {
			None => (),
			Some(Either::Left(false)) => {
				config.http = None;
			},
			Some(Either::Left(true)) => {
				config.http = Some(tangram_server::config::Http::default());
			},
			Some(Either::Right(http)) => {
				let mut http_ = config.http.unwrap_or_default();
				if let Some(url) = http.url.clone() {
					http_.url = Some(url);
				}
				config.http = Some(http_);
			},
		}

		// Set the index config.
		if let Some(index) = self.config.as_ref().and_then(|config| config.index.clone()) {
			config.index = match index {
				self::config::Index::Sqlite(index) => {
					let mut index_ = tangram_server::config::SqliteIndex {
						connections: parallelism,
						path: config.directory.clone(),
					};
					if let Some(connections) = index.connections {
						index_.connections = connections;
					}
					if let Some(path) = index.path {
						index_.path = path;
					}
					tangram_server::config::Index::Sqlite(index_)
				},
				self::config::Index::Postgres(index) => {
					let mut database_ = tangram_server::config::PostgresIndex {
						connections: parallelism,
						url: "postgres://localhost:5432".parse().unwrap(),
					};
					if let Some(connections) = index.connections {
						database_.connections = connections;
					}
					if let Some(url) = index.url {
						database_.url = url;
					}
					tangram_server::config::Index::Postgres(database_)
				},
			};
		}

		// Set the indexer config.
		match self
			.config
			.as_ref()
			.and_then(|config| config.indexer.clone())
		{
			None => (),
			Some(Either::Left(false)) => {
				config.indexer = None;
			},
			Some(Either::Left(true)) => {
				config.indexer = Some(tangram_server::config::Indexer::default());
			},
			Some(Either::Right(indexer)) => {
				let mut indexer_ = config.indexer.unwrap_or_default();
				if let Some(message_batch_size) = indexer.message_batch_size {
					indexer_.message_batch_size = message_batch_size;
				}
				if let Some(message_batch_timeout) = indexer.message_batch_timeout {
					indexer_.message_batch_timeout = message_batch_timeout;
				}
				if let Some(insert_batch_size) = indexer.insert_batch_size {
					indexer_.insert_batch_size = insert_batch_size;
				}
				config.indexer = Some(indexer_);
			},
		}

		// Set the messenger config.
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

		// Set the remotes config.
		if let Some(remotes) = self
			.config
			.as_ref()
			.and_then(|config| config.remotes.as_ref())
		{
			config.remotes = Some(
				remotes
					.iter()
					.map(|remote| tangram_server::config::Remote {
						name: remote.name.clone(),
						url: remote.url.clone(),
					})
					.collect(),
			);
		}

		// Set the runner config.
		match self
			.config
			.as_ref()
			.and_then(|config| config.runner.clone())
		{
			None => (),
			Some(Either::Left(false)) => {
				config.runner = None;
			},
			Some(Either::Left(true)) => {
				config.runner = Some(tangram_server::config::Runner::default());
			},
			Some(Either::Right(runner)) => {
				let mut runner_ = config.runner.unwrap_or_default();
				if let Some(concurrency) = runner.concurrency {
					runner_.concurrency = concurrency;
				}
				if let Some(heartbeat_interval) = runner.heartbeat_interval {
					runner_.heartbeat_interval = heartbeat_interval;
				}
				if let Some(remotes) = runner.remotes.clone() {
					runner_.remotes = remotes;
				}
				config.runner = Some(runner_);
			},
		}

		// Set the store config.
		if let Some(store) = self.config.as_ref().and_then(|config| config.store.clone()) {
			config.store = match store {
				#[cfg(feature = "foundationdb")]
				config::Store::Fdb(fdb) => {
					tangram_server::config::Store::Fdb(tangram_server::config::FdbStore {
						path: fdb.path,
					})
				},
				config::Store::Lmdb(lmdb) => {
					tangram_server::config::Store::Lmdb(tangram_server::config::LmdbStore {
						path: lmdb.path.unwrap_or_else(|| config.directory.join("store")),
					})
				},
				config::Store::Memory => tangram_server::config::Store::Memory,
				config::Store::S3(s3) => {
					tangram_server::config::Store::S3(tangram_server::config::S3Store {
						access_key: s3.access_key,
						bucket: s3.bucket,
						region: s3.region,
						secret_key: s3.secret_key,
						url: s3.url,
					})
				},
			};
		}

		// Set the vfs config.
		match self.config.as_ref().and_then(|config| config.vfs.clone()) {
			None => (),
			Some(Either::Left(false)) => {
				config.vfs = None;
			},
			Some(Either::Left(true)) => {
				config.vfs = Some(tangram_server::config::Vfs::default());
			},
			Some(Either::Right(vfs)) => {
				let mut vfs_ = config.vfs.unwrap_or_default();
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

		// Set the watchdog config.
		match self
			.config
			.as_ref()
			.and_then(|config| config.watchdog.clone())
		{
			None => (),
			Some(Either::Left(false)) => {
				config.watchdog = None;
			},
			Some(Either::Left(true)) => {
				config.watchdog = Some(tangram_server::config::Watchdog::default());
			},
			Some(Either::Right(watchdog)) => {
				let mut watchdog_ = config.watchdog.unwrap_or_default();
				if let Some(batch_size) = watchdog.batch_size {
					watchdog_.batch_size = batch_size;
				}
				if let Some(interval) = watchdog.interval {
					watchdog_.interval = interval;
				}
				if let Some(ttl) = watchdog.ttl {
					watchdog_.ttl = ttl;
				}
				config.watchdog = Some(watchdog_);
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
		// Ensure the directory exists.
		let directory = self
			.args
			.directory
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.directory.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Get the log file path.
		let log_path = directory.join("log");

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
			.current_dir(PathBuf::from(std::env::var("HOME").unwrap()))
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
		let directory = self
			.args
			.directory
			.clone()
			.or(self
				.config
				.as_ref()
				.and_then(|config| config.directory.clone()))
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		let lock_path = directory.join("lock");

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
			// Kill the server. If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGINT) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if error.raw_os_error() == Some(libc::ESRCH) {
					return Ok(());
				}
				return Err(tg::error!(!error, "failed to stop the server"));
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
			// Kill the server. If the server has exited, then return.
			let ret = unsafe { libc::kill(pid.to_i32().unwrap(), libc::SIGTERM) };
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				if error.raw_os_error() == Some(libc::ESRCH) {
					return Ok(());
				}
				return Err(tg::error!(!error, "failed to stop the server"));
			}

			// Otherwise, sleep.
			tokio::time::sleep(Duration::from_millis(duration)).await;
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	// Run the command.
	async fn command(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Archive(args) => self.command_archive(args).boxed(),
			Command::Blob(args) => self.command_blob(args).boxed(),
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Bundle(args) => self.command_bundle(args).boxed(),
			Command::Cache(args) => self.command_cache(args).boxed(),
			Command::Cancel(args) => self.command_process_cancel(args).boxed(),
			Command::Cat(args) => self.command_cat(args).boxed(),
			Command::Check(args) => self.command_check(args).boxed(),
			Command::Checkin(args) => self.command_checkin(args).boxed(),
			Command::Checkout(args) => self.command_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Children(args) => self.command_children(args).boxed(),
			Command::Clean(args) => self.command_clean(args).boxed(),
			Command::Compress(args) => self.command_compress(args).boxed(),
			Command::Decompress(args) => self.command_decompress(args).boxed(),
			Command::Document(args) => self.command_document(args).boxed(),
			Command::Download(args) => self.command_download(args).boxed(),
			Command::Export(args) => self.command_export(args).boxed(),
			Command::Extract(args) => self.command_extract(args).boxed(),
			Command::Format(args) => self.command_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Health(args) => self.command_health(args).boxed(),
			Command::Import(args) => self.command_import(args).boxed(),
			Command::Index(args) => self.command_index(args).boxed(),
			Command::Init(args) => self.command_init(args).boxed(),
			Command::List(args) => self.command_tag_list(args).boxed(),
			Command::Log(args) => self.command_process_log(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::Metadata(args) => self.command_metadata(args).boxed(),
			Command::New(args) => self.command_new(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Outdated(args) => self.command_outdated(args).boxed(),
			Command::Output(args) => self.command_process_output(args).boxed(),
			Command::Process(args) => self.command_process(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Put(args) => self.command_put(args).boxed(),
			Command::Remote(args) => self.command_remote(args).boxed(),
			Command::Run(args) => self.command_run(args).boxed(),
			Command::Sandbox(_) => return Err(tg::error!("unreachable")),
			Command::Serve(args) => self.command_server_run(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Signal(args) => self.command_process_signal(args).boxed(),
			Command::Spawn(args) => self.command_process_spawn(args).boxed(),
			Command::Status(args) => self.command_process_status(args).boxed(),
			Command::Tag(args) => self.command_tag(args).boxed(),
			Command::Tangram(args) => self.command_tangram(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_update(args).boxed(),
			Command::View(args) => self.command_view(args).boxed(),
			Command::Wait(args) => self.command_process_wait(args).boxed(),
		}
		.await
	}

	fn read_config(directory: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let directory = directory.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = match std::fs::read_to_string(&directory) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(source) => {
				return Err(
					tg::error!(!source, %directory = directory.display(), "failed to read the config file"),
				);
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| tg::error!(!source, %directory = directory.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	fn _write_config(config: &Config, directory: Option<PathBuf>) -> tg::Result<()> {
		let directory = directory.unwrap_or_else(|| {
			PathBuf::from(std::env::var("HOME").unwrap()).join(".config/tangram/config.json")
		});
		let config = serde_json::to_string_pretty(&config)
			.map_err(|source| tg::error!(!source, "failed to serialize the config"))?;
		std::fs::write(directory, config)
			.map_err(|source| tg::error!(!source, "failed to save the config"))?;
		Ok(())
	}

	fn print_output(value: &tg::Value) {
		let stdout = std::io::stdout();
		let output = if stdout.is_terminal() {
			let options = tg::value::print::Options {
				depth: Some(0),
				style: tg::value::print::Style::Pretty { indentation: "  " },
			};
			value.print(options)
		} else {
			value.to_string()
		};
		println!("{output}");
	}

	async fn print_json<T>(output: &T, pretty: Option<bool>) -> tg::Result<()>
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
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<Either<tg::Process, tg::Object>>> {
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
		let stream = handle.get(&reference).await?;
		let output = self.render_progress_stream(stream).await?;
		Ok(output)
	}

	async fn get_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<Either<tg::Process, tg::Object>>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let referent = self.get_reference(reference).await?;
			referents.push(referent);
		}
		Ok(referents)
	}

	async fn get_module(&mut self, reference: &tg::Reference) -> tg::Result<tg::Module> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(reference).await?;
		let item = referent
			.item
			.right()
			.ok_or_else(|| tg::error!("expected an object"))?;
		let mut referent = tg::Referent {
			item,
			path: referent.path,
			tag: referent.tag,
		};

		// If the reference's path is relative, then make the referent's path relative to the current working directory.
		referent.path = referent
			.path
			.take()
			.map(|path| {
				if reference.path().is_none_or(Path::is_absolute) {
					Ok(path)
				} else {
					let current_dir = std::env::current_dir()
						.map_err(|source| tg::error!(!source, "failed to get current dir"))?;
					crate::util::path_diff(&current_dir, &path)
				}
			})
			.transpose()?;

		let module = match referent.item.clone() {
			tg::Object::Directory(directory) => {
				let root_module_name = tg::package::try_get_root_module_file_name(
					&handle,
					Either::Left(&directory.clone().into()),
				)
				.await?
				.ok_or_else(|| tg::error!("could not determine the executable"))?;
				if let Some(path) = &mut referent.path {
					*path = path.join(root_module_name);
				} else {
					referent.path.replace(root_module_name.into());
				}
				let kind = if Path::new(root_module_name)
					.extension()
					.is_some_and(|extension| extension == "js")
				{
					tg::module::Kind::Js
				} else if Path::new(root_module_name)
					.extension()
					.is_some_and(|extension| extension == "ts")
				{
					tg::module::Kind::Ts
				} else {
					unreachable!();
				};
				let item = directory.get(&handle, root_module_name).await?;
				let item = tg::module::Item::Object(item.into());
				let referent = tg::Referent {
					item,
					path: referent.path,
					tag: referent.tag,
				};
				tg::Module { kind, referent }
			},

			tg::Object::File(file) => {
				let path = referent
					.path
					.as_ref()
					.ok_or_else(|| tg::error!("expected a path"))?;
				if !tg::package::is_module_path(path) {
					return Err(tg::error!("expected a module path"));
				}
				let kind = if path.extension().is_some_and(|extension| extension == "js") {
					tg::module::Kind::Js
				} else if path.extension().is_some_and(|extension| extension == "ts") {
					tg::module::Kind::Ts
				} else {
					unreachable!()
				};
				let item = tg::module::Item::Object(file.clone().into());
				let referent = tg::Referent {
					item,
					path: referent.path,
					tag: referent.tag,
				};
				tg::Module { kind, referent }
			},

			tg::Object::Symlink(_) => {
				return Err(tg::error!("unimplemented"));
			},

			_ => {
				return Err(tg::error!("expected an artifact"));
			},
		};
		Ok(module)
	}

	/// Initialize miette.
	fn initialize_miette() {
		let theme = miette::GraphicalTheme {
			characters: miette::ThemeCharacters::unicode(),
			styles: miette::ThemeStyles {
				error: owo_colors::style().red(),
				highlights: vec![owo_colors::style().red()],
				link: owo_colors::style().blue(),
				linum: owo_colors::style().dimmed(),
				warning: owo_colors::style().yellow(),
				..miette::ThemeStyles::none()
			},
		};
		let handler = miette::GraphicalReportHandler::new()
			.with_theme(theme)
			.without_syntax_highlighting();
		miette::set_hook(Box::new(move |_| Box::new(handler.clone()))).unwrap();
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
			filter: "tangram_cli=info,tangram_client=info,tangram_database=info,tangram_server=info,tangram_vfs=info".to_owned(),
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
		std::panic::set_hook(Box::new(|panic_info| {
			let payload = panic_info.payload();
			let payload = payload
				.downcast_ref::<&str>()
				.copied()
				.or(payload.downcast_ref::<String>().map(String::as_str));
			let location = panic_info.location().map(ToString::to_string);
			let backtrace = std::backtrace::Backtrace::force_capture();
			tracing::error!(payload, location, %backtrace, "a panic occurred");
		}));
	}

	fn set_file_descriptor_limit() -> tg::Result<()> {
		let mut rlimit_nofile = libc::rlimit {
			rlim_cur: 0,
			rlim_max: 0,
		};
		let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the file descriptor limit"
			));
		}
		rlimit_nofile.rlim_cur = rlimit_nofile.rlim_max;
		let ret = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const rlimit_nofile) };
		if ret != 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the file descriptor limit"
			));
		}
		Ok(())
	}
}
