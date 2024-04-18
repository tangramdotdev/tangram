use self::config::Config;
use clap::Parser;
use crossterm::style::Stylize;
use either::Either;
use futures::FutureExt as _;
use itertools::Itertools as _;
use std::{fmt::Write, path::PathBuf};
use tangram_client as tg;
use tangram_server::Server;
use tg::Client;
use tracing_subscriber::prelude::*;
use url::Url;

mod commands;
mod config;
mod tree;
mod tui;

pub const API_URL: &str = "https://api.tangram.dev";

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Cli {
	config: Option<Config>,
	handle: Either<Client, Server>,
}

#[derive(Debug, clap::Parser)]
#[clap(
	about = env!("CARGO_PKG_DESCRIPTION"),
	disable_help_subcommand = true,
	long_version = env!("CARGO_PKG_VERSION"),
	name = env!("CARGO_CRATE_NAME"),
	version = env!("CARGO_PKG_VERSION"),
)]
struct Args {
	/// The path to the config file.
	#[clap(long)]
	config: Option<PathBuf>,

	/// This argument controls whether the CLI runs as a client or a server. When set to `auto`, the CLI will run as a client and start a separate server process if the connection fails or the server's version does not match. If the command is `tg server run`, the mode is set to `server`.
	#[clap(short, long, default_value = "Mode::Auto")]
	mode: Mode,

	#[clap(subcommand)]
	command: Command,
}

#[derive(Clone, Debug, Default, clap::ValueEnum)]
enum Mode {
	#[default]
	Auto,
	Client,
	Server,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Build(self::commands::build::Args),
	Cat(self::commands::cat::Args),
	Check(self::commands::check::Args),
	Checkin(self::commands::checkin::Args),
	Checkout(self::commands::checkout::Args),
	Checksum(self::commands::checksum::Args),
	Clean(self::commands::clean::Args),
	Doc(self::commands::doc::Args),
	Format(self::commands::format::Args),
	Get(self::commands::get::Args),
	Init(self::commands::init::Args),
	Log(self::commands::log::Args),
	Login(self::commands::login::Args),
	Lsp(self::commands::lsp::Args),
	New(self::commands::new::Args),
	Object(self::commands::object::Args),
	Outdated(self::commands::package::OutdatedArgs),
	Publish(self::commands::package::PublishArgs),
	Package(self::commands::package::Args),
	Pull(self::commands::pull::Args),
	Push(self::commands::push::Args),
	Run(self::commands::run::Args),
	Search(self::commands::package::SearchArgs),
	Server(self::commands::server::Args),
	Tree(self::commands::tree::Args),
	Update(self::commands::package::UpdateArgs),
	Upgrade(self::commands::upgrade::Args),
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

	// Get the mode. If the command is `tg server run` then set the mode to `server`.
	let mode = if matches!(
		args.command,
		Command::Server(self::commands::server::Args {
			command: self::commands::server::Command::Run(_),
			..
		})
	) {
		Mode::Server
	} else {
		args.mode
	};

	// Create the future.
	let future = async move {
		let cli = match Cli::new(mode, config.clone()).await {
			Ok(cli) => cli,
			Err(error) => {
				eprintln!("{}: an error occurred", "error".red().bold());
				Cli::print_error(None::<Client>, &error, config.as_ref()).await;
				return Err(1);
			},
		};
		let result = cli.command(args.command).await;
		match result {
			Ok(()) => (),
			Err(error) => {
				eprintln!("{}: failed to run the command", "error".red().bold());
				Cli::print_error(Some(cli.handle), &error, config.as_ref()).await;
				return Err(1);
			},
		};
		Ok(())
	};

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	builder.disable_lifo_slot();
	let runtime = builder.build().unwrap();
	let result = runtime.block_on(future);

	// Handle the result.
	match result {
		Ok(()) => 0.into(),
		Err(code) => code.into(),
	}
}

impl Cli {
	pub async fn new(mode: Mode, config: Option<Config>) -> tg::Result<Self> {
		// Create the handle.
		let handle = match mode {
			Mode::Auto => Either::Left(Self::auto(config.as_ref()).await?),
			Mode::Client => Either::Left(Self::client(config.as_ref()).await?),
			Mode::Server => Either::Right(Self::server(config.as_ref()).await?),
		};

		// Create the CLI.
		let cli = Cli {
			config: config.clone(),
			handle,
		};

		Ok(cli)
	}

	async fn auto(config: Option<&Config>) -> tg::Result<tg::Client> {
		let client = Self::client(config).await?;

		// Attempt to connect to the server.
		let mut connected = client.connect().await.is_ok();

		// If the client is not connected and the URL is local, then start the server and attempt to connect.
		let local = client.url().scheme() == "http+unix"
			|| matches!(client.url().host_str(), Some("localhost" | "0.0.0.0"));
		if !connected && local {
			Self::start_server(config).await?;
			for _ in 0..10 {
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
				if client.connect().await.is_ok() {
					connected = true;
					break;
				}
			}
		};

		// If the client is not connected, then return an error.
		if !connected {
			return Err(tg::error!(%url = client.url(), "failed to connect to the server"));
		}

		// Check the version.
		let client_version = VERSION;
		let server_version = &client.health().await?.version;
		if let Some(server_version) = server_version {
			if client_version != server_version {
				eprintln!("{}: the client version {client_version} does not match the server version {server_version}", "warning".yellow().bold());
			}
		}

		Ok(client)
	}

	/// Start the server.
	async fn start_server(config: Option<&Config>) -> tg::Result<()> {
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;
		let path = config
			.as_ref()
			.and_then(|config| config.path.clone())
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the server path"))?;
		let stdout = tokio::fs::File::create(path.join("log"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the server log file"))?;
		let stderr = stdout
			.try_clone()
			.await
			.map_err(|source| tg::error!(!source, "failed to clone the server log file"))?;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(&path)
			.stdin(std::process::Stdio::null())
			.stdout(stdout.into_std().await)
			.stderr(stderr.into_std().await)
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the server"))?;
		Ok(())
	}

	async fn client(config: Option<&Config>) -> tg::Result<Client> {
		// Get the path.
		let path = config
			.and_then(|config| config.path.clone())
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = config
			.and_then(|config| config.url.clone())
			.unwrap_or_else(|| {
				let path = path.join("socket");
				let path = path.to_str().unwrap();
				let path = urlencoding::encode(path);
				format!("http+unix://{path}").parse().unwrap()
			});

		// Create the client.
		let client = tg::Client::new(url, None);

		Ok(client)
	}

	async fn server(config: Option<&Config>) -> tg::Result<Server> {
		// Get the path.
		let path = config
			.and_then(|config| config.path.clone())
			.unwrap_or_else(|| PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram"));

		// Get the url.
		let url = config
			.and_then(|config| config.url.clone())
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
		let write_build_logs_to_stderr = config
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.write_build_logs_to_stderr)
			.unwrap_or(false);
		let advanced = tangram_server::options::Advanced {
			error_trace_options,
			file_descriptor_semaphore_size,
			preserve_temp_directories,
			write_build_logs_to_stderr,
		};

		// Create the build options.
		let build = match config.and_then(|config| config.build.as_ref()) {
			Some(Either::Left(false)) => None,
			Some(Either::Left(true)) | None => Some(None),
			Some(Either::Right(value)) => Some(Some(value)),
		};
		let build = build.map(|build| {
			let max_concurrency = build
				.and_then(|build| build.max_concurrency)
				.unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
			tangram_server::options::Build { max_concurrency }
		});

		// Create the database options.
		let database = config
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
			.and_then(|config| config.remotes.as_ref())
			.map(|remotes| {
				remotes
					.iter()
					.map(|remote| {
						let build = remote.build.unwrap_or_default();
						let url = remote.url.clone();
						let client = tg::Client::new(url, None);
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
			let url = Url::parse(API_URL).unwrap();
			let client = tg::Client::new(url, None);
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

		Ok(server)
	}

	// Run the command
	async fn command(&self, command: Command) -> tg::Result<()> {
		match command {
			Command::Build(args) => self.command_build(args).boxed(),
			Command::Cat(args) => self.command_cat(args).boxed(),
			Command::Check(args) => self.command_check(args).boxed(),
			Command::Checkin(args) => self.command_checkin(args).boxed(),
			Command::Checkout(args) => self.command_checkout(args).boxed(),
			Command::Checksum(args) => self.command_checksum(args).boxed(),
			Command::Clean(args) => self.command_clean(args).boxed(),
			Command::Doc(args) => self.command_doc(args).boxed(),
			Command::Format(args) => self.command_format(args).boxed(),
			Command::Get(args) => self.command_get(args).boxed(),
			Command::Init(args) => self.command_init(args).boxed(),
			Command::Log(args) => self.command_log(args).boxed(),
			Command::Login(args) => self.command_login(args).boxed(),
			Command::Lsp(args) => self.command_lsp(args).boxed(),
			Command::New(args) => self.command_new(args).boxed(),
			Command::Outdated(args) => self.command_package_outdated(args).boxed(),
			Command::Object(args) => self.command_object(args).boxed(),
			Command::Package(args) => self.command_package(args).boxed(),
			Command::Publish(args) => self.command_package_publish(args).boxed(),
			Command::Pull(args) => self.command_pull(args).boxed(),
			Command::Push(args) => self.command_push(args).boxed(),
			Command::Run(args) => self.command_run(args).boxed(),
			Command::Search(args) => self.command_package_search(args).boxed(),
			Command::Server(args) => self.command_server(args).boxed(),
			Command::Tree(args) => self.command_tree(args).boxed(),
			Command::Update(args) => self.command_package_update(args).boxed(),
			Command::Upgrade(args) => self.command_upgrade(args).boxed(),
		}
		.await
	}

	fn read_config(path: Option<PathBuf>) -> tg::Result<Option<Config>> {
		let home = std::env::var("HOME")
			.map_err(|source| tg::error!(!source, "failed to get the HOME environment variable"))?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
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
		let home = std::env::var("HOME")
			.map_err(|source| tg::error!(!source, "failed to get the HOME environment variable"))?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
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
							let path = path.components().iter().skip(1).join("/");
							format!("{source}:{path}")
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

	async fn print_diagnostic(&self, diagnostic: &tg::Diagnostic) -> tg::Result<()> {
		match diagnostic.severity {
			tg::diagnostic::Severity::Error => eprintln!("{}:", "error".red().bold()),
			tg::diagnostic::Severity::Warning => eprintln!("{}:", "warning".yellow().bold()),
			tg::diagnostic::Severity::Info => eprintln!("{}:", "info".blue().bold()),
			tg::diagnostic::Severity::Hint => eprintln!("{}:", "hint".cyan().bold()),
		};
		eprint!("{} {} ", "->".red(), diagnostic.message);
		if let Some(location) = &diagnostic.location {
			let (package, path) = location.module.source();
			if let Some(package) = package {
				let package = tg::Directory::with_id(package);
				let metadata = tg::package::get_metadata(&self.handle, &package).await.ok();
				let (name, version) = metadata
					.map(|metadata| (metadata.name, metadata.version))
					.unwrap_or_default();
				let name = name.as_deref().unwrap_or("<unknown>");
				let version = version.as_deref().unwrap_or("<unknown>");
				eprint!("{name}@{version}: {path}:");
			} else {
				eprint!("{path}:");
			};
			eprint!(
				"{}:{}",
				location.range.start.line + 1,
				location.range.start.character + 1,
			);
		}
		eprintln!();
		Ok(())
	}

	async fn get_description_for_package<H>(
		handle: &H,
		package: &tg::directory::Id,
	) -> tg::Result<String>
	where
		H: tg::Handle,
	{
		let dependency = tg::Dependency::with_id(package.clone());
		let arg = tg::package::GetArg {
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
