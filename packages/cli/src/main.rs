use self::config::Config;
use clap::Parser;
use crossterm::style::Stylize;
use futures::FutureExt as _;
use itertools::Itertools as _;
use std::fmt::Write;
use std::path::PathBuf;
use tangram_client as tg;
use tg::Handle as _;
use tracing_subscriber::prelude::*;
use url::Url;

mod commands;
mod config;
mod tree;
mod tui;

pub const API_URL: &str = "https://api.tangram.dev";

struct Cli {
	url: Url,
	config: Option<Config>,
	client: tokio::sync::Mutex<Option<tg::Client>>,
	version: String,
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
	/// The URL of the server to connect to.
	#[clap(long)]
	url: Option<Url>,

	/// The path to the config file.
	#[clap(long)]
	config: Option<PathBuf>,

	#[clap(subcommand)]
	command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
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
	initialize_v8();

	// Parse the args.
	let args = Args::parse();

	// Read the config.
	let Ok(config) = Cli::read_config(args.config.clone()) else {
		eprintln!("{}: failed to read the config", "error".red());
		return 1.into();
	};

	// Set the file descriptor limit.
	set_file_descriptor_limit(&config)
		.inspect_err(|_| {
			eprintln!(
				"{}: failed to set the file descriptor limit",
				"warning".yellow()
			);
		})
		.ok();

	// Set up tracing.
	set_up_tracing(&config);

	// Get the url.
	let url = args.url.clone().unwrap_or_else(|| {
		let path = default_path();
		let path = path.join("socket");
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		format!("http+unix://{path}").parse().unwrap()
	});

	// Create the client.
	let client = tokio::sync::Mutex::new(None);

	// Get the version.
	let version = if cfg!(debug_assertions) {
		let executable_path = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))
			.unwrap();
		let metadata = std::fs::metadata(executable_path)
			.map_err(|source| tg::error!(!source, "failed to get the executable metadata"))
			.unwrap();
		metadata
			.modified()
			.map_err(|source| tg::error!(!source, "failed to get the executable modified time"))
			.unwrap()
			.duration_since(std::time::SystemTime::UNIX_EPOCH)
			.unwrap()
			.as_secs()
			.to_string()
	} else {
		env!("CARGO_PKG_VERSION").to_owned()
	};

	// Create the CLI.
	let cli = Cli {
		url,
		config: config.clone(),
		client,
		version,
	};

	// Create the command future.
	let future = cli.command(args.command).then(|result| async {
		if let Err(error) = &result {
			let options = config
				.as_ref()
				.and_then(|config| config.advanced.as_ref())
				.and_then(|advanced| advanced.error_trace_options.clone())
				.unwrap_or_default();
			let trace = error.trace(&options);
			eprintln!("{}: failed to run the command", "error".red().bold());
			cli.print_error(&trace).await;
		}
		result
	});

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	builder.disable_lifo_slot();
	let runtime = builder.build().unwrap();
	let result = runtime.block_on(future);

	// Handle the result.
	match result {
		Ok(()) => 0.into(),
		Err(_) => 1.into(),
	}
}

impl Cli {
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

	async fn client(&self) -> tg::Result<tg::Client> {
		// If the client is already initialized, then return it.
		if let Some(client) = self.client.lock().await.as_ref().cloned() {
			return Ok(client);
		}

		// Attempt to connect to the server.
		let client = tg::Builder::new(self.url.clone()).build();
		let mut connected = client.connect().await.is_ok();

		// If the client is not connected and this is not a debug build, then start the server and attempt to connect.
		if !connected && cfg!(not(debug_assertions)) {
			self.start_server().await?;
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
			return Err(tg::error!(%url = self.url, "failed to connect to the server"));
		}

		// Check the version.
		let client_version = &self.version;
		let server_version = &client.health().await?.version;
		if client_version != server_version {
			eprintln!("{}: the client version {client_version} does not match the server version {server_version}", "warning".yellow().bold());
		}

		// Store the client.
		*self.client.lock().await = Some(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> tg::Result<()> {
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;
		let path = self
			.config
			.as_ref()
			.and_then(|config| config.path.clone())
			.unwrap_or_else(default_path);
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

	async fn print_error(&self, trace: &tg::error::Trace<'_>) {
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
							let source = self
								.get_description_for_package(package)
								.await
								.unwrap_or_else(|_| package.to_string());
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

	async fn get_description_for_package(&self, package: &tg::directory::Id) -> tg::Result<String> {
		let client = &self.client().await?;
		let dependency = tg::Dependency::with_id(package.clone());
		let arg = tg::package::GetArg {
			metadata: true,
			path: true,
			..Default::default()
		};
		let output = client
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
}

fn set_file_descriptor_limit(config: &Option<Config>) -> tg::Result<()> {
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

fn set_up_tracing(config: &Option<Config>) {
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
			let filter = tracing_subscriber::filter::EnvFilter::try_new(&tracing.filter).unwrap();
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

fn default_path() -> PathBuf {
	let home = std::env::var("HOME")
		.map_err(|source| tg::error!(!source, "failed to get the home directory path"))
		.unwrap();
	PathBuf::from(home).join(".tangram")
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
