use self::config::Config;
use clap::Parser;
use crossterm::style::Stylize;
use futures::FutureExt;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result};
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
	user: Option<tg::User>,
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
	Autoenv(self::commands::autoenv::Args),
	Build(self::commands::build::Args),
	Cat(self::commands::cat::Args),
	Check(self::commands::check::Args),
	Checkin(self::commands::checkin::Args),
	Checkout(self::commands::checkout::Args),
	Checksum(self::commands::checksum::Args),
	Clean(self::commands::clean::Args),
	Doc(self::commands::doc::Args),
	Fmt(self::commands::fmt::Args),
	Get(self::commands::get::Args),
	Hook(self::commands::hook::Args),
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

fn default_path() -> PathBuf {
	let home = std::env::var("HOME")
		.map_err(|source| error!(!source, "failed to get the home directory path"))
		.unwrap();
	PathBuf::from(home).join(".tangram")
}

fn main() {
	let mut config = None;
	let result = main_inner(&mut config);
	if let Err(error) = result {
		let options = config
			.as_ref()
			.and_then(|config| config.advanced.as_ref())
			.and_then(|advanced| advanced.error_trace_options.clone())
			.unwrap_or_default();
		let trace = error.trace(&options);
		eprintln!("{}: an error occurred", "error".red().bold());
		trace.eprint();
		std::process::exit(1);
	}
}

fn main_inner(config_: &mut Option<Config>) -> Result<()> {
	// Parse the arguments.
	let args = Args::parse();

	// Read the config.
	let config = Cli::read_config(args.config.clone())
		.map_err(|source| error!(!source, "failed to read the config"))?;
	*config_ = config.clone();

	// Set the file descriptor limit.
	set_file_descriptor_limit(&config)?;

	// Set up tracing.
	set_up_tracing(&config);

	// Initialize V8.
	initialize_v8();

	// Read the user.
	let user = Cli::read_user(None)?;

	// Get the url.
	let url = args.url.unwrap_or_else(|| {
		format!("unix:{}", default_path().join("socket").display())
			.parse()
			.unwrap()
	});

	// Create the client.
	let client = tokio::sync::Mutex::new(None);

	// Get the version.
	let version = if cfg!(debug_assertions) {
		let executable_path = std::env::current_exe()
			.map_err(|source| error!(!source, "failed to get the current executable path"))?;
		let metadata = std::fs::metadata(executable_path)
			.map_err(|source| error!(!source, "failed to get the executable metadata"))?;
		metadata
			.modified()
			.map_err(|source| error!(!source, "failed to get the executable modified time"))?
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
		config,
		client,
		user,
		version,
	};

	// Run the command.
	let future = match args.command {
		Command::Autoenv(args) => cli.command_autoenv(args).boxed(),
		Command::Build(args) => cli.command_build(args).boxed(),
		Command::Cat(args) => cli.command_cat(args).boxed(),
		Command::Check(args) => cli.command_check(args).boxed(),
		Command::Checkin(args) => cli.command_checkin(args).boxed(),
		Command::Checkout(args) => cli.command_checkout(args).boxed(),
		Command::Checksum(args) => cli.command_checksum(args).boxed(),
		Command::Clean(args) => cli.command_clean(args).boxed(),
		Command::Doc(args) => cli.command_doc(args).boxed(),
		Command::Fmt(args) => cli.command_fmt(args).boxed(),
		Command::Get(args) => cli.command_get(args).boxed(),
		Command::Hook(args) => cli.command_hook(args).boxed(),
		Command::Init(args) => cli.command_init(args).boxed(),
		Command::Log(args) => cli.command_log(args).boxed(),
		Command::Login(args) => cli.command_login(args).boxed(),
		Command::Lsp(args) => cli.command_lsp(args).boxed(),
		Command::New(args) => cli.command_new(args).boxed(),
		Command::Outdated(args) => cli.command_package_outdated(args).boxed(),
		Command::Object(args) => cli.command_object(args).boxed(),
		Command::Package(args) => cli.command_package(args).boxed(),
		Command::Publish(args) => cli.command_package_publish(args).boxed(),
		Command::Pull(args) => cli.command_pull(args).boxed(),
		Command::Push(args) => cli.command_push(args).boxed(),
		Command::Run(args) => cli.command_run(args).boxed(),
		Command::Search(args) => cli.command_package_search(args).boxed(),
		Command::Server(args) => cli.command_server(args).boxed(),
		Command::Tree(args) => cli.command_tree(args).boxed(),
		Command::Update(args) => cli.command_package_update(args).boxed(),
		Command::Upgrade(args) => cli.command_upgrade(args).boxed(),
	};

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	builder.disable_lifo_slot();
	let runtime = builder.build().unwrap();
	runtime.block_on(future)?;

	Ok(())
}

impl Cli {
	async fn client(&self) -> Result<tg::Client> {
		// If the client is already initialized, then return it.
		if let Some(client) = self.client.lock().await.as_ref().cloned() {
			return Ok(client);
		}

		// Attempt to connect to the server.
		let user = self.user.clone();
		let client = tg::Builder::new(self.url.clone()).user(user).build();
		let mut connected = client.connect().await.is_ok();

		// If this is a debug build, then require that the client is connected and has the same version as the server.
		if cfg!(debug_assertions) {
			if !connected {
				return Err(error!(%url = self.url, "failed to connect to the server"));
			}
			let client_version = client.health().await?.version;
			if connected && client_version != self.version {
				let server_version = &self.version;
				return Err(
					error!(%client_version, %server_version, "the server has different version from the client"),
				);
			}
			// Store the client.
			*self.client.lock().await = Some(client.clone());
			return Ok(client);
		}

		// If the client is connected, check the version.
		if connected && client.health().await?.version != self.version {
			client.stop().await?;
			tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
			client.disconnect().await?;
			connected = false;
		}

		// If the client is not connected, start the server and attempt to connect.
		if !connected {
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
			return Err(error!(%url = self.url, "failed to connect to the server"));
		}

		// Store the client.
		*self.client.lock().await = Some(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> Result<()> {
		let executable = std::env::current_exe()
			.map_err(|source| error!(!source, "failed to get the current executable path"))?;
		let path = self
			.config
			.as_ref()
			.and_then(|config| config.path.clone())
			.unwrap_or_else(default_path);
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|source| error!(!source, "failed to create the server path"))?;
		let stdout = tokio::fs::File::create(path.join("log"))
			.await
			.map_err(|source| error!(!source, "failed to create the server log file"))?;
		let stderr = stdout
			.try_clone()
			.await
			.map_err(|source| error!(!source, "failed to clone the server log file"))?;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(&path)
			.stdin(std::process::Stdio::null())
			.stdout(stdout.into_std().await)
			.stderr(stderr.into_std().await)
			.spawn()
			.map_err(|source| error!(!source, "failed to spawn the server"))?;
		Ok(())
	}

	fn read_config(path: Option<PathBuf>) -> Result<Option<Config>> {
		let home = std::env::var("HOME")
			.map_err(|error| {
				error!(
					source = error,
					"failed to get the HOME environment variable"
				)
			})
			.unwrap();
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
		let config = match std::fs::read_to_string(&path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => {
				return Err(
					error!(source = error, %path = path.display(), "failed to read the config file"),
				)
			},
		};
		let config = serde_json::from_str(&config).map_err(
			|source| error!(!source, %path = path.display(), "failed to deserialize the config"),
		)?;
		Ok(Some(config))
	}

	fn write_config(config: &Config, path: Option<PathBuf>) -> Result<()> {
		let home = std::env::var("HOME").map_err(|error| {
			error!(
				source = error,
				"failed to get the HOME environment variable"
			)
		})?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
		let config = serde_json::to_string_pretty(&config)
			.map_err(|source| error!(!source, "failed to serialize the config"))?;
		std::fs::write(path, config)
			.map_err(|source| error!(!source, "failed to save the config"))?;
		Ok(())
	}

	fn read_user(path: Option<PathBuf>) -> Result<Option<tg::User>> {
		let home = std::env::var("HOME")
			.map_err(|error| {
				error!(
					source = error,
					"failed to get the HOME environment variable"
				)
			})
			.unwrap();
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/user.json"));
		let user = match std::fs::read_to_string(path) {
			Ok(user) => user,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => return Err(error!(source = error, "failed to read the user file")),
		};
		let user = serde_json::from_str(&user)
			.map_err(|source| error!(!source, "failed to deserialize the user"))?;
		Ok(Some(user))
	}

	fn write_user(user: &tg::User, path: Option<PathBuf>) -> Result<()> {
		let home = std::env::var("HOME").map_err(|error| {
			error!(
				source = error,
				"failed to get the HOME environment variable"
			)
		})?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/user.json"));
		let user = serde_json::to_string_pretty(&user.clone())
			.map_err(|source| error!(!source, "failed to serialize the user"))?;
		std::fs::write(path, user).map_err(|source| error!(!source, "failed to save the user"))?;
		Ok(())
	}
}

fn set_file_descriptor_limit(config: &Option<Config>) -> Result<()> {
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
			return Err(error!(
				source = std::io::Error::last_os_error(),
				"failed to set the file descriptor limit"
			));
		}
	}
	Ok(())
}

fn initialize_v8() {
	// Set the ICU data.
	#[repr(C, align(16))]
	struct IcuData([u8; 10_631_872]);
	static ICU_DATA: IcuData = IcuData(*include_bytes!(concat!(
		env!("CARGO_MANIFEST_DIR"),
		"/../../packages/server/src/language/icudtl.dat"
	)));
	v8::icu::set_common_data_73(&ICU_DATA.0).unwrap();

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
	let fmt_layer = config
		.as_ref()
		.and_then(|config| config.tracing.as_ref())
		.map(|tracing| {
			let filter = tracing_subscriber::filter::EnvFilter::try_new(tracing).unwrap();
			tracing_subscriber::fmt::layer()
				.compact()
				.with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
				.with_writer(std::io::stderr)
				.with_filter(filter)
		});
	tracing_subscriber::registry()
		.with(console_layer)
		.with(fmt_layer)
		.init();
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
