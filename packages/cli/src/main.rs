use self::config::Config;
use clap::Parser;
use futures::FutureExt;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result};
use tracing_subscriber::prelude::*;

mod commands;
mod config;
mod tui;
mod util;

pub const API_URL: &str = "https://api.tangram.dev";

struct Cli {
	address: tg::Address,
	config: Option<Config>,
	client: tokio::sync::Mutex<Option<tg::Client>>,
	user: Option<tg::User>,
	version: String,
}

#[derive(Debug, clap::Parser)]
#[command(
	about = env!("CARGO_PKG_DESCRIPTION"),
	disable_help_subcommand = true,
	long_version = env!("CARGO_PKG_VERSION"),
	name = env!("CARGO_CRATE_NAME"),
	verbatim_doc_comment,
	version = env!("CARGO_PKG_VERSION"),
)]
struct Args {
	/// The address to connect to.
	#[arg(long)]
	address: Option<tg::Address>,

	/// The path to the config file.
	#[arg(long)]
	config: Option<PathBuf>,

	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, clap::Subcommand)]
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
	Init(self::commands::init::Args),
	Log(self::commands::log::Args),
	Login(self::commands::login::Args),
	Lsp(self::commands::lsp::Args),
	New(self::commands::new::Args),
	Object(self::commands::object::Args),
	Outdated(self::commands::outdated::Args),
	Publish(self::commands::publish::Args),
	Pull(self::commands::pull::Args),
	Push(self::commands::push::Args),
	Run(self::commands::run::Args),
	Search(self::commands::search::Args),
	Server(self::commands::server::Args),
	Tree(self::commands::tree::Args),
	Update(self::commands::update::Args),
	Upgrade(self::commands::upgrade::Args),
}

fn default_path() -> PathBuf {
	let home = std::env::var("HOME")
		.map_err(|error| error!(source = error, "Failed to get the home directory path."))
		.unwrap();
	PathBuf::from(home).join(".tangram")
}

fn main() {
	let result = main_inner();
	if let Err(error) = result {
		eprintln!("An error occurred.");
		eprintln!("{}", error.trace());
		std::process::exit(1);
	}
}

fn main_inner() -> Result<()> {
	// Parse the arguments.
	let args = Args::parse();

	// Read the config.
	let config = Cli::read_config(args.config)?;

	// Set the file descriptor limit.
	set_file_descriptor_limit(&config)?;

	// Read the user.
	let user = Cli::read_user(None)?;

	// Initialize V8.
	initialize_v8();

	// Get the address.
	let address = args
		.address
		.unwrap_or_else(|| tg::Address::Unix(default_path().join("socket")));

	// Create the client.
	let client = tokio::sync::Mutex::new(None);

	// Get the version.
	let version = if cfg!(debug_assertions) {
		let executable_path = std::env::current_exe()
			.map_err(|error| error!(source = error, "Failed to get the current executable path."))
			.unwrap();
		let metadata = std::fs::metadata(executable_path)
			.map_err(|error| error!(source = error, "Failed to get the executable metadata."))
			.unwrap();
		metadata
			.modified()
			.map_err(|error| {
				error!(
					source = error,
					"Failed to get the executable modified time."
				)
			})
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
		address,
		config,
		client,
		user,
		version,
	};

	// Set up tracing.
	set_up_tracing();

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
		Command::Init(args) => cli.command_init(args).boxed(),
		Command::Log(args) => cli.command_log(args).boxed(),
		Command::Login(args) => cli.command_login(args).boxed(),
		Command::Lsp(args) => cli.command_lsp(args).boxed(),
		Command::New(args) => cli.command_new(args).boxed(),
		Command::Outdated(args) => cli.command_outdated(args).boxed(),
		Command::Object(args) => cli.command_object(args).boxed(),
		Command::Publish(args) => cli.command_publish(args).boxed(),
		Command::Pull(args) => cli.command_pull(args).boxed(),
		Command::Push(args) => cli.command_push(args).boxed(),
		Command::Run(args) => cli.command_run(args).boxed(),
		Command::Search(args) => cli.command_search(args).boxed(),
		Command::Server(args) => cli.command_server(args).boxed(),
		Command::Tree(args) => cli.command_tree(args).boxed(),
		Command::Update(args) => cli.command_update(args).boxed(),
		Command::Upgrade(args) => cli.command_upgrade(args).boxed(),
	};

	// Create the tokio runtime and block on the future.
	let mut builder = tokio::runtime::Builder::new_multi_thread();
	builder.enable_all();
	builder.disable_lifo_slot();
	let runtime = builder.build().unwrap();
	runtime.block_on(future)
}

impl Cli {
	async fn client(&self) -> Result<tg::Client> {
		// If the client is already initialized, then return it.
		if let Some(client) = self.client.lock().await.as_ref().cloned() {
			return Ok(client);
		}

		// Attempt to connect to the server.
		let user = self.user.clone();
		let client = tg::Builder::new(self.address.clone()).user(user).build();
		let mut connected = client.connect().await.is_ok();

		// If this is a debug build, then require that the client is connected and has the same version as the server.
		if cfg!(debug_assertions) {
			if !connected {
				let addr = &self.address;
				return Err(error!(%addr, "Failed to connect to the server."));
			}
			let client_version = client.health().await?.version;
			if connected && client_version != self.version {
				let server_version = &self.version;
				return Err(
					error!(%client_version, %server_version, "The server has different version from the client."),
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
			let addr = &self.address;
			return Err(error!(?addr, "Failed to connect to the server."));
		}

		// Store the client.
		*self.client.lock().await = Some(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> Result<()> {
		let executable = std::env::current_exe().map_err(|error| {
			error!(source = error, "Failed to get the current executable path.")
		})?;
		let path = self
			.config
			.as_ref()
			.and_then(|config| config.path.clone())
			.unwrap_or_else(default_path);
		tokio::fs::create_dir_all(&path)
			.await
			.map_err(|error| error!(source = error, "Failed to create the server path."))?;
		let stdout = tokio::fs::File::create(path.join("log"))
			.await
			.map_err(|error| error!(source = error, "Failed to create the server log file."))?;
		let stderr = stdout
			.try_clone()
			.await
			.map_err(|error| error!(source = error, "Failed to clone the server log file."))?;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(&path)
			.stdin(std::process::Stdio::null())
			.stdout(stdout.into_std().await)
			.stderr(stderr.into_std().await)
			.spawn()
			.map_err(|error| error!(source = error, "Failed to spawn the server."))?;
		Ok(())
	}

	fn read_config(path: Option<PathBuf>) -> Result<Option<Config>> {
		let home = std::env::var("HOME")
			.map_err(|error| {
				error!(
					source = error,
					"Failed to get the HOME environment variable."
				)
			})
			.unwrap();
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
		let config = match std::fs::read_to_string(path) {
			Ok(config) => config,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => return Err(error!(source = error, "Failed to read the config file.")),
		};
		let config = serde_json::from_str(&config)
			.map_err(|error| error!(source = error, "Failed to deserialize the config."))?;
		Ok(Some(config))
	}

	fn write_config(config: &Config, path: Option<PathBuf>) -> Result<()> {
		let home = std::env::var("HOME").map_err(|error| {
			error!(
				source = error,
				"Failed to get the HOME environment variable."
			)
		})?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/config.json"));
		let config = serde_json::to_string_pretty(&config)
			.map_err(|error| error!(source = error, "Failed to serialize the config."))?;
		std::fs::write(path, config)
			.map_err(|error| error!(source = error, "Failed to save the config."))?;
		Ok(())
	}

	fn read_user(path: Option<PathBuf>) -> Result<Option<tg::User>> {
		let home = std::env::var("HOME")
			.map_err(|error| {
				error!(
					source = error,
					"Failed to get the HOME environment variable."
				)
			})
			.unwrap();
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/user.json"));
		let user = match std::fs::read_to_string(path) {
			Ok(user) => user,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(error) => return Err(error!(source = error, "Failed to read the user file.")),
		};
		let user = serde_json::from_str(&user)
			.map_err(|error| error!(source = error, "Failed to deserialize the user."))?;
		Ok(Some(user))
	}

	fn write_user(user: &tg::User, path: Option<PathBuf>) -> Result<()> {
		let home = std::env::var("HOME").map_err(|error| {
			error!(
				source = error,
				"Failed to get the HOME environment variable."
			)
		})?;
		let path = path.unwrap_or_else(|| PathBuf::from(home).join(".config/tangram/user.json"));
		let user = serde_json::to_string_pretty(&user.clone())
			.map_err(|error| error!(source = error, "Failed to serialize the user."))?;
		std::fs::write(path, user)
			.map_err(|error| error!(source = error, "Failed to save the user."))?;
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
				"Failed to set the file descriptor limit."
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

fn set_up_tracing() {
	let console_layer = std::env::var("TANGRAM_TOKIO_CONSOLE")
		.ok()
		.map(|_| console_subscriber::spawn());
	let fmt_layer = std::env::var("TANGRAM_TRACING").ok().map(|env| {
		let filter = tracing_subscriber::filter::EnvFilter::try_new(env).unwrap();
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
