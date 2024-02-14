use self::config::Config;
use clap::Parser;
use futures::FutureExt;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tracing_subscriber::prelude::*;

mod commands;
mod config;
mod tui;

pub const API_URL: &str = "https://api.tangram.dev";

struct Cli {
	address: tg::Address,
	client: tokio::sync::Mutex<Option<tg::Client>>,
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
	#[clap(long)]
	address: Option<tg::Address>,

	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Autoenv(self::commands::autoenv::Args),
	Build(self::commands::build::Args),
	Check(self::commands::check::Args),
	Checkin(self::commands::checkin::Args),
	Checkout(self::commands::checkout::Args),
	Checksum(self::commands::checksum::Args),
	Clean(self::commands::clean::Args),
	Doc(self::commands::doc::Args),
	Env(self::commands::env::Args),
	Fmt(self::commands::fmt::Args),
	Get(self::commands::get::Args),
	Init(self::commands::init::Args),
	Log(self::commands::log::Args),
	Login(self::commands::login::Args),
	Lsp(self::commands::lsp::Args),
	New(self::commands::new::Args),
	Outdated(self::commands::outdated::Args),
	Publish(self::commands::publish::Args),
	Pull(self::commands::pull::Args),
	Push(self::commands::push::Args),
	Run(self::commands::run::Args),
	Search(self::commands::search::Args),
	Server(self::commands::server::Args),
	Test(self::commands::test::Args),
	Tree(self::commands::tree::Args),
	Update(self::commands::update::Args),
	Upgrade(self::commands::upgrade::Args),
}

fn default_path() -> PathBuf {
	let home = std::env::var("HOME")
		.wrap_err("Failed to get the home directory path.")
		.unwrap();
	PathBuf::from(home).join(".tangram")
}

fn main() {
	// Setup tracing.
	setup_tracing();

	// Initialize V8. This must happen on the main thread.
	initialize_v8();

	let worker_threads = std::env::var("TANGRAM_WORKER_THREADS")
		.ok()
		.and_then(|n| n.parse().ok())
		.unwrap_or(std::thread::available_parallelism().unwrap().get());

	// Create the tokio runtime and run the main function.
	tokio::runtime::Builder::new_multi_thread()
		.worker_threads(worker_threads)
		.enable_all()
		.disable_lifo_slot()
		.build()
		.unwrap()
		.block_on(main_inner());
}

async fn main_inner() {
	// Parse the arguments.
	let args = Args::parse();

	// Get the address.
	let address = args
		.address
		.unwrap_or_else(|| tg::Address::Unix(default_path().join("socket")));

	// Create the client.
	let client = tokio::sync::Mutex::new(None);

	// Get the version.
	let version = if cfg!(debug_assertions) {
		let executable_path = std::env::current_exe()
			.wrap_err("Failed to get the current executable path.")
			.unwrap();
		let metadata = tokio::fs::metadata(&executable_path)
			.await
			.wrap_err("Failed to get the executable metadata.")
			.unwrap();
		metadata
			.modified()
			.wrap_err("Failed to get the executable modified time.")
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
		client,
		version,
	};

	// Run the command.
	let result = match args.command {
		Command::Autoenv(args) => cli.command_autoenv(args).boxed(),
		Command::Build(args) => cli.command_build(args).boxed(),
		Command::Check(args) => cli.command_check(args).boxed(),
		Command::Checkin(args) => cli.command_checkin(args).boxed(),
		Command::Checkout(args) => cli.command_checkout(args).boxed(),
		Command::Checksum(args) => cli.command_checksum(args).boxed(),
		Command::Clean(args) => cli.command_clean(args).boxed(),
		Command::Doc(args) => cli.command_doc(args).boxed(),
		Command::Env(args) => cli.command_env(args).boxed(),
		Command::Fmt(args) => cli.command_fmt(args).boxed(),
		Command::Get(args) => cli.command_get(args).boxed(),
		Command::Init(args) => cli.command_init(args).boxed(),
		Command::Log(args) => cli.command_log(args).boxed(),
		Command::Login(args) => cli.command_login(args).boxed(),
		Command::Lsp(args) => cli.command_lsp(args).boxed(),
		Command::New(args) => cli.command_new(args).boxed(),
		Command::Outdated(args) => cli.command_outdated(args).boxed(),
		Command::Publish(args) => cli.command_publish(args).boxed(),
		Command::Pull(args) => cli.command_pull(args).boxed(),
		Command::Push(args) => cli.command_push(args).boxed(),
		Command::Run(args) => cli.command_run(args).boxed(),
		Command::Search(args) => cli.command_search(args).boxed(),
		Command::Server(args) => cli.command_server(args).boxed(),
		Command::Test(args) => cli.command_test(args).boxed(),
		Command::Tree(args) => cli.command_tree(args).boxed(),
		Command::Update(args) => cli.command_update(args).boxed(),
		Command::Upgrade(args) => cli.command_upgrade(args).boxed(),
	}
	.await;

	// Handle the result.
	if let Err(error) = result {
		// Print the error trace.
		eprintln!("An error occurred.");
		eprintln!("{}", error.trace());

		// Exit with a non-zero code.
		std::process::exit(1);
	}
}

impl Cli {
	async fn client(&self) -> Result<tg::Client> {
		// If the client is already initialized, then return it.
		if let Some(client) = self.client.lock().await.as_ref().cloned() {
			return Ok(client);
		}

		// Attempt to connect to the server.
		let user = self.user().await?.clone();
		let client = tg::Builder::new(self.address.clone()).user(user).build();
		let mut connected = client.connect().await.is_ok();

		// If this is a debug build, then require that the client is connected and has the same version as the server.
		if cfg!(debug_assertions) {
			if !connected {
				return Err(error!("Failed to connect to the server."));
			}
			if connected && client.health().await?.version != self.version {
				return Err(error!("The server has different version from the client."));
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
			return Err(error!("Failed to connect to the server."));
		}

		// Store the client.
		*self.client.lock().await = Some(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> Result<()> {
		let executable =
			std::env::current_exe().wrap_err("Failed to get the current executable path.")?;
		let path = self
			.config(None)
			.await?
			.as_ref()
			.and_then(|config| config.path.clone())
			.unwrap_or_else(default_path);
		tokio::fs::create_dir_all(&path)
			.await
			.wrap_err("Failed to create the server path.")?;
		let stdout = tokio::fs::File::create(path.join("stdout"))
			.await
			.wrap_err("Failed to create the server stdout file.")?;
		let stderr = tokio::fs::File::create(path.join("stderr"))
			.await
			.wrap_err("Failed to create the server stderr file.")?;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(&path)
			.stdin(std::process::Stdio::null())
			.stdout(stdout.into_std().await)
			.stderr(stderr.into_std().await)
			.spawn()
			.wrap_err("Failed to spawn the server.")?;
		Ok(())
	}

	pub async fn config(&self, path: Option<PathBuf>) -> Result<Option<Config>> {
		let path = path.unwrap_or_else(|| {
			let home = std::env::var("HOME")
				.wrap_err("Failed to get the HOME environment variable.")
				.unwrap();
			PathBuf::from(home).join(".config/tangram/config.json")
		});
		let exists = tokio::fs::try_exists(&path)
			.await
			.wrap_err("Failed to check if the config file exists.")?;
		let config = if exists {
			let config = tokio::fs::read_to_string(path)
				.await
				.wrap_err("Failed to read the config file.")?;
			Some(serde_json::from_str(&config).wrap_err("Failed to deserialize the config.")?)
		} else {
			None
		};
		Ok(config)
	}

	pub async fn save_config(&self, config: Config) -> Result<()> {
		let home =
			std::env::var("HOME").wrap_err("Failed to get the HOME environment variable.")?;
		let path = PathBuf::from(home).join(".config/tangram/config.json");
		let config =
			serde_json::to_string_pretty(&config).wrap_err("Failed to serialize the config.")?;
		tokio::fs::write(path, &config)
			.await
			.wrap_err("Failed to save the config.")?;
		Ok(())
	}

	pub async fn user(&self) -> Result<Option<tg::User>> {
		let home =
			std::env::var("HOME").wrap_err("Failed to get the HOME environment variable.")?;
		let path = PathBuf::from(home).join(".config/tangram/user.json");
		let exists = tokio::fs::try_exists(&path)
			.await
			.wrap_err("Failed to check if the user file exists.")?;
		let user = if exists {
			let config = tokio::fs::read_to_string(path)
				.await
				.wrap_err("Failed to read the user file.")?;
			Some(serde_json::from_str(&config).wrap_err("Failed to deserialize the user.")?)
		} else {
			None
		};
		Ok(user)
	}

	pub async fn save_user(&self, user: tg::User) -> Result<()> {
		let home =
			std::env::var("HOME").wrap_err("Failed to get the HOME environment variable.")?;
		let path = PathBuf::from(home).join(".config/tangram/user.json");
		let user = serde_json::to_string_pretty(&user.clone())
			.wrap_err("Failed to serialize the user.")?;
		tokio::fs::write(path, &user)
			.await
			.wrap_err("Failed to save the user.")?;
		Ok(())
	}
}

fn initialize_v8() {
	// Set the ICU data.
	#[repr(C, align(16))]
	struct IcuData([u8; 10_631_872]);
	static ICU_DATA: IcuData = IcuData(*include_bytes!(concat!(
		env!("CARGO_MANIFEST_DIR"),
		"/../language/src/icudtl.dat"
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

fn setup_tracing() {
	// Create the env layer.
	let tracing_env_filter = std::env::var("TANGRAM_TRACING").ok();
	let env_layer = tracing_env_filter
		.map(|env_filter| tracing_subscriber::filter::EnvFilter::try_new(env_filter).unwrap());

	// If tracing is enabled, create and initialize the subscriber.
	if let Some(env_layer) = env_layer {
		let format_layer = tracing_subscriber::fmt::layer()
			.compact()
			.with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
			.with_writer(std::io::stderr);
		let subscriber = tracing_subscriber::registry()
			.with(env_layer)
			.with(format_layer);
		subscriber.init();
	}
}
