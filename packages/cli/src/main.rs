use self::commands::Args;
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tangram_client as tg;
use tangram_error::{return_error, Result, WrapErr};
use tg::Handle;
use tracing_subscriber::prelude::*;
use url::Url;

mod commands;
mod tui;

pub const API_URL: &str = "https://api.tangram.dev";

struct Cli {
	tg: tokio::sync::Mutex<Option<Arc<dyn tg::Handle>>>,
	config: std::sync::RwLock<Option<Config>>,
	path: PathBuf,
	user: std::sync::RwLock<Option<tg::User>>,
	version: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct Config {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	autoenv: Option<AutoenvConfig>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	build: Option<BuildConfig>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<RemoteConfig>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct AutoenvConfig {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct BuildConfig {
	/// Configure remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<RemoteBuildConfig>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct RemoteBuildConfig {
	/// Enable remote builds.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	enable: bool,

	/// Limit remote builds to targets with the specified hosts.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	hosts: Option<Vec<tg::System>>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct RemoteConfig {
	/// The remote URL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	url: Option<Url>,
}

#[tokio::main]
async fn main() {
	// Run the main function.
	let result = main_inner().await;

	// Handle the result.
	if let Err(error) = result {
		// Print the error trace.
		eprintln!("An error occurred.");
		eprintln!("{}", error.trace());

		// Exit with a non-zero code.
		std::process::exit(1);
	}
}

async fn main_inner() -> Result<()> {
	// Setup tracing.
	setup_tracing();

	// Initialize V8.
	initialize_v8();

	// Parse the arguments.
	let args = Args::parse();

	// Create the container for the client.
	let tg = tokio::sync::Mutex::new(None);

	// Get the path.
	let home = std::env::var("HOME").wrap_err("Failed to get the home directory path.")?;
	let home = PathBuf::from(home);
	let path = home.join(".tangram");

	// Create the config.
	let config = std::sync::RwLock::new(None);

	// Create the user.
	let user = std::sync::RwLock::new(None);

	// Get the version.
	let version = if cfg!(debug_assertions) {
		let executable_path =
			std::env::current_exe().wrap_err("Failed to get the current executable path.")?;
		let metadata = tokio::fs::metadata(&executable_path)
			.await
			.wrap_err("Failed to get the executable metadata.")?;
		metadata
			.modified()
			.wrap_err("Failed to get the executable modified time.")?
			.duration_since(std::time::SystemTime::UNIX_EPOCH)
			.unwrap()
			.as_secs()
			.to_string()
	} else {
		env!("CARGO_PKG_VERSION").to_owned()
	};

	// Create the CLI.
	let cli = Cli {
		tg,
		config,
		path,
		user,
		version,
	};

	// Run the command.
	cli.run(args).await?;

	Ok(())
}

impl Cli {
	async fn handle(&self) -> Result<Arc<dyn tg::Handle>> {
		// If the handle is already initialized, return it.
		if let Some(tg) = self.tg.lock().await.as_ref().cloned() {
			return Ok(tg);
		}

		// Attempt to connect to the server.
		let addr = tg::client::Addr::Unix(self.path.join("socket"));
		let user = self.user().await?.clone();
		let client = tg::client::Builder::new(addr).user(user).build();
		let mut connected = client.connect().await.is_ok();

		// If this is a debug build, then require that the client is connected and has the same version as the server.
		if cfg!(debug_assertions) {
			if !connected {
				return_error!("Failed to connect to the server.");
			}
			if connected && client.status().await?.version != self.version {
				return_error!("The server has different version from the client.");
			}
			// Store the client.
			let client = Arc::new(client);
			*self.tg.lock().await = Some(client.clone());
			return Ok(client);
		}

		// If the client is connected, check the version.
		if connected && client.status().await?.version != self.version {
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
			return_error!("Failed to connect to the server.");
		}

		// Store the client.
		let client = Arc::new(client);
		*self.tg.lock().await = Some(client.clone());

		Ok(client)
	}

	/// Start the server.
	async fn start_server(&self) -> Result<()> {
		let executable =
			std::env::current_exe().wrap_err("Failed to get the current executable path.")?;
		tokio::fs::create_dir_all(&self.path)
			.await
			.wrap_err("Failed to create the server path.")?;
		let stdout = tokio::fs::File::create(self.path.join("stdout"))
			.await
			.wrap_err("Failed to create the server stdout file.")?;
		let stderr = tokio::fs::File::create(self.path.join("stderr"))
			.await
			.wrap_err("Failed to create the server stderr file.")?;
		tokio::process::Command::new(executable)
			.args(["server", "run"])
			.current_dir(&self.path)
			.stdin(std::process::Stdio::null())
			.stdout(stdout.into_std().await)
			.stderr(stderr.into_std().await)
			.spawn()
			.wrap_err("Failed to spawn the server.")?;
		Ok(())
	}

	pub async fn config(&self) -> Result<Option<Config>> {
		if let Some(config) = self.config.read().unwrap().as_ref() {
			return Ok(Some(config.clone()));
		}
		let path = self.path.join("config.json");
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
		*self.config.write().unwrap() = config.clone();
		Ok(config)
	}

	pub async fn save_config(&self, config: Config) -> Result<()> {
		self.config.write().unwrap().replace(config.clone());
		let path = self.path.join("config.json");
		let config =
			serde_json::to_string_pretty(&config).wrap_err("Failed to serialize the config.")?;
		tokio::fs::write(path, &config)
			.await
			.wrap_err("Failed to save the config.")?;
		Ok(())
	}

	pub async fn user(&self) -> Result<Option<tg::User>> {
		if let Some(user) = self.user.read().unwrap().as_ref() {
			return Ok(Some(user.clone()));
		}
		let path = self.path.join("user.json");
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
		*self.user.write().unwrap() = user.clone();
		Ok(user)
	}

	pub async fn save_user(&self, user: tg::User) -> Result<()> {
		self.user.write().unwrap().replace(user.clone());
		let path = self.path.join("user.json");
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
