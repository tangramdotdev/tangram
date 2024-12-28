use crate::Config;
use futures::{Future, FutureExt as _};
use std::{panic::AssertUnwindSafe, sync::Arc};
use tangram_client as tg;
use tangram_temp::Temp;
use url::Url;

pub async fn test<F, Fut>(tg: &'static str, f: F)
where
	F: FnOnce(Arc<tokio::sync::Mutex<Context>>) -> Fut,
	Fut: Future<Output = ()> + Send,
{
	// Create the context.
	let context = Arc::new(tokio::sync::Mutex::new(Context::new(tg)));

	// Run the test and catch a panic if one occurs.
	let result = AssertUnwindSafe(f(context.clone())).catch_unwind().await;

	// Handle the result.
	let context = context.lock().await;
	match result {
		Ok(()) => {
			// If there was no panic, then gracefully shutdown the servers.
			for server in &context.servers {
				let mut process = server.process.lock().await;
				unsafe { libc::kill(process.id().unwrap().try_into().unwrap(), libc::SIGINT) };
				let result = process.wait().await;
				if let Ok(status) = result {
					if !status.success() {}
				}
			}
		},
		Err(error) => {
			// If there was a panic, then forcefully shutdown the servers and resume unwinding.
			for server in &context.servers {
				let mut process = server.process.lock().await;
				unsafe { libc::kill(process.id().unwrap().try_into().unwrap(), libc::SIGKILL) };
				process.wait().await.ok();
			}
			std::panic::resume_unwind(error);
		},
	}
}

pub struct Context {
	servers: Vec<Arc<Server>>,
	tg: &'static str,
}

pub struct Server {
	config: Config,
	process: tokio::sync::Mutex<tokio::process::Child>,
	temp: Arc<Temp>,
	tg: &'static str,
}

impl Context {
	#[must_use]
	pub fn new(tg: &'static str) -> Self {
		Self {
			servers: Vec::new(),
			tg,
		}
	}

	pub async fn spawn_server(&mut self) -> tg::Result<Arc<Server>> {
		let server = Server::new(self.tg).await?;
		let server = Arc::new(server);
		self.servers.push(server.clone());
		Ok(server)
	}

	pub async fn spawn_server_with_config(&mut self, config: Config) -> tg::Result<Arc<Server>> {
		let server = Server::with_config(self.tg, config).await?;
		let server = Arc::new(server);
		self.servers.push(server.clone());
		Ok(server)
	}

	pub async fn spawn_server_with_temp_and_config(
		&mut self,
		config: Config,
	) -> tg::Result<Arc<Server>> {
		let server = Server::with_config(self.tg, config).await?;
		let server = Arc::new(server);
		self.servers.push(server.clone());
		Ok(server)
	}
}

impl Server {
	async fn new(tg: &'static str) -> tg::Result<Self> {
		let config = Config::default();
		Self::with_config(tg, config).await
	}

	async fn with_config(tg: &'static str, config: Config) -> tg::Result<Self> {
		// Create a temp and create the config and data paths.
		let temp = Arc::new(Temp::new());
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
		let config_path = temp.path().join(".config/tangram/config.json");
		tokio::fs::create_dir_all(config_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the config directory"))?;
		let config_json = serde_json::to_vec_pretty(&config).unwrap();
		tokio::fs::write(&config_path, config_json)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the config"))?;
		let data_path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&data_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the data directory"))?;

		// Create the command.
		let log_path = temp.path().join(".tangram/log");
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
		let mut command = tokio::process::Command::new(tg);
		command.stdout(stdout);
		command.stderr(stderr);
		command.arg("--config");
		command.arg(&config_path);
		command.arg("--path");
		command.arg(data_path);
		command.arg("serve");

		// Spawn the process.
		let process = command.spawn().unwrap();
		let process = tokio::sync::Mutex::new(process);

		// Create the server.
		let server = Self {
			config,
			process,
			temp,
			tg,
		};

		Ok(server)
	}

	#[must_use]
	pub fn tg(&self) -> tokio::process::Command {
		let mut command = tokio::process::Command::new(self.tg);
		let config_path = self.temp.path().join(".config/tangram/config.json");
		let data_path = self.temp.path().join(".tangram");
		command
			.arg("--config")
			.arg(config_path)
			.arg("--path")
			.arg(data_path)
			.arg("--mode")
			.arg("client");
		command
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
	}

	#[must_use]
	pub fn process(&self) -> &tokio::sync::Mutex<tokio::process::Child> {
		&self.process
	}

	#[must_use]
	pub fn url(&self) -> Url {
		let path = self.temp.path().join(".tangram/socket");
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		format!("http+unix://{path}").parse().unwrap()
	}

	pub async fn print_log(&self) -> tg::Result<()> {
		let log_path = self.temp.join(".tangram/log");
		let mut log_file = tokio::fs::File::open(log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?;
		let mut stderr = tokio::io::stderr();
		tokio::io::copy(&mut log_file, &mut stderr).await.ok();
		Ok(())
	}
}

#[macro_export]
macro_rules! assert_output_success {
	($output:expr) => {
		let output = &$output;
		if !output.status.success() {
			let mut stderr = tokio::io::stderr();
			stderr.write_all(&output.stderr).await.unwrap();
			stderr.flush().await.unwrap();
		}
		assert!(output.status.success());
	};
}
pub use assert_output_success;
