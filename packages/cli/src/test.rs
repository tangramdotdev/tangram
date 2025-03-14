use crate::Config;
use futures::FutureExt as _;
use std::{panic::AssertUnwindSafe, sync::Arc};
use tangram_client as tg;
use tangram_temp::Temp;
use url::Url;

pub async fn test<F>(tg: &'static str, f: F)
where
	F: AsyncFnOnce(&mut Context) -> () + Send,
{
	// Create the context.
	let mut context = Context::new(tg);

	// Run the test and catch a panic if one occurs.
	let result = AssertUnwindSafe(f(&mut context)).catch_unwind().await;

	// Handle the result.
	match result {
		Ok(()) => {
			// If there was no panic, then gracefully shutdown the servers.
			for server in &context.servers {
				server.stop_gracefully().await;
			}
		},
		Err(error) => {
			// If there was a panic, then forcefully shut down the servers and resume unwinding.
			for server in &context.servers {
				server.stop_forcefully().await;
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
	process: tokio::sync::Mutex<Option<tokio::process::Child>>,
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
		temp: Temp,
		config: Config,
	) -> tg::Result<Arc<Server>> {
		let server = Server::with_temp_and_config(self.tg, temp, config).await?;
		let server = Arc::new(server);
		self.servers.push(server.clone());
		Ok(server)
	}
}

impl Server {
	async fn new(tg: &'static str) -> tg::Result<Self> {
		let config = Config {
			remotes: Some(Vec::new()),
			..Default::default()
		};
		Self::with_config(tg, config).await
	}

	async fn with_config(tg: &'static str, config: Config) -> tg::Result<Self> {
		// Create a temp and create the config and data paths.
		let temp = Temp::new();
		Self::with_temp_and_config(tg, temp, config).await
	}

	async fn with_temp_and_config(
		tg: &'static str,
		temp: Temp,
		config: Config,
	) -> tg::Result<Self> {
		let temp = Arc::new(temp);
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
		let mut command = tokio::process::Command::new(tg);
		command.arg("--config");
		command.arg(&config_path);
		command.arg("--path");
		command.arg(data_path);
		command.arg("serve");

		// Spawn the process.
		let process = command.spawn().unwrap();
		let process = tokio::sync::Mutex::new(Some(process));

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
	pub fn url(&self) -> Url {
		let path = self.temp.path().join(".tangram/socket");
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		format!("http+unix://{path}").parse().unwrap()
	}

	pub async fn stop_gracefully(&self) {
		if let Some(process) = self.process.lock().await.take() {
			unsafe { libc::kill(process.id().unwrap().try_into().unwrap(), libc::SIGINT) };
			let output = process.wait_with_output().await.unwrap();
			assert_success!(output);
		}
	}

	pub async fn stop_forcefully(&self) {
		if let Some(mut process) = self.process.lock().await.take() {
			unsafe { libc::kill(process.id().unwrap().try_into().unwrap(), libc::SIGKILL) };
			process.wait().await.ok();
		}
	}

	pub fn temp(&self) -> Arc<Temp> {
		self.temp.clone()
	}
}

#[macro_export]
macro_rules! assert_success {
	($output:expr) => {
		let output = &$output;
		if !output.status.success() {
			let stderr = std::str::from_utf8(&output.stderr).unwrap();
			for line in stderr.lines() {
				eprintln!("{line}");
			}
		}
		assert!(output.status.success());
	};
}

#[macro_export]
macro_rules! assert_failure {
	($output:expr) => {
		let output = &$output;
		if output.status.success() {
			let stderr = std::str::from_utf8(&output.stderr).unwrap();
			for line in stderr.lines() {
				eprintln!("{line}");
			}
		}
		assert!(!output.status.success());
	};
}

pub use assert_failure;
pub use assert_success;
