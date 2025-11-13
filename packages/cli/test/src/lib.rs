use {
	serde_json::json, std::time::Duration, tangram_client::prelude::*, tangram_temp::Temp,
	tangram_uri::Uri,
};

pub struct Server {
	child: tokio::process::Child,
	config: serde_json::Value,
	temp: Temp,
	tg: &'static str,
}

impl Server {
	pub async fn new(tg: &'static str) -> tg::Result<Self> {
		let config = json!({
			"remotes": [],
		});
		Self::with_config(tg, config).await
	}

	pub async fn with_config(tg: &'static str, config: serde_json::Value) -> tg::Result<Self> {
		// Create a temp and create the config and data paths.
		let temp = Temp::new();
		Self::with_temp_and_config(tg, temp, config).await
	}

	pub async fn with_temp_and_config(
		tg: &'static str,
		temp: Temp,
		config: serde_json::Value,
	) -> tg::Result<Self> {
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
		let directory_path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&directory_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Spawn the server.
		let mut command = tokio::process::Command::new(tg);
		command
			.kill_on_drop(true)
			.arg("--config")
			.arg(&config_path)
			.arg("--directory")
			.arg(&directory_path)
			.arg("--quiet")
			.arg("serve")
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::inherit());
		let child = command.spawn().unwrap();

		// Wait for the server to start.
		let mut started = false;
		for _ in 0..100 {
			let status = tokio::process::Command::new(tg)
				.kill_on_drop(true)
				.arg("--config")
				.arg(&config_path)
				.arg("--directory")
				.arg(&directory_path)
				.arg("--mode")
				.arg("client")
				.arg("--quiet")
				.arg("health")
				.stdin(std::process::Stdio::null())
				.stdout(std::process::Stdio::null())
				.stderr(std::process::Stdio::null())
				.status()
				.await
				.unwrap();
			if status.success() {
				started = true;
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		if !started {
			return Err(tg::error!("failed to start the server"));
		}

		// Create the server.
		let server = Self {
			child,
			config,
			temp,
			tg,
		};

		Ok(server)
	}

	#[must_use]
	pub fn tg(&self) -> tokio::process::Command {
		let mut command = tokio::process::Command::new(self.tg);
		let config_path = self.temp.path().join(".config/tangram/config.json");
		let directory_path = self.temp.path().join(".tangram");
		command
			.kill_on_drop(true)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::piped())
			.stderr(std::process::Stdio::piped())
			.arg("--config")
			.arg(config_path)
			.arg("--directory")
			.arg(directory_path)
			.arg("--mode")
			.arg("client")
			.arg("--quiet");
		command
	}

	pub async fn stop(mut self) -> std::process::Output {
		self.child.kill().await.unwrap();
		self.child.wait_with_output().await.unwrap()
	}

	pub fn child(&self) -> &tokio::process::Child {
		&self.child
	}

	#[must_use]
	pub fn config(&self) -> &serde_json::Value {
		&self.config
	}

	pub fn temp(&self) -> &Temp {
		&self.temp
	}

	#[must_use]
	pub fn url(&self) -> Uri {
		let path = self.temp.path().join(".tangram/socket");
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		format!("http+unix://{path}").parse().unwrap()
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
			let stdout = std::str::from_utf8(&output.stdout).unwrap();
			for line in stdout.lines() {
				println!("{line}");
			}

			let stderr = std::str::from_utf8(&output.stderr).unwrap();
			for line in stderr.lines() {
				eprintln!("{line}");
			}
		}
		assert!(!output.status.success());
	};
}
