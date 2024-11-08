use futures::{Future, FutureExt as _};
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tangram_temp::Temp;

pub async fn test<F, Fut>(
	mut config: serde_json::Value,
	args: &[&str],
	assertions: F,
) -> std::io::Result<()>
where
	F: FnOnce(&Server, String, String) -> Fut,
	Fut: Future<Output = std::io::Result<()>>,
{
	if let Some(obj) = config.as_object_mut() {
		obj.insert("tracing".to_string(), json!({}));
	};
	let mut server = Server::start(config).await?;
	let result = AssertUnwindSafe(async {
		let output = server.tg().args(args).output().await?;
		let stdout = String::from_utf8(output.stdout).unwrap();
		let stderr = String::from_utf8(output.stderr).unwrap();
		(assertions)(&server, stdout, stderr).await?;
		Ok::<_, std::io::Error>(())
	})
	.catch_unwind()
	.await;
	server.cleanup().await?;
	result.unwrap()
}

const TG: &str = env!("CARGO_BIN_EXE_tangram");

pub struct Server {
	child: tokio::process::Child,
	pub config_path: PathBuf,
	pub data_path: PathBuf,
	temp: Temp,
}

impl Server {
	pub async fn start(config: serde_json::Value) -> std::io::Result<Self> {
		let temp = Temp::new();
		tokio::fs::create_dir_all(temp.path()).await?;
		let config_path = temp.path().join(".config/tangram/config.json");
		tokio::fs::create_dir_all(config_path.parent().unwrap()).await?;
		let config = serde_json::to_vec_pretty(&config).unwrap();
		tokio::fs::write(&config_path, config).await?;
		let data_path = temp.path().join(".tangram");
		tokio::fs::create_dir_all(&data_path).await?;
		let args = [
			"--config",
			config_path.to_str().unwrap(),
			"--path",
			data_path.to_str().unwrap(),
			"serve",
		];
		let child = tokio::process::Command::new(TG).args(args).spawn().unwrap();
		Ok(Self {
			child,
			config_path,
			data_path,
			temp,
		})
	}

	#[must_use]
	pub fn tg(&self) -> tokio::process::Command {
		let config = self.config_path.to_str().unwrap();
		let path = self.data_path.to_str().unwrap();
		let mut command = tokio::process::Command::new(TG);
		command.args(["--config", config, "--path", path, "--mode", "client"]);
		command
	}

	pub fn stop(&mut self) -> std::io::Result<()> {
		self.child.start_kill()
	}

	pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
		self.child.wait().await
	}

	pub async fn cleanup(&mut self) -> std::io::Result<()> {
		self.stop()?;
		self.wait().await?;
		self.temp.remove().await?;
		Ok(())
	}
}
