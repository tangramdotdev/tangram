use insta::assert_snapshot;
use serde_json::json;
use std::path::PathBuf;
use tangram_temp::Temp;

#[tokio::test]
async fn test() -> std::io::Result<()> {
	let config = json!({});
	let mut server = Server::start(config).await?;
	let result = AssertUnwindSafe(async { Ok::<_, std::io::Error>(()) })
		.catch_unwind()
		.await;
	let args = ["health"];
	let output = server.tg().args(args).output().await?;
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert_snapshot!(stdout, @r"");
	server.stop()?;
	server.wait().await?;
	temp.remove().await?;
	Ok(())
}

const TG: &str = env!("CARGO_BIN_EXE_tangram");

struct Server {
	child: tokio::process::Child,
	config_path: PathBuf,
	data_path: PathBuf,
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
		let args = ["serve"];
		let child = tokio::process::Command::new(TG).args(args).spawn().unwrap();
		Ok(Self {
			child,
			config_path,
			data_path,
			temp,
		})
	}

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
}
