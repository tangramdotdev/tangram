use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tokio::{
		io::{AsyncBufReadExt as _, AsyncWriteExt as _},
		net::UnixStream,
	},
};

#[derive(Clone)]
pub struct Server;

impl Server {
	pub async fn start(server: &crate::Server, path: &Path) -> tg::Result<Self> {
		#[derive(serde::Serialize)]
		struct Request {
			#[serde(rename = "data_directory")]
			data_directory: PathBuf,
			#[serde(rename = "mount_path")]
			mount_path: PathBuf,
			token: String,
			#[serde(rename = "type")]
			type_: &'static str,
		}

		#[derive(serde::Deserialize)]
		struct Response {
			message: Option<String>,
			#[serde(rename = "type")]
			type_: String,
		}

		let socket = std::env::var_os("TANGRAM_MACOS_APP_SOCKET")
			.ok_or_else(|| tg::error!("missing the macos app socket environment variable"))?;
		let token = std::env::var("TANGRAM_MACOS_APP_TOKEN").map_err(|error| {
			tg::error!(!error, "missing the macos app token environment variable")
		})?;
		let stream = UnixStream::connect(socket)
			.await
			.map_err(|error| tg::error!(!error, "failed to connect to the macos app"))?;
		let (reader, mut writer) = stream.into_split();
		let request = Request {
			data_directory: server.path.clone(),
			mount_path: path.to_path_buf(),
			token,
			type_: "mount_vfs",
		};
		let mut request = serde_json::to_vec(&request)
			.map_err(|error| tg::error!(!error, "failed to encode the fskit request"))?;
		request.push(b'\n');
		writer
			.write_all(&request)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the fskit request"))?;
		writer
			.flush()
			.await
			.map_err(|error| tg::error!(!error, "failed to flush the fskit request"))?;

		let mut reader = tokio::io::BufReader::new(reader);
		let mut response = String::new();
		reader
			.read_line(&mut response)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the fskit response"))?;
		let response = serde_json::from_str::<Response>(&response)
			.map_err(|error| tg::error!(!error, "failed to decode the fskit response"))?;
		if response.type_ != "ok" {
			let message = response
				.message
				.unwrap_or_else(|| "unknown error".to_owned());
			return Err(tg::error!("failed to start the fskit vfs: {message}"));
		}
		Ok(Self)
	}
}
