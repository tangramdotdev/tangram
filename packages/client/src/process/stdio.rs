use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
	tokio::io::AsyncRead,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	PartialEq,
	Eq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Stdio {
	#[tangram_serialize(id = 0)]
	Null,
	#[tangram_serialize(id = 1)]
	Log,
	#[tangram_serialize(id = 2)]
	Pipe,
	#[tangram_serialize(id = 3)]
	Pty,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Stream {
	#[tangram_serialize(id = 0)]
	Stdin,
	#[tangram_serialize(id = 1)]
	Stdout,
	#[tangram_serialize(id = 2)]
	Stderr,
}

impl tg::Client {
	pub async fn try_read_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static>> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stdin)
			.await
	}

	pub async fn write_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stdin, reader)
			.await
	}

	pub async fn try_read_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static>> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stdout)
			.await
	}

	pub async fn write_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stdout, reader)
			.await
	}

	pub async fn try_read_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static>> {
		self.try_read_process_stdio(id, arg, tg::process::stdio::Stream::Stderr)
			.await
	}

	pub async fn write_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio(id, arg, tg::process::stdio::Stream::Stderr, reader)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<tangram_futures::BoxAsyncRead<'static>>> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/{stream}/read?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::ACCEPT,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let reader = Box::pin(response.reader());
		Ok(Some(reader))
	}

	async fn write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/processes/{id}/{stream}/write?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.reader(reader)
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		Ok(())
	}
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Null => write!(f, "null"),
			Self::Log => write!(f, "log"),
			Self::Pipe => write!(f, "pipe"),
			Self::Pty => write!(f, "pty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"null" => Ok(Self::Null),
			"log" => Ok(Self::Log),
			"pipe" => Ok(Self::Pipe),
			"pty" => Ok(Self::Pty),
			_ => Err(tg::error!("invalid stdio")),
		}
	}
}

impl std::fmt::Display for Stream {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdin => write!(f, "stdin"),
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}

impl std::str::FromStr for Stream {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"stdin" => Ok(Self::Stdin),
			"stdout" => Ok(Self::Stdout),
			"stderr" => Ok(Self::Stderr),
			stream => Err(tg::error!(%stream, "unknown stream")),
		}
	}
}
