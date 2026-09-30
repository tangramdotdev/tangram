use {
	serde::{Serialize, de::DeserializeOwned},
	serde_json::{Value, json},
	std::{io, path::Path, process::Stdio},
	tokio::{
		io::{
			AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
			BufReader,
		},
		process::{Child, ChildStdin, ChildStdout, Command},
	},
};

pub mod protocol;

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub struct Client {
	child: Child,
	input: BufReader<ChildStdout>,
	next_id: u64,
	output: ChildStdin,
}

pub trait Host: Sync {
	fn callback(
		&self,
		method: &str,
		params: Value,
	) -> impl Future<Output = io::Result<Value>> + Send;
}

impl Client {
	pub async fn new(executable: &Path) -> io::Result<Self> {
		let mut child = Command::new(executable)
			.args([
				"--api",
				"--async",
				"--cwd",
				"/",
				"--useCaseSensitiveFileNames=true",
				"--callbacks=readFile,fileExists,directoryExists,getAccessibleEntries,realpath:identity,stat:fakeStat,writeFile:noop,removeFile:noop",
			])
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.kill_on_drop(true)
			.spawn()?;

		let input = BufReader::new(child.stdout.take().unwrap());
		let output = child.stdin.take().unwrap();
		let client = Self {
			child,
			input,
			next_id: 1,
			output,
		};

		Ok(client)
	}

	pub async fn initialize(
		&mut self,
		host: &impl Host,
	) -> io::Result<protocol::InitializeResponse> {
		self.request("initialize", &Value::Null, host).await
	}

	pub async fn request<P: Serialize + Sync, R: DeserializeOwned>(
		&mut self,
		method: &str,
		params: &P,
		host: &impl Host,
	) -> io::Result<R> {
		let id = self.next_id;
		self.next_id += 1;
		let message = json!({ "id": id, "jsonrpc": "2.0", "method": method, "params": params });
		write_message(&mut self.output, &message).await?;

		loop {
			let message = read_message(&mut self.input).await?;
			if let Some(method) = message.get("method").and_then(Value::as_str) {
				let Some(callback_id) = message.get("id") else {
					continue;
				};
				let params = message.get("params").cloned().unwrap_or(Value::Null);
				let result = host.callback(method, params).await;
				let response = match result {
					Ok(result) => json!({ "id": callback_id, "jsonrpc": "2.0", "result": result }),
					Err(error) => {
						json!({
							"error": { "code": -32603, "message": error.to_string() },
							"id": callback_id,
							"jsonrpc": "2.0",
						})
					},
				};
				write_message(&mut self.output, &response).await?;
				continue;
			}
			if message.get("id").and_then(Value::as_u64) != Some(id) {
				return Err(io::Error::other("unexpected typescript response ID"));
			}
			if let Some(error) = message.get("error") {
				return Err(io::Error::other(format!(
					"the typescript API request {method} failed: {error}"
				)));
			}
			let result = message.get("result").cloned().unwrap_or(Value::Null);
			let result = serde_json::from_value(result)?;

			return Ok(result);
		}
	}

	pub async fn create_module_resolver(
		&mut self,
		options: &Value,
		callback: &str,
		host: &impl Host,
	) -> io::Result<u64> {
		let params = json!({ "compilerOptions": options, "resolveModuleNameCallback": callback });
		self.request("createModuleResolver", &params, host).await
	}

	pub async fn create_snapshot(
		&mut self,
		params: &protocol::CreateSnapshotParams,
		host: &impl Host,
	) -> io::Result<protocol::CreateSnapshotResponse> {
		self.request("createSnapshot", params, host).await
	}

	pub async fn diagnostics(
		&mut self,
		snapshot: u64,
		project: &str,
		host: &impl Host,
	) -> io::Result<Vec<protocol::Diagnostic>> {
		let params = json!({ "project": project, "snapshot": snapshot });
		let mut diagnostics = Vec::new();
		for method in [
			"getConfigFileParsingDiagnostics",
			"getProgramDiagnostics",
			"getGlobalDiagnostics",
			"getDeclarationDiagnostics",
			"getSyntacticDiagnostics",
			"getSemanticDiagnostics",
		] {
			let result: Option<Vec<protocol::Diagnostic>> =
				self.request(method, &params, host).await?;
			diagnostics.extend(result.unwrap_or_default());
		}

		Ok(diagnostics)
	}

	pub async fn release_snapshot(&mut self, snapshot: u64, host: &impl Host) -> io::Result<()> {
		let params = json!({ "snapshot": snapshot });
		let _: bool = self.request("release", &params, host).await?;
		Ok(())
	}

	pub async fn release_module_resolver(
		&mut self,
		resolver: u64,
		host: &impl Host,
	) -> io::Result<()> {
		let params = json!({ "resolver": resolver });
		self.request::<_, ()>("releaseModuleResolver", &params, host)
			.await?;
		Ok(())
	}

	pub async fn stop(mut self) -> io::Result<()> {
		self.child.kill().await?;
		self.child.wait().await?;
		Ok(())
	}
}

async fn write_message(output: &mut (impl AsyncWrite + Unpin), message: &Value) -> io::Result<()> {
	let body = serde_json::to_vec(message)?;
	let header = format!("Content-Length: {}\r\n\r\n", body.len());
	output.write_all(header.as_bytes()).await?;
	output.write_all(&body).await?;
	output.flush().await?;
	Ok(())
}

async fn read_message(input: &mut (impl AsyncBufRead + Unpin)) -> io::Result<Value> {
	let mut length = None;
	let mut header_bytes = 0;
	loop {
		let mut line = String::new();
		if input.read_line(&mut line).await? == 0 {
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"the typescript API connection closed",
			));
		}
		header_bytes += line.len();
		if header_bytes > 8192 {
			return Err(io::Error::other(
				"the typescript message header is too large",
			));
		}
		if line == "\r\n" {
			break;
		}
		if let Some((name, value)) = line.split_once(':')
			&& name.eq_ignore_ascii_case("Content-Length")
		{
			if length.is_some() {
				return Err(io::Error::other(
					"duplicate Content-Length header in the typescript response",
				));
			}
			length = Some(value.trim().parse::<usize>().map_err(io::Error::other)?);
		}
	}

	let length = length
		.filter(|length| *length <= MAX_MESSAGE_SIZE)
		.ok_or_else(|| {
			io::Error::other("invalid Content-Length header in the typescript response")
		})?;
	let mut body = vec![0; length];
	input.read_exact(&mut body).await?;
	let message = serde_json::from_slice(&body)?;

	Ok(message)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn framing() {
		let message = json!({ "id": 1, "result": "λ😀" });
		let mut bytes = Vec::new();
		write_message(&mut bytes, &message).await.unwrap();
		let actual = read_message(&mut bytes.as_slice()).await.unwrap();
		assert_eq!(actual, message);
	}

	#[tokio::test]
	async fn rejects_oversized_message() {
		let header = format!("Content-Length: {}\r\n\r\n", MAX_MESSAGE_SIZE + 1);
		assert!(read_message(&mut header.as_bytes()).await.is_err());
	}
}
