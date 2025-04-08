use crate as tg;
use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub args: tg::value::Array,
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub cwd: Option<PathBuf>,
	pub env: tg::value::Map,
	pub executable: Option<tg::command::Executable>,
	pub host: Option<String>,
	pub mounts: Vec<tg::command::Mount>,
	pub network: bool,
	pub parent: Option<tg::process::Id>,
	pub remote: Option<String>,
	pub retry: bool,
	pub stdin: Option<tg::Blob>,
	pub user: Option<String>,
}

impl tg::Process {
	pub async fn build<H>(handle: &H, arg: tg::process::build::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let host = arg
			.host
			.ok_or_else(|| tg::error!("expected the host to be set"))?;
		let executable = arg
			.executable
			.ok_or_else(|| tg::error!("expected the executable to be set"))?;
		let mut builder = tg::Command::builder(host, executable);
		builder = builder.args(arg.args);
		builder = builder.cwd(arg.cwd);
		builder = builder.env(arg.env);
		builder = builder.mounts(arg.mounts);
		builder = builder.stdin(arg.stdin);
		builder = builder.user(arg.user);
		let command = builder.build();
		let command_id = command.id(handle).await?;
		if arg.network && arg.checksum.is_none() {
			return Err(tg::error!(
				"a checksum is required to build with network enabled"
			));
		}
		let arg = tg::process::spawn::Arg {
			cached: arg.cached,
			checksum: arg.checksum,
			command: Some(command_id),
			mounts: vec![],
			network: arg.network,
			parent: arg.parent,
			remote: arg.remote,
			retry: arg.retry,
			stderr: None,
			stdin: None,
			stdout: None,
		};
		let process = Self::spawn(handle, arg).await?;
		let output = process.output(handle).await?;
		Ok(output)
	}
}
