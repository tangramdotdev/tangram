use {crate::prelude::*, std::path::PathBuf};

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
	pub name: Option<String>,
	pub network: bool,
	pub parent: Option<tg::process::Id>,
	pub remote: Option<String>,
	pub retry: bool,
	pub stdin: Option<tg::Blob>,
	pub user: Option<String>,
}

pub async fn build<H>(handle: &H, arg: tg::build::Arg) -> tg::Result<tg::Value>
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
	let command_id = command.store(handle).await?;
	let mut command = tg::Referent::with_item(command_id);
	if let Some(name) = arg.name {
		command.options.name.replace(name);
	}
	if arg.network && arg.checksum.is_none() {
		return Err(tg::error!(
			"a checksum is required to build with network enabled"
		));
	}
	let arg = tg::process::spawn::Arg {
		cached: arg.cached,
		checksum: arg.checksum,
		command,
		local: None,
		mounts: vec![],
		network: arg.network,
		parent: arg.parent,
		remotes: arg.remote.map(|r| vec![r]),
		retry: arg.retry,
		stderr: None,
		stdin: None,
		stdout: None,
	};
	let process = tg::Process::spawn(handle, arg).await?;
	let output = process.output(handle).await?;
	Ok(output)
}
