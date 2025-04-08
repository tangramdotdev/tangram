use crate as tg;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub cached: Option<bool>,
	pub checksum: Option<tg::Checksum>,
	pub network: bool,
	pub parent: Option<tg::process::Id>,
	pub remote: Option<String>,
	pub retry: bool,
}

impl tg::Process {
	pub async fn build<H>(
		handle: &H,
		command: &tg::Command,
		arg: tg::process::build::Arg,
	) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
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
		let wait = process.wait(handle).await?;
		if let Some(error) = wait.error {
			return Err(error);
		}
		match wait.exit {
			Some(tg::process::Exit::Code { code }) => {
				if code != 0 {
					return Err(tg::error!("the process exited with code {code}"));
				}
			},
			Some(tg::process::Exit::Signal { signal }) => {
				return Err(tg::error!("the process exited with signal {signal}"));
			},
			_ => (),
		}
		let output = wait
			.output
			.ok_or_else(|| tg::error!(%process = process.id(), "expected the output to be set"))?;
		Ok(output)
	}
}
