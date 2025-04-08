use crate as tg;

impl tg::Artifact {
	pub async fn extract<H>(handle: &H, blob: &tg::Blob) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = Self::extract_command(blob);
		let arg = tg::process::build::Arg::default();
		let output = tg::Process::build(handle, &command, arg).await?;
		let artifact = output.try_into()?;
		Ok(artifact)
	}

	#[must_use]
	pub fn extract_command(blob: &tg::Blob) -> tg::Command {
		let host = "builtin";
		let executable = tg::command::Executable::Path("extract".into());
		let args = vec![blob.clone().into()];
		tg::Command::builder(host, executable).args(args).build()
	}
}
