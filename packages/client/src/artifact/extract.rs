use crate as tg;

impl tg::Artifact {
	pub async fn extract<H>(handle: &H, blob: &tg::Blob) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = Self::extract_command(blob);
		let arg = tg::process::spawn::Arg {
			command: Some(command.id(handle).await?),
			..Default::default()
		};
		let output = tg::Process::run(handle, arg).await?;
		let artifact = output.try_into()?;
		Ok(artifact)
	}

	#[must_use]
	pub fn extract_command(blob: &tg::Blob) -> tg::Command {
		let host = "builtin";
		let args = vec!["extract".into(), blob.clone().into()];
		tg::Command::builder(host).args(args).build()
	}
}
