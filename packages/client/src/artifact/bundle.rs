use crate as tg;

impl tg::Artifact {
	pub async fn bundle<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = self.bundle_command();
		let arg = tg::process::spawn::Arg {
			command: Some(command.id(handle).await?),
			..Default::default()
		};
		let output = tg::Process::spawn(handle, arg)
			.await?
			.wait(handle)
			.await?
			.into_output()?;
		let artifact = output.try_into()?;
		Ok(artifact)
	}

	#[must_use]
	pub fn bundle_command(&self) -> tg::Command {
		let host = "builtin";
		let executable = tg::command::Executable::Path("bundle".into());
		let args = vec!["bundle".into(), self.clone().into()];
		tg::Command::builder(host, executable).args(args).build()
	}
}
