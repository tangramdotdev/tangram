use crate as tg;

impl tg::Blob {
	pub async fn decompress<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = self.decompress_command();
		let arg = tg::process::spawn::Arg {
			command: Some(command.id(handle).await?),
			..Default::default()
		};
		let output = tg::Process::run(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn decompress_command(&self) -> tg::Command {
		let host = "builtin";
		let args = vec!["decompress".into(), self.clone().into()];
		tg::Command::builder(host).args(args).build()
	}
}
