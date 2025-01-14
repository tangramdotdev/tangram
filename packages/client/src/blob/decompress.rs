use crate as tg;

impl tg::Blob {
	pub async fn decompress<H>(
		&self,
		handle: &H,
		format: tg::blob::compress::Format,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = self.decompress_command(format);
		let arg = tg::command::spawn::Arg::default();
		let output = command.output(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn decompress_command(&self, format: tg::blob::compress::Format) -> tg::Command {
		let host = "builtin";
		let args = vec![
			"decompress".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Command::builder(host).args(args).build()
	}
}
