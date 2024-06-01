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
		let target = self.decompress_target(format);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn decompress_target(&self, format: tg::blob::compress::Format) -> tg::Target {
		let host = "builtin";
		let args = vec![
			"decompress".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Target::builder(host).args(args).build()
	}
}
