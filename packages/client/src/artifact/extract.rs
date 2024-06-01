use crate as tg;

impl tg::Artifact {
	pub async fn extract<H>(
		handle: &H,
		blob: &tg::Blob,
		format: Option<tg::artifact::archive::Format>,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let target = Self::extract_target(blob, format);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).await?;
		let artifact = output.try_into()?;
		Ok(artifact)
	}

	#[must_use]
	pub fn extract_target(
		blob: &tg::Blob,
		format: Option<tg::artifact::archive::Format>,
	) -> tg::Target {
		let host = "builtin";
		let args = vec![
			"extract".into(),
			blob.clone().into(),
			format.map(|format| format.to_string()).into(),
		];
		tg::Target::builder(host).args(args).build()
	}
}
