use crate as tg;
use futures::FutureExt as _;

impl tg::Artifact {
	pub async fn checksum<H>(
		&self,
		handle: &H,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum>
	where
		H: tg::Handle,
	{
		let target = self.checksum_target(algorithm);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).boxed().await?;
		let checksum = output
			.try_unwrap_string()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse()?;
		Ok(checksum)
	}

	#[must_use]
	pub fn checksum_target(&self, algorithm: tg::checksum::Algorithm) -> tg::Target {
		let host = "builtin";
		let args = vec![
			"checksum".into(),
			self.clone().into(),
			algorithm.to_string().into(),
		];
		tg::Target::builder(host).args(args).build()
	}
}
