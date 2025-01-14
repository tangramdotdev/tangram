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
		let command = self.checksum_command(algorithm);
		let arg = tg::command::spawn::Arg::default();
		let output = command.output(handle, arg).boxed().await?;
		let checksum = output
			.try_unwrap_string()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse()?;
		Ok(checksum)
	}

	#[must_use]
	pub fn checksum_command(&self, algorithm: tg::checksum::Algorithm) -> tg::Command {
		let host = "builtin";
		let args = vec![
			"checksum".into(),
			self.clone().into(),
			algorithm.to_string().into(),
		];
		tg::Command::builder(host).args(args).build()
	}
}
