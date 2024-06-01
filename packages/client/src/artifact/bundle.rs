use crate as tg;
use futures::FutureExt as _;

impl tg::Artifact {
	pub async fn bundle<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let target = self.bundle_target();
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).boxed().await?;
		let artifact = output.try_into()?;
		Ok(artifact)
	}

	#[must_use]
	pub fn bundle_target(&self) -> tg::Target {
		let host = "builtin";
		let args = vec!["bundle".into(), self.clone().into()];
		tg::Target::builder(host).args(args).build()
	}
}
