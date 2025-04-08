use crate as tg;
use futures::FutureExt as _;

impl tg::Artifact {
	pub async fn bundle<H>(&self, handle: &H) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = self.bundle_command();
		let arg = tg::process::build::Arg::default();
		let output = tg::Process::build(handle, &command, arg).boxed().await?;
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
