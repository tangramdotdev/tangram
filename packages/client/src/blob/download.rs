use crate as tg;
use url::Url;

impl tg::Blob {
	pub async fn download<H>(handle: &H, url: &Url, checksum: &tg::Checksum) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let command = Self::download_command(url);
		let arg = tg::process::spawn::Arg {
			checksum: Some(checksum.clone()),
			command: Some(command.id(handle).await?),
			..Default::default()
		};
		let output = tg::Process::run(handle, arg).await?;
		let blob = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?
			.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn download_command(url: &Url) -> tg::Command {
		let host = "builtin";
		let args = vec!["download".into(), url.to_string().into()];
		tg::Command::builder(host).args(args).build()
	}
}
