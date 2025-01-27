use crate as tg;
use url::Url;

impl tg::Blob {
	pub async fn download<H>(handle: &H, url: &Url, checksum: &tg::Checksum) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let host = "builtin";
		let args = vec!["download".into(), url.to_string().into()];
		let command = tg::Command::builder(host).args(args).build();
		let arg = tg::command::spawn::Arg {
			checksum: Some(checksum.clone()),
			..Default::default()
		};
		let output = command.output(handle, arg).await?;
		let blob = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?
			.try_into()?;
		Ok(blob)
	}
}
