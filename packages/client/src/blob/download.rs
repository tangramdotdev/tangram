use crate as tg;
use url::Url;

impl tg::Blob {
	pub async fn download<H>(handle: &H, url: &Url, checksum: &tg::Checksum) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let target = Self::download_target(url, checksum);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).await?;
		let blob = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?
			.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn download_target(url: &Url, checksum: &tg::Checksum) -> tg::Target {
		let host = "builtin";
		let args = vec!["download".into(), url.to_string().into()];
		tg::Target::builder(host)
			.args(args)
			.checksum(checksum.clone())
			.build()
	}
}
