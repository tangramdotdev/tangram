use crate as tg;
use futures::FutureExt as _;
use url::Url;

impl tg::Blob {
	pub async fn download<H>(handle: &H, url: &Url, checksum: &tg::Checksum) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let target = Self::download_target(url, checksum);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).boxed().await?;
		let blob = output
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?
			.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn download_target(url: &Url, checksum: &tg::Checksum) -> tg::Target {
		let host = "js";
		let executable = "export default tg.target((...args) => tg.download(...args));";
		let args = vec![
			"default".into(),
			url.to_string().into(),
			checksum.to_string().into(),
		];
		tg::Target::builder(host, executable).args(args).build()
	}
}
