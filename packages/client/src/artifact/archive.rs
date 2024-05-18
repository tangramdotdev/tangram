use crate as tg;
use futures::FutureExt as _;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Tar,
	Zip,
}

impl tg::Artifact {
	pub async fn archive<H>(&self, handle: &H, format: Format) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let target = self.archive_target(format);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).boxed().await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn archive_target(&self, format: Format) -> tg::Target {
		let host = "js";
		let executable = "export default tg.target((...args) => tg.archive(...args));";
		let args = vec![
			"default".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Target::builder(host, executable).args(args).build()
	}
}

impl std::fmt::Display for Format {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tar => {
				write!(f, "tar")?;
			},
			Self::Zip => {
				write!(f, "zip")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Format {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"tar" => Ok(Self::Tar),
			"zip" => Ok(Self::Zip),
			extension => Err(tg::error!(%extension, "invalid format")),
		}
	}
}

impl From<Format> for String {
	fn from(value: Format) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Format {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}
