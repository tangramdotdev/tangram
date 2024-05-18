use crate as tg;
use futures::FutureExt as _;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Bz2,
	Gz,
	Xz,
	Zstd,
}

impl tg::Blob {
	pub async fn compress<H>(
		&self,
		handle: &H,
		format: tg::blob::compress::Format,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		let target = self.compress_target(format);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).boxed().await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn compress_target(&self, format: tg::blob::compress::Format) -> tg::Target {
		let host = "js";
		let executable = "export default tg.target((...args) => tg.compress(...args));";
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
		let s = match self {
			Self::Bz2 => "bz2",
			Self::Gz => "gz",
			Self::Xz => "xz",
			Self::Zstd => "zst",
		};
		write!(f, "{s}")?;
		Ok(())
	}
}

impl std::str::FromStr for Format {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"bz2" => Ok(Self::Bz2),
			"gz" => Ok(Self::Gz),
			"xz" => Ok(Self::Xz),
			"zst" => Ok(Self::Zstd),
			extension => Err(tg::error!(%extension, "invalid compression format")),
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
