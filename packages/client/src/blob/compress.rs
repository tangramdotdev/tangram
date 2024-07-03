use crate as tg;

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
		let output = target.output(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn compress_target(&self, format: tg::blob::compress::Format) -> tg::Target {
		let host = "builtin";
		let args = vec![
			"compress".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Target::builder(host).args(args).build()
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
