use crate as tg;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Tar,
	Tgar,
	Zip,
}

impl tg::Artifact {
	pub async fn archive<H>(&self, handle: &H, format: Format) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let target = self.archive_target(format);
		let arg = tg::target::build::Arg::default();
		let output = target.output(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn archive_target(&self, format: Format) -> tg::Target {
		let host = "builtin";
		let args = vec![
			"archive".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Target::builder(host).args(args).build()
	}
}

impl std::fmt::Display for Format {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tar => {
				write!(f, "tar")?;
			},
			Self::Tgar => {
				write!(f, "tgar")?;
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
			"tgar" => Ok(Self::Tgar),
			"zip" => Ok(Self::Zip),
			extension => Err(tg::error!(%extension, "invalid format")),
		}
	}
}
