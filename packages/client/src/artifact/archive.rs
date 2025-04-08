use crate as tg;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Format {
	Tar,
	Zip,
}

impl tg::Artifact {
	pub async fn archive<H>(
		&self,
		handle: &H,
		format: Format,
		compression: Option<tg::blob::compress::Format>,
	) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let command = self.archive_command(format, compression);
		let arg = tg::process::build::Arg::default();
		let output = tg::Process::build(handle, &command, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn archive_command(
		&self,
		format: Format,
		compression: Option<tg::blob::compress::Format>,
	) -> tg::Command {
		let host = "builtin";
		let args = vec![
			"archive".into(),
			self.clone().into(),
			format.to_string().into(),
			compression
				.map(|compression| compression.to_string())
				.into(),
		];
		tg::Command::builder(host).args(args).build()
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
