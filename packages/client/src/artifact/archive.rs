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
		compression_format: Option<tg::blob::compress::Format>,
	) -> tg::Result<tg::Blob>
	where
		H: tg::Handle,
	{
		let command = self.archive_command(format, compression_format);
		let arg = tg::process::spawn::Arg {
			command: Some(command.id(handle).await?),
			..Default::default()
		};
		let output = tg::Process::run(handle, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn archive_command(
		&self,
		format: Format,
		compression_format: Option<tg::blob::compress::Format>,
	) -> tg::Command {
		let host = "builtin";
		let args = vec![
			"archive".into(),
			self.clone().into(),
			format.to_string().into(),
			compression_format
				.map(|compression_format| compression_format.to_string())
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
