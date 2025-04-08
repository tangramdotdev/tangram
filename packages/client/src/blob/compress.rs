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
		let command = self.compress_command(format);
		let arg = tg::process::build::Arg::default();
		let output = tg::Process::build(handle, &command, arg).await?;
		let blob = output.try_into()?;
		Ok(blob)
	}

	#[must_use]
	pub fn compress_command(&self, format: tg::blob::compress::Format) -> tg::Command {
		let host = "builtin";
		let args = vec![
			"compress".into(),
			self.clone().into(),
			format.to_string().into(),
		];
		tg::Command::builder(host).args(args).build()
	}
}

impl Format {
	#[must_use]
	pub fn with_magic_number(bytes: &[u8]) -> Option<Self> {
		let len = bytes.len();
		if len < 2 {
			return None;
		}

		// Gz
		if bytes[0] == 0x1F && bytes[1] == 0x8B {
			return Some(tg::blob::compress::Format::Gz);
		}

		// Zstd
		if len >= 4 && bytes[0] == 0x28 && bytes[1] == 0xB5 && bytes[2] == 0x2F && bytes[3] == 0xFD
		{
			return Some(tg::blob::compress::Format::Zstd);
		}

		// Bz2
		if len >= 3 && bytes[0] == 0x42 && bytes[1] == 0x5A && bytes[2] == 0x68 {
			return Some(tg::blob::compress::Format::Bz2);
		}

		// Xz
		if len >= 6
			&& bytes[0] == 0xFD
			&& bytes[1] == 0x37
			&& bytes[2] == 0x7A
			&& bytes[3] == 0x58
			&& bytes[4] == 0x5A
			&& bytes[5] == 0x00
		{
			return Some(tg::blob::compress::Format::Xz);
		}

		None
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
