use {crate::prelude::*, std::collections::BTreeMap, tangram_uri::Uri};

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum ArchiveFormat {
	Tar,
	Zip,
}

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum CompressionFormat {
	Bz2,
	Gz,
	Xz,
	Zstd,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct DownloadOptions {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::checksum::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<DownloadMode>,
}

#[derive(
	Clone, Copy, Debug, Default, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub enum DownloadMode {
	#[default]
	Raw,
	Decompress,
	Extract,
}

pub async fn archive<H>(
	artifact: &tg::Artifact,
	handle: &H,
	format: tg::ArchiveFormat,
	compression: Option<tg::CompressionFormat>,
) -> tg::Result<tg::Blob>
where
	H: tg::Handle,
{
	let command = archive_command(artifact, format, compression);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let blob = output.try_into()?;
	Ok(blob)
}

#[must_use]
pub fn archive_command(
	artifact: &tg::Artifact,
	format: tg::ArchiveFormat,
	compression: Option<tg::CompressionFormat>,
) -> tg::Command {
	let host = "builtin";
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "archive".into(),
	});
	let args = vec![
		artifact.clone().into(),
		format.to_string().into(),
		compression
			.map(|compression| compression.to_string())
			.into(),
	];
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn bundle<H>(artifact: &tg::Artifact, handle: &H) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
{
	let command = bundle_command(artifact);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let artifact = output.try_into()?;
	Ok(artifact)
}

#[must_use]
pub fn bundle_command(artifact: &tg::Artifact) -> tg::Command {
	let host = "builtin";
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "bundle".into(),
	});
	let args = vec![artifact.clone().into()];
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn checksum<H>(
	input: tg::Either<&tg::Blob, &tg::Artifact>,
	handle: &H,
	algorithm: tg::checksum::Algorithm,
) -> tg::Result<tg::Checksum>
where
	H: tg::Handle,
{
	let command = checksum_command(input.cloned(), algorithm);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let checksum = output
		.try_unwrap_string()
		.ok()
		.ok_or_else(|| tg::error!("expected a string"))?
		.parse()
		.map_err(|source| tg::error!(!source, "failed to parse the checksum"))?;
	Ok(checksum)
}

#[must_use]
pub fn checksum_command(
	input: tg::Either<tg::Blob, tg::Artifact>,
	algorithm: tg::checksum::Algorithm,
) -> tg::Command {
	let host = "builtin";
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "checksum".into(),
	});
	let args = vec![input.into(), algorithm.to_string().into()];
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn compress<H>(
	input: &tg::Blob,
	handle: &H,
	format: tg::CompressionFormat,
) -> tg::Result<tg::Blob>
where
	H: tg::Handle,
{
	let command = compress_command(input, format);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let blob = output.try_into()?;
	Ok(blob)
}

#[must_use]
pub fn compress_command(input: &tg::Blob, format: tg::CompressionFormat) -> tg::Command {
	let host = "builtin";
	let args = vec![input.clone().into(), format.to_string().into()];
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "compress".into(),
	});
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn decompress<H>(input: &tg::Blob, handle: &H) -> tg::Result<tg::Blob>
where
	H: tg::Handle,
{
	let command = decompress_command(input);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let blob = output.try_into()?;
	Ok(blob)
}

#[must_use]
pub fn decompress_command(input: &tg::Blob) -> tg::Command {
	let host = "builtin";
	let args = vec![input.clone().into()];
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "decompress".into(),
	});
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn download<H>(
	handle: &H,
	url: &Uri,
	checksum: &tg::Checksum,
	options: Option<DownloadOptions>,
) -> tg::Result<tg::Either<tg::Blob, tg::Artifact>>
where
	H: tg::Handle,
{
	let command = download_command(url, options);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command_and_checksum(command, Some(checksum.clone()));
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let output = if output.is_blob() {
		tg::Either::Left(output.try_into()?)
	} else if output.is_artifact() {
		tg::Either::Right(output.try_into()?)
	} else {
		return Err(tg::error!("expected a blob or an artifact"));
	};
	Ok(output)
}

#[must_use]
pub fn download_command(url: &Uri, options: Option<DownloadOptions>) -> tg::Command {
	let host = "builtin";
	let mut args = vec![url.to_string().into()];
	if let Some(options) = options {
		args.push(options.into());
	}
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "download".into(),
	});
	tg::Command::builder(host, executable).args(args).build()
}

pub async fn extract<H>(handle: &H, input: &tg::Blob) -> tg::Result<tg::Artifact>
where
	H: tg::Handle,
{
	let command = extract_command(input);
	let command = command.store(handle).await?;
	let command = tg::Referent::with_item(command);
	let arg = tg::process::spawn::Arg::with_command(command);
	let output = tg::Process::spawn(handle, arg)
		.await?
		.wait(handle)
		.await?
		.into_output()?;
	let artifact = output.try_into()?;
	Ok(artifact)
}

#[must_use]
pub fn extract_command(input: &tg::Blob) -> tg::Command {
	let host = "builtin";
	let args = vec![input.clone().into()];
	let executable = tg::command::Executable::Path(tg::command::PathExecutable {
		path: "extract".into(),
	});
	tg::Command::builder(host, executable).args(args).build()
}

impl std::fmt::Display for ArchiveFormat {
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

impl std::str::FromStr for ArchiveFormat {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"tar" => Ok(Self::Tar),
			"zip" => Ok(Self::Zip),
			format => Err(tg::error!(%format, "invalid format")),
		}
	}
}

impl std::fmt::Display for CompressionFormat {
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

impl std::str::FromStr for CompressionFormat {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"bz2" => Ok(Self::Bz2),
			"gz" => Ok(Self::Gz),
			"xz" => Ok(Self::Xz),
			"zst" => Ok(Self::Zstd),
			format => Err(tg::error!(%format, "invalid compression format")),
		}
	}
}

impl std::fmt::Display for DownloadMode {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Raw => {
				write!(f, "raw")?;
			},
			Self::Decompress => {
				write!(f, "decompress")?;
			},
			Self::Extract => {
				write!(f, "extract")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for DownloadMode {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"raw" => Ok(Self::Raw),
			"decompress" => Ok(Self::Decompress),
			"extract" => Ok(Self::Extract),
			mode => Err(tg::error!(%mode, "invalid mode")),
		}
	}
}

impl From<DownloadOptions> for tg::Value {
	fn from(options: DownloadOptions) -> Self {
		let mut map = BTreeMap::new();
		if let Some(mode) = options.mode {
			map.insert("mode".to_owned(), mode.to_string().into());
		}
		tg::Value::Map(map)
	}
}

impl TryFrom<tg::Value> for DownloadOptions {
	type Error = tg::Error;

	fn try_from(value: tg::Value) -> Result<Self, Self::Error> {
		let mut options = Self::default();
		let map = value
			.try_unwrap_map()
			.ok()
			.ok_or_else(|| tg::error!("expected a map"))?;
		if let Some(value) = map.get("mode") {
			let mode = value
				.clone()
				.try_unwrap_string()
				.ok()
				.ok_or_else(|| tg::error!("expected a string"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the mode"))?;
			options.mode = Some(mode);
		}
		Ok(options)
	}
}
