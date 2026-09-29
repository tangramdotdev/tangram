use {
	crate::prelude::*,
	futures::{FutureExt as _, TryStreamExt as _, stream::FuturesOrdered},
	std::path::{Path, PathBuf},
	tangram_uri::Uri,
};

const TANGRAM_STORE_PATH: &str = ".tangram/store";

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum ArchiveFormat {
	Tar,
	Zip,
}

#[derive(
	Clone,
	Copy,
	Debug,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[display(rename_all = "lowercase")]
#[from_str(rename_all = "lowercase")]
pub enum CompressionFormat {
	Bz2,
	Gz,
	Xz,
	Zst,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct DownloadOptions {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub checksum: Option<tg::checksum::Algorithm>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub mode: Option<DownloadMode>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum DownloadMode {
	#[default]
	Raw,
	Decompress,
	Extract,
}

pub async fn archive(
	artifact: &tg::Artifact,
	format: tg::ArchiveFormat,
	compression: Option<tg::CompressionFormat>,
) -> tg::Result<tg::Blob> {
	let instance = tg::instance()?;
	archive_with_instance(artifact, instance, format, compression)
		.boxed_local()
		.await
}

pub async fn archive_with_instance<I>(
	artifact: &tg::Artifact,
	instance: &I,
	format: tg::ArchiveFormat,
	compression: Option<tg::CompressionFormat>,
) -> tg::Result<tg::Blob>
where
	I: tg::Instance,
{
	validate_archive_artifact_with_instance(artifact, instance).await?;
	let mut args = vec![tg::Value::from("builtin"), "archive".into()];
	if let Some(compression) = compression {
		args.extend(["--compression".into(), compression.to_string().into()]);
	}
	args.extend([
		"--format".into(),
		format.to_string().into(),
		"--input".into(),
		artifact.clone().into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	]);
	let arg = tg::process::Arg {
		args,
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("archive".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let file: tg::File = output.try_into()?;
	let blob = file.contents_with_instance(instance).await?;

	Ok(blob)
}

pub async fn validate_archive_artifact_with_instance<I>(
	artifact: &tg::Artifact,
	instance: &I,
) -> tg::Result<()>
where
	I: tg::Instance,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			for artifact in directory.entries_with_instance(instance).await?.values() {
				Box::pin(validate_archive_artifact_with_instance(artifact, instance)).await?;
			}
		},
		tg::Artifact::File(file) => {
			if !file.dependencies_with_instance(instance).await?.is_empty() {
				return Err(tg::error!("cannot archive a file with dependencies"));
			}
		},
		tg::Artifact::Symlink(symlink) => {
			if symlink.artifact_with_instance(instance).await?.is_some() {
				return Err(tg::error!("cannot archive a symlink with an artifact"));
			}
			if symlink.path_with_instance(instance).await?.is_none() {
				return Err(tg::error!("cannot archive a symlink without a path"));
			}
		},
	}

	Ok(())
}

#[must_use]
pub fn archive_command(
	artifact: &tg::Artifact,
	format: tg::ArchiveFormat,
	compression: Option<tg::CompressionFormat>,
) -> tg::Command {
	let mut args: Vec<tg::command::Value> = vec!["builtin".into(), "archive".into()];
	if let Some(compression) = compression {
		args.extend(["--compression".into(), compression.to_string().into()]);
	}
	args.extend([
		"--format".into(),
		format.to_string().into(),
		"--input".into(),
		artifact.clone().into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	]);
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

pub async fn bundle(artifact: &tg::Artifact) -> tg::Result<tg::Artifact> {
	let instance = tg::instance()?;
	bundle_with_instance(artifact, instance).await
}

pub async fn bundle_with_instance<I>(
	artifact: &tg::Artifact,
	instance: &I,
) -> tg::Result<tg::Artifact>
where
	I: tg::Instance,
{
	let dependencies = Box::pin(artifact.recursive_dependencies_with_instance(instance)).await?;
	if dependencies.is_empty() {
		return Ok(artifact.clone());
	}
	let entries = dependencies
		.into_iter()
		.map(|id| async move {
			let artifact = tg::Artifact::with_id(id.clone());
			let artifact = remove_dependencies(instance, &artifact, 3).await?;
			Ok::<_, tg::Error>((id.to_string(), artifact))
		})
		.collect::<FuturesOrdered<_>>()
		.try_collect()
		.await?;
	let artifacts = tg::Directory::with_entries(entries);
	let directory = artifact
		.clone()
		.try_unwrap_directory()
		.map_err(|_| tg::error!("the artifact must be a directory"))?;
	let directory = remove_dependencies(instance, &directory.into(), 0)
		.await?
		.try_unwrap_directory()
		.map_err(|_| tg::error!("the artifact must be a directory"))?;
	let directory = directory
		.to_builder_with_instance(instance)
		.await?
		.add_with_instance(instance, TANGRAM_STORE_PATH.as_ref(), artifacts.into())
		.await?
		.build();

	Ok(directory.into())
}

pub async fn checksum(
	input: tg::Either<&tg::Blob, &tg::File>,
	algorithm: tg::checksum::Algorithm,
) -> tg::Result<tg::Checksum> {
	let instance = tg::instance()?;
	checksum_with_instance(input, instance, algorithm)
		.boxed_local()
		.await
}

pub async fn checksum_with_instance<I>(
	input: tg::Either<&tg::Blob, &tg::File>,
	instance: &I,
	algorithm: tg::checksum::Algorithm,
) -> tg::Result<tg::Checksum>
where
	I: tg::Instance,
{
	let input = match input {
		tg::Either::Left(blob) => tg::File::with_contents(blob.clone()),
		tg::Either::Right(file) => file.clone(),
	};
	let args = vec![
		tg::Value::from("builtin"),
		"checksum".into(),
		"--algorithm".into(),
		algorithm.to_string().into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let arg = tg::process::Arg {
		args,
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("checksum".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let output: tg::File = output.try_into()?;
	let checksum = output
		.text_with_instance(instance)
		.await?
		.parse()
		.map_err(|error| tg::error!(!error, "failed to parse the checksum"))?;

	Ok(checksum)
}

#[must_use]
pub fn checksum_command(
	input: tg::Either<tg::Blob, tg::File>,
	algorithm: tg::checksum::Algorithm,
) -> tg::Command {
	let input = match input {
		tg::Either::Left(blob) => tg::File::with_contents(blob),
		tg::Either::Right(file) => file,
	};
	let args: Vec<tg::command::Value> = vec![
		"builtin".into(),
		"checksum".into(),
		"--algorithm".into(),
		algorithm.to_string().into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

pub async fn compress(input: &tg::Blob, format: tg::CompressionFormat) -> tg::Result<tg::Blob> {
	let instance = tg::instance()?;
	compress_with_instance(input, instance, format)
		.boxed_local()
		.await
}

pub async fn compress_with_instance<I>(
	input: &tg::Blob,
	instance: &I,
	format: tg::CompressionFormat,
) -> tg::Result<tg::Blob>
where
	I: tg::Instance,
{
	let input = tg::File::with_contents(input.clone());
	let args = vec![
		tg::Value::from("builtin"),
		"compress".into(),
		"--format".into(),
		format.to_string().into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let arg = tg::process::Arg {
		args,
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("compress".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let file: tg::File = output.try_into()?;
	let blob = file.contents_with_instance(instance).await?;

	Ok(blob)
}

#[must_use]
pub fn compress_command(
	input: tg::Either<tg::Blob, tg::File>,
	format: tg::CompressionFormat,
) -> tg::Command {
	let input = match input {
		tg::Either::Left(blob) => tg::File::with_contents(blob),
		tg::Either::Right(file) => file,
	};
	let args: Vec<tg::command::Value> = vec![
		"builtin".into(),
		"compress".into(),
		"--format".into(),
		format.to_string().into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

pub async fn decompress(input: &tg::Blob) -> tg::Result<tg::Blob> {
	let instance = tg::instance()?;
	decompress_with_instance(input, instance)
		.boxed_local()
		.await
}

pub async fn decompress_with_instance<I>(input: &tg::Blob, instance: &I) -> tg::Result<tg::Blob>
where
	I: tg::Instance,
{
	let input = tg::File::with_contents(input.clone());
	let args = vec![
		tg::Value::from("builtin"),
		"decompress".into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let arg = tg::process::Arg {
		args,
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("decompress".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let file: tg::File = output.try_into()?;
	let blob = file.contents_with_instance(instance).await?;

	Ok(blob)
}

#[must_use]
pub fn decompress_command(input: tg::Either<tg::Blob, tg::File>) -> tg::Command {
	let input = match input {
		tg::Either::Left(blob) => tg::File::with_contents(blob),
		tg::Either::Right(file) => file,
	};
	let args: Vec<tg::command::Value> = vec![
		"builtin".into(),
		"decompress".into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

pub async fn download(
	url: &Uri,
	checksum: Option<&tg::Checksum>,
	options: Option<DownloadOptions>,
) -> tg::Result<tg::Either<tg::Blob, tg::Artifact>> {
	let instance = tg::instance()?;
	download_with_instance(instance, url, checksum, options)
		.boxed_local()
		.await
}

pub async fn download_with_instance<I>(
	instance: &I,
	url: &Uri,
	checksum: Option<&tg::Checksum>,
	options: Option<DownloadOptions>,
) -> tg::Result<tg::Either<tg::Blob, tg::Artifact>>
where
	I: tg::Instance,
{
	let checksum = checksum.cloned().unwrap_or_default();
	let mut options = options.unwrap_or_default();
	options.checksum.get_or_insert(checksum.algorithm());
	let mode = options.mode.unwrap_or_default();
	let mut args = vec![tg::Value::from("builtin"), "download".into()];
	if let Some(algorithm) = options.checksum {
		args.extend(["--checksum".into(), algorithm.to_string().into()]);
	}
	args.extend([
		"--mode".into(),
		mode.to_string().into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
		url.to_string().into(),
	]);
	let arg = tg::process::Arg {
		args,
		checksum: Some(checksum),
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("download".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let output = match mode {
		tg::DownloadMode::Raw => {
			let file: tg::File = output.try_into()?;
			let blob = file.contents_with_instance(instance).await?;
			tg::Either::Left(blob)
		},
		tg::DownloadMode::Decompress | tg::DownloadMode::Extract => {
			tg::Either::Right(output.try_into()?)
		},
	};

	Ok(output)
}

#[must_use]
pub fn download_command(url: &Uri, options: Option<DownloadOptions>) -> tg::Command {
	let options = options.unwrap_or_default();
	let mut args: Vec<tg::command::Value> = vec!["builtin".into(), "download".into()];
	if let Some(algorithm) = options.checksum {
		args.extend(["--checksum".into(), algorithm.to_string().into()]);
	}
	args.extend([
		"--mode".into(),
		options.mode.unwrap_or_default().to_string().into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
		url.to_string().into(),
	]);
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

pub async fn extract(input: &tg::Blob) -> tg::Result<tg::Artifact> {
	let instance = tg::instance()?;
	extract_with_instance(instance, input).boxed_local().await
}

pub async fn extract_with_instance<I>(instance: &I, input: &tg::Blob) -> tg::Result<tg::Artifact>
where
	I: tg::Instance,
{
	let input = tg::File::with_contents(input.clone());
	let args = vec![
		tg::Value::from("builtin"),
		"extract".into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let arg = tg::process::Arg {
		args,
		executable: Some(tg::command::Executable {
			artifact: None,
			path: Some("tg".into()),
		}),
		host: Some(tg::host::current().to_owned()),
		name: Some("extract".into()),
		..Default::default()
	};
	let output = tg::process::build_with_instance(instance, arg).await?;
	let artifact = output.try_into()?;
	Ok(artifact)
}

#[must_use]
pub fn extract_command(input: &tg::Blob) -> tg::Command {
	let input = tg::File::with_contents(input.clone());
	let args: Vec<tg::command::Value> = vec![
		"builtin".into(),
		"extract".into(),
		"--input".into(),
		input.into(),
		"--output".into(),
		tg::Placeholder::new("output").into(),
	];
	let executable = tg::command::Executable {
		artifact: None,
		path: Some("tg".into()),
	};
	let host = tg::host::current();
	tg::Command::builder()
		.host(host)
		.executable(executable)
		.args(args)
		.finish()
		.expect("the command builder should be complete")
}

async fn remove_dependencies<I>(
	instance: &I,
	artifact: &tg::Artifact,
	depth: usize,
) -> tg::Result<tg::Artifact>
where
	I: tg::Instance,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entries = Box::pin(async move {
				directory
					.entries_with_instance(instance)
					.await?
					.iter()
					.map(|(name, artifact)| async move {
						let artifact = remove_dependencies(instance, artifact, depth + 1).await?;
						Ok::<_, tg::Error>((name.clone(), artifact))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await
			})
			.await?;
			let directory = tg::Directory::with_entries(entries);
			Ok(directory.into())
		},
		tg::Artifact::File(file) => {
			let contents = file.contents_with_instance(instance).await?;
			let executable = file.executable_with_instance(instance).await?;
			let file = tg::File::builder()
				.contents(contents)
				.executable(executable)
				.build()?;
			Ok(file.into())
		},
		tg::Artifact::Symlink(symlink) => {
			let artifact = symlink.artifact_with_instance(instance).await?;
			let path = symlink.path_with_instance(instance).await?;
			let mut target = PathBuf::new();
			if let Some(artifact) = artifact {
				for _ in 0..depth.saturating_sub(1) {
					target.push("..");
				}
				target.push(TANGRAM_STORE_PATH);
				target.push(artifact.id().to_string());
			}
			if let Some(path) = path {
				target.push(path);
			}
			if target == Path::new("") {
				return Err(tg::error!("invalid symlink"));
			}
			Ok(tg::Symlink::with_path(target).into())
		},
	}
}
