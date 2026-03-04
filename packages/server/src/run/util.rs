use {
	crate::Server,
	futures::{TryFutureExt as _, TryStreamExt as _, future},
	std::{collections::BTreeMap, path::Path, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio::io::{AsyncRead, AsyncWrite},
	tokio_util::io::ReaderStream,
};

pub async fn cache_children(server: &Server, process: &tg::Process) -> tg::Result<()> {
	// Do nothing if the VFS is enabled.
	if server.vfs.lock().unwrap().is_some() {
		return Ok(());
	}

	// Get the process's command's children that are artifacts.
	let artifacts: Vec<tg::artifact::Id> = process
		.command(server)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the command"))?
		.children(server)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the command's children"))?
		.into_iter()
		.filter_map(|object| object.id().try_into().ok())
		.collect::<Vec<_>>();

	// Check out the artifacts.
	let arg = tg::cache::Arg { artifacts };
	let stream = server
		.cache(arg)
		.await
		.map_err(|source| tg::error!(!source, "failed to cache the artifacts"))?;

	// Log the progress stream.
	server
		.log_progress_stream(process, stream)
		.await
		.map_err(|source| tg::error!(!source, "failed to log the progress stream"))?;

	Ok(())
}

pub fn render_args_string(
	args: &[tg::value::Data],
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|value| render_value_string(value, artifacts_path, output_path))
		.collect::<tg::Result<Vec<_>>>()
}

pub fn render_args_dash_a(args: &[tg::value::Data]) -> Vec<String> {
	args.iter()
		.flat_map(|value| {
			let value = tg::Value::try_from_data(value.clone()).unwrap().to_string();
			["-A".to_owned(), value]
		})
		.collect::<Vec<_>>()
}

pub fn render_env(
	env: &tg::value::data::Map,
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>> {
	let mut output = BTreeMap::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut output, key)?;
	}
	let output = output
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value_string(value, artifacts_path, output_path)?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

pub fn render_value_string(
	value: &tg::value::Data,
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into()),
			tg::template::data::Component::Placeholder(placeholder) => {
				if placeholder.name == "output" {
					Ok(output_path.to_str().unwrap().to_owned().into())
				} else {
					Err(tg::error!(
						name = %placeholder.name,
						"invalid placeholder"
					))
				}
			},
		}),
		tg::value::Data::Placeholder(placeholder) => {
			if placeholder.name == "output" {
				Ok(output_path.to_str().unwrap().to_owned())
			} else {
				Err(tg::error!(
					name = %placeholder.name,
					"invalid placeholder"
				))
			}
		},
		_ => Ok(tg::Value::try_from_data(value.clone()).unwrap().to_string()),
	}
}

pub async fn stdio_task<I, O, E>(
	server: &Server,
	id: &tg::process::Id,
	remote: Option<&String>,
	stdin_blob: Option<tg::Blob>,
	stdin: Option<I>,
	stdout: Option<O>,
	stderr: Option<E>,
) -> tg::Result<()>
where
	I: AsyncWrite + Unpin + Send + 'static,
	O: AsyncRead + Unpin + Send + 'static,
	E: AsyncRead + Unpin + Send + 'static,
{
	// Write the stdin blob to stdin if necessary.
	let stdin = Task::spawn({
		let server = server.clone();
		|_| {
			async move {
				let Some(mut stdin) = stdin else {
					return Ok(());
				};
				let Some(blob) = stdin_blob else {
					return Ok(());
				};
				let mut reader = blob.read(&server, tg::read::Options::default()).await?;
				tokio::io::copy(&mut reader, &mut stdin)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the blob to stdin"))?;
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| {
				tracing::error!(error = %error.trace());
			})
		}
	});

	let stdout = Task::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		|_| async move {
			let Some(stdout) = stdout else {
				return Ok(());
			};
			stdio_task_inner(
				&server,
				&id,
				remote.as_ref(),
				tg::process::log::Stream::Stdout,
				stdout,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	let stderr = Task::spawn({
		let server = server.clone();
		let id = id.clone();
		let remote = remote.cloned();
		|_| async move {
			let Some(stderr) = stderr else {
				return Ok(());
			};
			stdio_task_inner(
				&server,
				&id,
				remote.as_ref(),
				tg::process::log::Stream::Stderr,
				stderr,
			)
			.await?;
			Ok::<_, tg::Error>(())
		}
	});

	// Join the tasks.
	let (stdout, stderr) = future::join(stdout.wait(), stderr.wait()).await;
	stdout
		.map_err(|source| tg::error!(!source, "the stdout task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to read stdout from pipe"))?;
	stderr
		.map_err(|source| tg::error!(!source, "the stderr task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to read stderr from pipe"))?;

	// Abort the stdin task.
	stdin.abort();

	Ok::<_, tg::Error>(())
}

pub async fn stdio_task_inner(
	server: &Server,
	id: &tg::process::Id,
	remote: Option<&String>,
	stream: tg::process::log::Stream,
	reader: impl AsyncRead + Unpin + Send + 'static,
) -> tg::Result<()> {
	let stream_ = ReaderStream::new(reader)
		.map_err(|source| tg::error!(!source, "failed to read from the reader"));
	let mut stream_ = pin!(stream_);
	while let Some(bytes) = stream_
		.try_next()
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to read from the stream"))?
	{
		let arg = tg::process::log::post::Arg {
			bytes,
			local: None,
			remotes: remote.cloned().map(|r| vec![r]),
			stream,
		};
		server
			.post_process_log(id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to post the process log"))?;
	}
	Ok(())
}

pub async fn signal_task(
	server: &Server,
	sandbox: &tangram_sandbox::Sandbox,
	sandbox_process: &tangram_sandbox::Process,
	id: &tg::process::Id,
	remote: Option<&String>,
) -> tg::Result<()> {
	// Get the signal stream for the process.
	let arg = tg::process::signal::get::Arg {
		local: None,
		remotes: remote.map(|r| vec![r.clone()]),
	};
	let mut stream = server
		.try_get_process_signal_stream(id, arg)
		.await
		.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the process's signal stream"),
		)?
		.ok_or_else(
			|| tg::error!(process = %id, "expected the process's signal stream to exist"),
		)?;

	// Handle the events.
	while let Some(event) = stream.try_next().await.map_err(
		|source| tg::error!(!source, process = %id, "failed to get the next signal event"),
	)? {
		match event {
			tg::process::signal::get::Event::Signal(signal) => {
				sandbox
					.kill(sandbox_process, signal)
					.await
					.map_err(|source| tg::error!(!source, "failed to signal the process"))?;
			},
			tg::process::signal::get::Event::End => break,
		}
	}

	Ok(())
}
