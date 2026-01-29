use {
	crate::Server,
	bytes::Bytes,
	std::{collections::BTreeMap, path::Path},
	tangram_client::prelude::*,
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

pub async fn log(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	message: Vec<u8>,
) -> tg::Result<()> {
	if message.is_empty() {
		return Ok(());
	}
	let state = process
		.load(server)
		.await
		.map_err(|source| tg::error!(!source, "failed to load the process"))?;
	let stdout = state.stdout.as_ref();
	let stderr = state.stderr.as_ref();
	if let (tg::process::log::Stream::Stdout, Some(stdout)) = (stream, stdout) {
		log_inner(server, stdout, message, process.remote())
			.await
			.ok();
	} else if let (tg::process::log::Stream::Stderr, Some(stderr)) = (stream, stderr) {
		log_inner(server, stderr, message, process.remote())
			.await
			.ok();
	} else {
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			local: None,
			remotes: process.remote().cloned().map(|r| vec![r]),
			stream,
		};
		server
			.post_process_log(process.id(), arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post the process log"))?;
	}
	Ok(())
}

async fn log_inner(
	server: &Server,
	stdio: &tg::process::Stdio,
	message: Vec<u8>,
	remote: Option<&String>,
) -> tg::Result<()> {
	match stdio {
		tg::process::Stdio::Pipe(id) => {
			let bytes = Bytes::from(message);
			let arg = tg::pipe::write::Arg {
				bytes,
				local: None,
				remotes: remote.cloned().map(|r| vec![r]),
			};
			server
				.write_pipe(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the pipe"))?;
		},
		tg::process::Stdio::Pty(id) => {
			let bytes = if let Ok(string) = std::str::from_utf8(&message) {
				Bytes::from(string.replace('\n', "\r\n"))
			} else {
				Bytes::from(message)
			};
			let arg = tg::pty::write::Arg {
				bytes,
				local: None,
				master: false,
				remotes: remote.cloned().map(|r| vec![r]),
			};
			server
				.write_pty(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to the pty"))?;
		},
	}
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

pub fn whoami() -> tg::Result<String> {
	unsafe {
		let uid = libc::getuid();
		let pwd = libc::getpwuid(uid);
		if pwd.is_null() {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to get username"));
		}
		let username = std::ffi::CStr::from_ptr((*pwd).pw_name)
			.to_str()
			.map_err(|source| tg::error!(!source, "non-utf8 username"))?
			.to_owned();
		Ok(username)
	}
}
