use {
	crate::Server,
	bytes::Bytes,
	futures::{future, stream},
	std::{
		collections::BTreeMap,
		path::{Path, PathBuf},
	},
	tangram_client as tg,
};

pub(super) async fn cache_children(server: &Server, process: &tg::Process) -> tg::Result<()> {
	// Do nothing if the VFS is enabled.
	if server.vfs.lock().unwrap().is_some() {
		return Ok(());
	}

	// Get the process's command's children that are artifacts.
	let artifacts: Vec<tg::artifact::Id> = process
		.command(server)
		.await?
		.children(server)
		.await?
		.into_iter()
		.filter_map(|object| object.id().try_into().ok())
		.collect::<Vec<_>>();

	// Check out the artifacts.
	let arg = tg::cache::Arg { artifacts };
	let stream = server.cache(arg).await?;

	// Log the progress stream.
	server.log_progress_stream(process, stream).await?;

	Ok(())
}

pub(crate) async fn log(
	server: &Server,
	process: &tg::Process,
	stream: tg::process::log::Stream,
	message: String,
) -> tg::Result<()> {
	let state = process.load(server).await?;
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
			remote: process.remote().cloned(),
			stream,
		};
		server.post_process_log(process.id(), arg).await?;
	}
	Ok(())
}

async fn log_inner(
	server: &Server,
	stdio: &tg::process::Stdio,
	message: String,
	remote: Option<&String>,
) -> tg::Result<()> {
	match stdio {
		tg::process::Stdio::Pipe(id) => {
			let bytes = Bytes::from(message);
			let stream = stream::once(future::ok(tg::pipe::Event::Chunk(bytes)));
			let arg = tg::pipe::write::Arg {
				remote: remote.cloned(),
			};
			server.write_pipe(id, arg, Box::new(stream)).await?;
		},
		tg::process::Stdio::Pty(id) => {
			let bytes = Bytes::from(message.replace('\n', "\r\n"));
			let stream = stream::once(future::ok(tg::pty::Event::Chunk(bytes)));
			let arg = tg::pty::write::Arg {
				master: false,
				remote: remote.cloned(),
			};
			server.write_pty(id, arg, Box::new(stream)).await?;
		},
	}
	Ok(())
}

/// Render a value.
pub fn render_value(artifacts_path: &Path, value: &tg::value::Data) -> String {
	if let Ok(string) = value.try_unwrap_string_ref() {
		return string.clone();
	}
	if let Ok(object) = value.try_unwrap_object_ref()
		&& let Ok(artifact) = tg::artifact::Id::try_from(object.clone())
	{
		let string = artifacts_path
			.join(artifact.to_string())
			.to_str()
			.unwrap()
			.to_owned();
		return string;
	}
	if let Ok(template) = value.try_unwrap_template_ref() {
		let string = template.render(|component| match component {
			tg::template::data::Component::String(string) => string.clone().into(),
			tg::template::data::Component::Artifact(artifact) => artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into(),
		});
		return string;
	}
	"<tangram value>".to_owned()
}

pub fn render_env(
	artifacts_path: &Path,
	env: &tg::value::data::Map,
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
			let value = render_value(artifacts_path, value);
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

pub async fn which(exe: &Path, env: &BTreeMap<String, String>) -> tg::Result<PathBuf> {
	if exe.is_absolute() || exe.components().count() > 1 {
		return Ok(exe.to_owned());
	}
	let Some(pathenv) = env.get("PATH") else {
		return Ok(exe.to_owned());
	};
	let name = exe.components().next();
	let Some(std::path::Component::Normal(name)) = name else {
		return Err(tg::error!(%path = exe.display(), "invalid executable path"));
	};
	let sep = ":";
	for path in pathenv.split(sep) {
		let path = Path::new(path).join(name);
		if tokio::fs::try_exists(&path).await.ok() == Some(true) {
			return Ok(path);
		}
	}
	Err(tg::error!(%path = exe.display(), "failed to find the executable"))
}

#[cfg(target_os = "linux")]
pub fn whoami() -> tg::Result<String> {
	unsafe {
		let uid = libc::getuid();
		let pwd = libc::getpwuid(uid);
		if pwd.is_null() {
			let source = std::io::Error::last_os_error();
			return Err(tg::error!(!source, "failed to get username"));
		}
		let username = std::ffi::CString::from_raw((*pwd).pw_name)
			.to_str()
			.map_err(|source| tg::error!(!source, "non-utf8 username"))?
			.to_owned();
		Ok(username)
	}
}
