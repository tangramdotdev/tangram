use super::{FromV8, Serde, ToV8};
use tangram_client as tg;

impl ToV8 for tg::process::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::process::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::process::Io {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::process::Io {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::Process {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let process = v8::String::new_external_onebyte_static(scope, "Process".as_bytes()).unwrap();
		let process = tangram.get(scope, process.into()).unwrap();
		let process = v8::Local::<v8::Function>::try_from(process).unwrap();

		let arg = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "id".as_bytes()).unwrap();
		let value = self.id().to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "remote".as_bytes()).unwrap();
		let value = self.remote().to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let value = self.state().read().unwrap().to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let instance = process
			.new_instance(scope, &[arg.into()])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Process {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let process = v8::String::new_external_onebyte_static(scope, "Process".as_bytes()).unwrap();
		let process = tangram.get(scope, process.into()).unwrap();
		let process = v8::Local::<v8::Function>::try_from(process).unwrap();

		if !value.instance_of(scope, process.into()).unwrap() {
			return Err(tg::error!("expected a process"));
		}
		let value = value.to_object(scope).unwrap();

		let id = v8::String::new_external_onebyte_static(scope, "id".as_bytes()).unwrap();
		let id = value.get(scope, id.into()).unwrap();
		let id = <_>::from_v8(scope, id)
			.map_err(|source| tg::error!(!source, "failed to deserialize the id"))?;

		let remote = v8::String::new_external_onebyte_static(scope, "remote".as_bytes()).unwrap();
		let remote = value.get(scope, remote.into()).unwrap();
		let remote = <_>::from_v8(scope, remote)
			.map_err(|source| tg::error!(!source, "failed to deserialize the remote"))?;

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::new(id, remote, state, None, None))
	}
}

impl ToV8 for tg::process::State {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "checksum".as_bytes()).unwrap();
		let value = self.checksum.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "command".as_bytes()).unwrap();
		let value = self.command.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "created_at".as_bytes()).unwrap();
		let value = self.created_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "cwd".as_bytes()).unwrap();
		let value = self.cwd.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "dequeued_at".as_bytes()).unwrap();
		let value = self.dequeued_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "enqueued_at".as_bytes()).unwrap();
		let value = self.enqueued_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "env".as_bytes()).unwrap();
		let value = self.env.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "error".as_bytes()).unwrap();
		let value = self.error.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "exit".as_bytes()).unwrap();
		let value = self.exit.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "finished_at".as_bytes()).unwrap();
		let value = self.finished_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "log".as_bytes()).unwrap();
		let value = self.log.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let value = self.network.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let value = self.output.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "retry".as_bytes()).unwrap();
		let value = self.retry.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "started_at".as_bytes()).unwrap();
		let value = self.started_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "status".as_bytes()).unwrap();
		let value = self.status.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "stderr".as_bytes()).unwrap();
		let value = self.stderr.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "stdin".as_bytes()).unwrap();
		let value = self.stdin.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "stdout".as_bytes()).unwrap();
		let value = self.stdout.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::mount::Source {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tangram_client::Result<Self> {
		let value = String::from_v8(scope, value)
			.map_err(|source| tg::error!(!source, "failed to read the source string"))?;
		if let Ok(id) = value.parse::<tg::artifact::Id>() {
			return Ok(Self::Artifact(tg::Artifact::with_id(id)));
		}
		let Ok(path_buf) = value.parse::<std::path::PathBuf>();
		Ok(Self::Path(path_buf))
	}
}

impl ToV8 for tg::process::mount::Source {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			tg::process::mount::Source::Artifact(artifact) => artifact.to_v8(scope),
			tg::process::mount::Source::Path(path_buf) => path_buf.to_v8(scope),
		}
	}
}

impl FromV8 for tg::process::Mount {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tangram_client::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let source = v8::String::new_external_onebyte_static(scope, "source".as_bytes()).unwrap();
		let source = value.get(scope, source.into()).unwrap();
		let source = <_>::from_v8(scope, source)
			.map_err(|source| tg::error!(!source, "failed to deserialize the source field"))?;

		let target = v8::String::new_external_onebyte_static(scope, "target".as_bytes()).unwrap();
		let target = value.get(scope, target.into()).unwrap();
		let target = <_>::from_v8(scope, target)
			.map_err(|source| tg::error!(!source, "failed to deserialize the target field"))?;

		let readonly =
			v8::String::new_external_onebyte_static(scope, "readonly".as_bytes()).unwrap();
		let readonly = value.get(scope, readonly.into()).unwrap();
		let readonly = <_>::from_v8(scope, readonly)
			.map_err(|source| tg::error!(!source, "failed to deserialize the readonly field"))?;

		Ok(tg::process::Mount {
			source,
			target,
			readonly,
		})
	}
}

impl ToV8 for tg::process::Mount {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "source".as_bytes()).unwrap();
		let value = self.source.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "target".as_bytes()).unwrap();
		let value = self.target.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "readonly".as_bytes()).unwrap();
		let value = self.readonly.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::State {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let cacheable =
			v8::String::new_external_onebyte_static(scope, "cacheable".as_bytes()).unwrap();
		let cacheable = value.get(scope, cacheable.into()).unwrap();
		let cacheable = <_>::from_v8(scope, cacheable)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cacheable field"))?;

		let checksum =
			v8::String::new_external_onebyte_static(scope, "checksum".as_bytes()).unwrap();
		let checksum = value.get(scope, checksum.into()).unwrap();
		let checksum = <_>::from_v8(scope, checksum)
			.map_err(|source| tg::error!(!source, "failed to deserialize the checksum field"))?;

		let children =
			v8::String::new_external_onebyte_static(scope, "children".as_bytes()).unwrap();
		let children = value.get(scope, children.into()).unwrap();
		let children = <_>::from_v8(scope, children)
			.map_err(|source| tg::error!(!source, "failed to deserialize the children field"))?;

		let command = v8::String::new_external_onebyte_static(scope, "command".as_bytes()).unwrap();
		let command = value.get(scope, command.into()).unwrap();
		let command = <_>::from_v8(scope, command)
			.map_err(|source| tg::error!(!source, "failed to deserialize the command field"))?;

		let created_at =
			v8::String::new_external_onebyte_static(scope, "created_at".as_bytes()).unwrap();
		let created_at = value.get(scope, created_at.into()).unwrap();
		let created_at = <_>::from_v8(scope, created_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the created_at field"))?;

		let cwd = v8::String::new_external_onebyte_static(scope, "cwd".as_bytes()).unwrap();
		let cwd = value.get(scope, cwd.into()).unwrap();
		let cwd = <_>::from_v8(scope, cwd)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cwd field"))?;

		let dequeued_at =
			v8::String::new_external_onebyte_static(scope, "dequeued_at".as_bytes()).unwrap();
		let dequeued_at = value.get(scope, dequeued_at.into()).unwrap();
		let dequeued_at = <_>::from_v8(scope, dequeued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the dequeued_at field"))?;

		let enqueued_at =
			v8::String::new_external_onebyte_static(scope, "enqueued_at".as_bytes()).unwrap();
		let enqueued_at = value.get(scope, enqueued_at.into()).unwrap();
		let enqueued_at = <_>::from_v8(scope, enqueued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the enqueued_at field"))?;

		let env = v8::String::new_external_onebyte_static(scope, "env".as_bytes()).unwrap();
		let env = value.get(scope, env.into()).unwrap();
		let env = <_>::from_v8(scope, env)
			.map_err(|source| tg::error!(!source, "failed to deserialize the env field"))?;

		let error = v8::String::new_external_onebyte_static(scope, "error".as_bytes()).unwrap();
		let error = value.get(scope, error.into()).unwrap();
		let error = <_>::from_v8(scope, error)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error field"))?;

		let exit = v8::String::new_external_onebyte_static(scope, "exit".as_bytes()).unwrap();
		let exit = value.get(scope, exit.into()).unwrap();
		let exit = <_>::from_v8(scope, exit)
			.map_err(|source| tg::error!(!source, "failed to deserialize the exit field"))?;

		let finished_at =
			v8::String::new_external_onebyte_static(scope, "finished_at".as_bytes()).unwrap();
		let finished_at = value.get(scope, finished_at.into()).unwrap();
		let finished_at = <_>::from_v8(scope, finished_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the finished_at field"))?;

		let log = v8::String::new_external_onebyte_static(scope, "log".as_bytes()).unwrap();
		let log = value.get(scope, log.into()).unwrap();
		let log = <_>::from_v8(scope, log)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log field"))?;

		let mounts = v8::String::new_external_onebyte_static(scope, "mounts".as_bytes()).unwrap();
		let mounts = value.get(scope, mounts.into()).unwrap();
		let mounts = <_>::from_v8(scope, mounts)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log field"))?;

		let network = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let network = value.get(scope, network.into()).unwrap();
		let network = <_>::from_v8(scope, network)
			.map_err(|source| tg::error!(!source, "failed to deserialize the network field"))?;

		let output = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let output = value.get(scope, output.into()).unwrap();
		let output = <_>::from_v8(scope, output)
			.map_err(|source| tg::error!(!source, "failed to deserialize the output field"))?;

		let retry = v8::String::new_external_onebyte_static(scope, "retry".as_bytes()).unwrap();
		let retry = value.get(scope, retry.into()).unwrap();
		let retry = <_>::from_v8(scope, retry)
			.map_err(|source| tg::error!(!source, "failed to deserialize the retry field"))?;

		let started_at =
			v8::String::new_external_onebyte_static(scope, "started_at".as_bytes()).unwrap();
		let started_at = value.get(scope, started_at.into()).unwrap();
		let started_at = <_>::from_v8(scope, started_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the started_at field"))?;

		let status = v8::String::new_external_onebyte_static(scope, "status".as_bytes()).unwrap();
		let status = value.get(scope, status.into()).unwrap();
		let status = <_>::from_v8(scope, status)
			.map_err(|source| tg::error!(!source, "failed to deserialize the status field"))?;

		let stderr = v8::String::new_external_onebyte_static(scope, "stderr".as_bytes()).unwrap();
		let stderr = value.get(scope, stderr.into()).unwrap();
		let stderr = <_>::from_v8(scope, stderr)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stderr"))?;

		let stdin = v8::String::new_external_onebyte_static(scope, "stdin".as_bytes()).unwrap();
		let stdin = value.get(scope, stdin.into()).unwrap();
		let stdin = <_>::from_v8(scope, stdin)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stdin"))?;

		let stdout = v8::String::new_external_onebyte_static(scope, "stdout".as_bytes()).unwrap();
		let stdout = value.get(scope, stdout.into()).unwrap();
		let stdout = <_>::from_v8(scope, stdout)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stdout"))?;

		Ok(Self {
			cacheable,
			checksum,
			children,
			command,
			created_at,
			cwd,
			dequeued_at,
			enqueued_at,
			env,
			error,
			exit,
			finished_at,
			log,
			mounts,
			network,
			output,
			retry,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
		})
	}
}

impl ToV8 for tg::process::spawn::Arg {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde::new(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::process::spawn::Arg {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.into_inner();
		Ok(value)
	}
}

impl ToV8 for tg::process::spawn::Output {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde::new(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::process::spawn::Output {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.into_inner();
		Ok(value)
	}
}

impl ToV8 for tg::process::Status {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::process::Status {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::process::Wait {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "error".as_bytes()).unwrap();
		let value = self.error.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "exit".as_bytes()).unwrap();
		let value = self.exit.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let value = self.output.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "status".as_bytes()).unwrap();
		let value = self.status.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::Wait {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, "error".as_bytes()).unwrap();
		let error = value.get(scope, error.into()).unwrap();
		let error = <_>::from_v8(scope, error)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;

		let exit = v8::String::new_external_onebyte_static(scope, "exit".as_bytes()).unwrap();
		let exit = value.get(scope, exit.into()).unwrap();
		let exit = <_>::from_v8(scope, exit)
			.map_err(|source| tg::error!(!source, "failed to deserialize the exit"))?;

		let output = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let output = value.get(scope, output.into()).unwrap();
		let output = <_>::from_v8(scope, output)
			.map_err(|source| tg::error!(!source, "failed to deserialize the output"))?;

		let status = v8::String::new_external_onebyte_static(scope, "status".as_bytes()).unwrap();
		let status = value.get(scope, status.into()).unwrap();
		let status = <_>::from_v8(scope, status)
			.map_err(|source| tg::error!(!source, "failed to deserialize the status"))?;

		Ok(Self {
			error,
			exit,
			output,
			status,
		})
	}
}

impl ToV8 for tg::process::Exit {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde::new(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::process::Exit {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.into_inner();
		Ok(value)
	}
}
