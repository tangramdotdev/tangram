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

impl ToV8 for tg::process::Stdio {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::process::Stdio {
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
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let process = v8::String::new_external_onebyte_static(scope, b"Process").unwrap();
		let process = tangram.get(scope, process.into()).unwrap();
		let process = v8::Local::<v8::Function>::try_from(process).unwrap();

		let arg = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"id").unwrap();
		let value = self.id().to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"remote").unwrap();
		let value = self.remote().to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"state").unwrap();
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
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let process = v8::String::new_external_onebyte_static(scope, b"Process").unwrap();
		let process = tangram.get(scope, process.into()).unwrap();
		let process = v8::Local::<v8::Function>::try_from(process).unwrap();

		if !value.instance_of(scope, process.into()).unwrap() {
			return Err(tg::error!("expected a process"));
		}
		let value = value.to_object(scope).unwrap();

		let id = v8::String::new_external_onebyte_static(scope, b"id").unwrap();
		let id = value.get(scope, id.into()).unwrap();
		let id = <_>::from_v8(scope, id)
			.map_err(|source| tg::error!(!source, "failed to deserialize the id"))?;

		let remote = v8::String::new_external_onebyte_static(scope, b"remote").unwrap();
		let remote = value.get(scope, remote.into()).unwrap();
		let remote = <_>::from_v8(scope, remote)
			.map_err(|source| tg::error!(!source, "failed to deserialize the remote"))?;

		let state = v8::String::new_external_onebyte_static(scope, b"state").unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::new(id, remote, state, None, None))
	}
}

impl ToV8 for tg::process::State {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"actual_checksum").unwrap();
		let value = self.actual_checksum.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"command").unwrap();
		let value = self.command.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"created_at").unwrap();
		let value = self.created_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"dequeued_at").unwrap();
		let value = self.dequeued_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"enqueued_at").unwrap();
		let value = self.enqueued_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"error").unwrap();
		let value = self.error.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"exit").unwrap();
		let value = self.exit.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"expected_checksum").unwrap();
		let value = self.expected_checksum.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"finished_at").unwrap();
		let value = self.finished_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"log").unwrap();
		let value = self.log.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"mounts").unwrap();
		let value = self.mounts.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"network").unwrap();
		let value = self.network.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"output").unwrap();
		let value = self.output.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"retry").unwrap();
		let value = self.retry.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"started_at").unwrap();
		let value = self.started_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"status").unwrap();
		let value = self.status.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"stderr").unwrap();
		let value = self.stderr.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"stdin").unwrap();
		let value = self.stdin.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"stdout").unwrap();
		let value = self.stdout.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::Mount {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let source = v8::String::new_external_onebyte_static(scope, b"source").unwrap();
		let source = value.get(scope, source.into()).unwrap();
		let source = <_>::from_v8(scope, source)
			.map_err(|source| tg::error!(!source, "failed to deserialize the source field"))?;

		let target = v8::String::new_external_onebyte_static(scope, b"target").unwrap();
		let target = value.get(scope, target.into()).unwrap();
		let target = <_>::from_v8(scope, target)
			.map_err(|source| tg::error!(!source, "failed to deserialize the target field"))?;

		let readonly = v8::String::new_external_onebyte_static(scope, b"readonly").unwrap();
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

		let key = v8::String::new_external_onebyte_static(scope, b"source").unwrap();
		let value = self.source.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"target").unwrap();
		let value = self.target.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"readonly").unwrap();
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
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let actual_checksum =
			v8::String::new_external_onebyte_static(scope, b"actual_checksum").unwrap();
		let actual_checksum = value.get(scope, actual_checksum.into()).unwrap();
		let actual_checksum = <_>::from_v8(scope, actual_checksum).map_err(|source| {
			tg::error!(!source, "failed to deserialize the actual_checksum field")
		})?;

		let cacheable = v8::String::new_external_onebyte_static(scope, b"cacheable").unwrap();
		let cacheable = value.get(scope, cacheable.into()).unwrap();
		let cacheable = <_>::from_v8(scope, cacheable)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cacheable field"))?;

		let children = v8::String::new_external_onebyte_static(scope, b"children").unwrap();
		let children = value.get(scope, children.into()).unwrap();
		let children = <_>::from_v8(scope, children)
			.map_err(|source| tg::error!(!source, "failed to deserialize the children field"))?;

		let command = v8::String::new_external_onebyte_static(scope, b"command").unwrap();
		let command = value.get(scope, command.into()).unwrap();
		let command = <_>::from_v8(scope, command)
			.map_err(|source| tg::error!(!source, "failed to deserialize the command field"))?;

		let created_at = v8::String::new_external_onebyte_static(scope, b"created_at").unwrap();
		let created_at = value.get(scope, created_at.into()).unwrap();
		let created_at = <_>::from_v8(scope, created_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the created_at field"))?;

		let dequeued_at = v8::String::new_external_onebyte_static(scope, b"dequeued_at").unwrap();
		let dequeued_at = value.get(scope, dequeued_at.into()).unwrap();
		let dequeued_at = <_>::from_v8(scope, dequeued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the dequeued_at field"))?;

		let enqueued_at = v8::String::new_external_onebyte_static(scope, b"enqueued_at").unwrap();
		let enqueued_at = value.get(scope, enqueued_at.into()).unwrap();
		let enqueued_at = <_>::from_v8(scope, enqueued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the enqueued_at field"))?;

		let error = v8::String::new_external_onebyte_static(scope, b"error").unwrap();
		let error = value.get(scope, error.into()).unwrap();
		let error = <_>::from_v8(scope, error)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error field"))?;

		let exit = v8::String::new_external_onebyte_static(scope, b"exit").unwrap();
		let exit = value.get(scope, exit.into()).unwrap();
		let exit = <_>::from_v8(scope, exit)
			.map_err(|source| tg::error!(!source, "failed to deserialize the exit field"))?;

		let expected_checksum =
			v8::String::new_external_onebyte_static(scope, b"expected_checksum").unwrap();
		let expected_checksum = value.get(scope, expected_checksum.into()).unwrap();
		let expected_checksum = <_>::from_v8(scope, expected_checksum).map_err(|source| {
			tg::error!(!source, "failed to deserialize the expected_checksum field")
		})?;

		let finished_at = v8::String::new_external_onebyte_static(scope, b"finished_at").unwrap();
		let finished_at = value.get(scope, finished_at.into()).unwrap();
		let finished_at = <_>::from_v8(scope, finished_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the finished_at field"))?;

		let log = v8::String::new_external_onebyte_static(scope, b"log").unwrap();
		let log = value.get(scope, log.into()).unwrap();
		let log = <_>::from_v8(scope, log)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log field"))?;

		let mounts = v8::String::new_external_onebyte_static(scope, b"mounts").unwrap();
		let mounts = value.get(scope, mounts.into()).unwrap();
		let mounts = <_>::from_v8(scope, mounts)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log field"))?;

		let network = v8::String::new_external_onebyte_static(scope, b"network").unwrap();
		let network = value.get(scope, network.into()).unwrap();
		let network = <_>::from_v8(scope, network)
			.map_err(|source| tg::error!(!source, "failed to deserialize the network field"))?;

		let output = v8::String::new_external_onebyte_static(scope, b"output").unwrap();
		let output = value.get(scope, output.into()).unwrap();
		let output = <_>::from_v8(scope, output)
			.map_err(|source| tg::error!(!source, "failed to deserialize the output field"))?;

		let retry = v8::String::new_external_onebyte_static(scope, b"retry").unwrap();
		let retry = value.get(scope, retry.into()).unwrap();
		let retry = <_>::from_v8(scope, retry)
			.map_err(|source| tg::error!(!source, "failed to deserialize the retry field"))?;

		let started_at = v8::String::new_external_onebyte_static(scope, b"started_at").unwrap();
		let started_at = value.get(scope, started_at.into()).unwrap();
		let started_at = <_>::from_v8(scope, started_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the started_at field"))?;

		let status = v8::String::new_external_onebyte_static(scope, b"status").unwrap();
		let status = value.get(scope, status.into()).unwrap();
		let status = <_>::from_v8(scope, status)
			.map_err(|source| tg::error!(!source, "failed to deserialize the status field"))?;

		let stderr = v8::String::new_external_onebyte_static(scope, b"stderr").unwrap();
		let stderr = value.get(scope, stderr.into()).unwrap();
		let stderr = <_>::from_v8(scope, stderr)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stderr"))?;

		let stdin = v8::String::new_external_onebyte_static(scope, b"stdin").unwrap();
		let stdin = value.get(scope, stdin.into()).unwrap();
		let stdin = <_>::from_v8(scope, stdin)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stdin"))?;

		let stdout = v8::String::new_external_onebyte_static(scope, b"stdout").unwrap();
		let stdout = value.get(scope, stdout.into()).unwrap();
		let stdout = <_>::from_v8(scope, stdout)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stdout"))?;

		Ok(Self {
			actual_checksum,
			cacheable,
			children,
			command,
			created_at,
			dequeued_at,
			enqueued_at,
			error,
			exit,
			expected_checksum,
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
		let value = Serde(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::process::spawn::Arg {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.0;
		Ok(value)
	}
}

impl ToV8 for tg::process::spawn::Output {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::process::spawn::Output {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.0;
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

		let key = v8::String::new_external_onebyte_static(scope, b"error").unwrap();
		let value = self.error.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"exit").unwrap();
		let value = self.exit.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"output").unwrap();
		let value = self.output.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::Wait {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, b"error").unwrap();
		let error = value.get(scope, error.into()).unwrap();
		let error = <_>::from_v8(scope, error)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;

		let exit = v8::String::new_external_onebyte_static(scope, b"exit").unwrap();
		let exit = value.get(scope, exit.into()).unwrap();
		let exit = <_>::from_v8(scope, exit)
			.map_err(|source| tg::error!(!source, "failed to deserialize the exit"))?;

		let output = v8::String::new_external_onebyte_static(scope, b"output").unwrap();
		let output = value.get(scope, output.into()).unwrap();
		let output = <_>::from_v8(scope, output)
			.map_err(|source| tg::error!(!source, "failed to deserialize the output"))?;

		Ok(Self {
			error,
			exit,
			output,
		})
	}
}
