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

		Ok(Self::new(id, remote, state, None))
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

		let key =
			v8::String::new_external_onebyte_static(scope, "commands_complete".as_bytes()).unwrap();
		let value = self.commands_complete.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "commands_count".as_bytes()).unwrap();
		let value = self.commands_count.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "commands_depth".as_bytes()).unwrap();
		let value = self.commands_depth.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "commands_weight".as_bytes()).unwrap();
		let value = self.commands_weight.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "complete".as_bytes()).unwrap();
		let value = self.complete.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "count".as_bytes()).unwrap();
		let value = self.count.to_v8(scope)?;
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

		let key =
			v8::String::new_external_onebyte_static(scope, "heartbeat_at".as_bytes()).unwrap();
		let value = self.heartbeat_at.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "log".as_bytes()).unwrap();
		let value = self.log.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "logs_complete".as_bytes()).unwrap();
		let value = self.logs_complete.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "logs_count".as_bytes()).unwrap();
		let value = self.logs_count.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "logs_depth".as_bytes()).unwrap();
		let value = self.logs_depth.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "logs_weight".as_bytes()).unwrap();
		let value = self.logs_weight.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let value = self.network.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let value = self.output.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "outputs_complete".as_bytes()).unwrap();
		let value = self.outputs_complete.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "outputs_count".as_bytes()).unwrap();
		let value = self.outputs_count.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "outputs_depth".as_bytes()).unwrap();
		let value = self.outputs_depth.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key =
			v8::String::new_external_onebyte_static(scope, "outputs_weight".as_bytes()).unwrap();
		let value = self.outputs_weight.to_v8(scope)?;
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

		let key = v8::String::new_external_onebyte_static(scope, "touched_at".as_bytes()).unwrap();
		let value = self.touched_at.to_v8(scope)?;
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

		let checksum =
			v8::String::new_external_onebyte_static(scope, "checksum".as_bytes()).unwrap();
		let checksum = value.get(scope, checksum.into()).unwrap();
		let checksum = <_>::from_v8(scope, checksum)
			.map_err(|source| tg::error!(!source, "failed to deserialize the checksum"))?;

		let command = v8::String::new_external_onebyte_static(scope, "command".as_bytes()).unwrap();
		let command = value.get(scope, command.into()).unwrap();
		let command = <_>::from_v8(scope, command)
			.map_err(|source| tg::error!(!source, "failed to deserialize the command"))?;

		let commands_complete =
			v8::String::new_external_onebyte_static(scope, "commands_complete".as_bytes()).unwrap();
		let commands_complete = value.get(scope, commands_complete.into()).unwrap();
		let commands_complete = <_>::from_v8(scope, commands_complete)
			.map_err(|source| tg::error!(!source, "failed to deserialize the commands_complete"))?;

		let commands_count =
			v8::String::new_external_onebyte_static(scope, "commands_count".as_bytes()).unwrap();
		let commands_count = value.get(scope, commands_count.into()).unwrap();
		let commands_count = <_>::from_v8(scope, commands_count)
			.map_err(|source| tg::error!(!source, "failed to deserialize the commands_count"))?;

		let commands_depth =
			v8::String::new_external_onebyte_static(scope, "commands_depth".as_bytes()).unwrap();
		let commands_depth = value.get(scope, commands_depth.into()).unwrap();
		let commands_depth = <_>::from_v8(scope, commands_depth)
			.map_err(|source| tg::error!(!source, "failed to deserialize the commands_depth"))?;

		let commands_weight =
			v8::String::new_external_onebyte_static(scope, "commands_weight".as_bytes()).unwrap();
		let commands_weight = value.get(scope, commands_weight.into()).unwrap();
		let commands_weight = <_>::from_v8(scope, commands_weight)
			.map_err(|source| tg::error!(!source, "failed to deserialize the commands_weight"))?;

		let complete =
			v8::String::new_external_onebyte_static(scope, "complete".as_bytes()).unwrap();
		let complete = value.get(scope, complete.into()).unwrap();
		let complete = <_>::from_v8(scope, complete)
			.map_err(|source| tg::error!(!source, "failed to deserialize the complete"))?;

		let count = v8::String::new_external_onebyte_static(scope, "count".as_bytes()).unwrap();
		let count = value.get(scope, count.into()).unwrap();
		let count = <_>::from_v8(scope, count)
			.map_err(|source| tg::error!(!source, "failed to deserialize the count"))?;

		let created_at =
			v8::String::new_external_onebyte_static(scope, "created_at".as_bytes()).unwrap();
		let created_at = value.get(scope, created_at.into()).unwrap();
		let created_at = <_>::from_v8(scope, created_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the created_at"))?;

		let cwd = v8::String::new_external_onebyte_static(scope, "cwd".as_bytes()).unwrap();
		let cwd = value.get(scope, cwd.into()).unwrap();
		let cwd = <_>::from_v8(scope, cwd)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cwd"))?;

		let dequeued_at =
			v8::String::new_external_onebyte_static(scope, "dequeued_at".as_bytes()).unwrap();
		let dequeued_at = value.get(scope, dequeued_at.into()).unwrap();
		let dequeued_at = <_>::from_v8(scope, dequeued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the dequeued_at"))?;

		let enqueued_at =
			v8::String::new_external_onebyte_static(scope, "enqueued_at".as_bytes()).unwrap();
		let enqueued_at = value.get(scope, enqueued_at.into()).unwrap();
		let enqueued_at = <_>::from_v8(scope, enqueued_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the enqueued_at"))?;

		let env = v8::String::new_external_onebyte_static(scope, "env".as_bytes()).unwrap();
		let env = value.get(scope, env.into()).unwrap();
		let env = <_>::from_v8(scope, env)
			.map_err(|source| tg::error!(!source, "failed to deserialize the env"))?;

		let error = v8::String::new_external_onebyte_static(scope, "error".as_bytes()).unwrap();
		let error = value.get(scope, error.into()).unwrap();
		let error = <_>::from_v8(scope, error)
			.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;

		let exit = v8::String::new_external_onebyte_static(scope, "exit".as_bytes()).unwrap();
		let exit = value.get(scope, exit.into()).unwrap();
		let exit = <_>::from_v8(scope, exit)
			.map_err(|source| tg::error!(!source, "failed to deserialize the exit"))?;

		let finished_at =
			v8::String::new_external_onebyte_static(scope, "finished_at".as_bytes()).unwrap();
		let finished_at = value.get(scope, finished_at.into()).unwrap();
		let finished_at = <_>::from_v8(scope, finished_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the finished_at"))?;

		let heartbeat_at =
			v8::String::new_external_onebyte_static(scope, "heartbeat_at".as_bytes()).unwrap();
		let heartbeat_at = value.get(scope, heartbeat_at.into()).unwrap();
		let heartbeat_at = <_>::from_v8(scope, heartbeat_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the heartbeat_at"))?;

		let log = v8::String::new_external_onebyte_static(scope, "log".as_bytes()).unwrap();
		let log = value.get(scope, log.into()).unwrap();
		let log = <_>::from_v8(scope, log)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log"))?;

		let logs_complete =
			v8::String::new_external_onebyte_static(scope, "logs_complete".as_bytes()).unwrap();
		let logs_complete = value.get(scope, logs_complete.into()).unwrap();
		let logs_complete = <_>::from_v8(scope, logs_complete)
			.map_err(|source| tg::error!(!source, "failed to deserialize the logs_complete"))?;

		let logs_count =
			v8::String::new_external_onebyte_static(scope, "logs_count".as_bytes()).unwrap();
		let logs_count = value.get(scope, logs_count.into()).unwrap();
		let logs_count = <_>::from_v8(scope, logs_count)
			.map_err(|source| tg::error!(!source, "failed to deserialize the logs_count"))?;

		let logs_depth =
			v8::String::new_external_onebyte_static(scope, "logs_depth".as_bytes()).unwrap();
		let logs_depth = value.get(scope, logs_depth.into()).unwrap();
		let logs_depth = <_>::from_v8(scope, logs_depth)
			.map_err(|source| tg::error!(!source, "failed to deserialize the logs_depth"))?;

		let logs_weight =
			v8::String::new_external_onebyte_static(scope, "logs_weight".as_bytes()).unwrap();
		let logs_weight = value.get(scope, logs_weight.into()).unwrap();
		let logs_weight = <_>::from_v8(scope, logs_weight)
			.map_err(|source| tg::error!(!source, "failed to deserialize the logs_weight"))?;

		let network = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let network = value.get(scope, network.into()).unwrap();
		let network = <_>::from_v8(scope, network)
			.map_err(|source| tg::error!(!source, "failed to deserialize the network"))?;

		let output = v8::String::new_external_onebyte_static(scope, "output".as_bytes()).unwrap();
		let output = value.get(scope, output.into()).unwrap();
		let output = <_>::from_v8(scope, output)
			.map_err(|source| tg::error!(!source, "failed to deserialize the output"))?;

		let outputs_complete =
			v8::String::new_external_onebyte_static(scope, "outputs_complete".as_bytes()).unwrap();
		let outputs_complete = value.get(scope, outputs_complete.into()).unwrap();
		let outputs_complete = <_>::from_v8(scope, outputs_complete)
			.map_err(|source| tg::error!(!source, "failed to deserialize the outputs_complete"))?;

		let outputs_count =
			v8::String::new_external_onebyte_static(scope, "outputs_count".as_bytes()).unwrap();
		let outputs_count = value.get(scope, outputs_count.into()).unwrap();
		let outputs_count = <_>::from_v8(scope, outputs_count)
			.map_err(|source| tg::error!(!source, "failed to deserialize the outputs_count"))?;

		let outputs_depth =
			v8::String::new_external_onebyte_static(scope, "outputs_depth".as_bytes()).unwrap();
		let outputs_depth = value.get(scope, outputs_depth.into()).unwrap();
		let outputs_depth = <_>::from_v8(scope, outputs_depth)
			.map_err(|source| tg::error!(!source, "failed to deserialize the outputs_depth"))?;

		let outputs_weight =
			v8::String::new_external_onebyte_static(scope, "outputs_weight".as_bytes()).unwrap();
		let outputs_weight = value.get(scope, outputs_weight.into()).unwrap();
		let outputs_weight = <_>::from_v8(scope, outputs_weight)
			.map_err(|source| tg::error!(!source, "failed to deserialize the outputs_weight"))?;

		let retry = v8::String::new_external_onebyte_static(scope, "retry".as_bytes()).unwrap();
		let retry = value.get(scope, retry.into()).unwrap();
		let retry = <_>::from_v8(scope, retry)
			.map_err(|source| tg::error!(!source, "failed to deserialize the retry"))?;

		let started_at =
			v8::String::new_external_onebyte_static(scope, "started_at".as_bytes()).unwrap();
		let started_at = value.get(scope, started_at.into()).unwrap();
		let started_at = <_>::from_v8(scope, started_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the started_at"))?;

		let status = v8::String::new_external_onebyte_static(scope, "status".as_bytes()).unwrap();
		let status = value.get(scope, status.into()).unwrap();
		let status = <_>::from_v8(scope, status)
			.map_err(|source| tg::error!(!source, "failed to deserialize the status"))?;

		let touched_at =
			v8::String::new_external_onebyte_static(scope, "touched_at".as_bytes()).unwrap();
		let touched_at = value.get(scope, touched_at.into()).unwrap();
		let touched_at = <_>::from_v8(scope, touched_at)
			.map_err(|source| tg::error!(!source, "failed to deserialize the touched_at"))?;

		Ok(Self {
			checksum,
			command,
			commands_complete,
			commands_count,
			commands_depth,
			commands_weight,
			complete,
			count,
			created_at,
			cwd,
			dequeued_at,
			enqueued_at,
			env,
			error,
			exit,
			finished_at,
			heartbeat_at,
			log,
			logs_complete,
			logs_count,
			logs_depth,
			logs_weight,
			network,
			output,
			outputs_complete,
			outputs_count,
			outputs_depth,
			outputs_weight,
			retry,
			started_at,
			status,
			touched_at,
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
