use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Command {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let command = v8::String::new_external_onebyte_static(scope, "Command".as_bytes()).unwrap();
		let command = tangram.get(scope, command.into()).unwrap();
		let command = v8::Local::<v8::Function>::try_from(command).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = command
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Command {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let command = v8::String::new_external_onebyte_static(scope, "Command".as_bytes()).unwrap();
		let command = tangram.get(scope, command.into()).unwrap();
		let command = v8::Local::<v8::Function>::try_from(command).unwrap();

		if !value.instance_of(scope, command.into()).unwrap() {
			return Err(tg::error!("expected a command"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::command::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::command::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::command::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "args".as_bytes()).unwrap();
		let value = self.args.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "env".as_bytes()).unwrap();
		let value = self.env.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "executable".as_bytes()).unwrap();
		let value = self.executable.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "host".as_bytes()).unwrap();
		let value = self.host.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::command::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let args = v8::String::new_external_onebyte_static(scope, "args".as_bytes()).unwrap();
		let args = value.get(scope, args.into()).unwrap();
		let args = <_>::from_v8(scope, args)
			.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

		let env = v8::String::new_external_onebyte_static(scope, "env".as_bytes()).unwrap();
		let env = value.get(scope, env.into()).unwrap();
		let env = <_>::from_v8(scope, env)
			.map_err(|source| tg::error!(!source, "failed to deserialize the env"))?;

		let executable =
			v8::String::new_external_onebyte_static(scope, "executable".as_bytes()).unwrap();
		let executable = value.get(scope, executable.into()).unwrap();
		let executable = <_>::from_v8(scope, executable)
			.map_err(|source| tg::error!(!source, "failed to deserialize the executable"))?;

		let host = v8::String::new_external_onebyte_static(scope, "host".as_bytes()).unwrap();
		let host = value.get(scope, host.into()).unwrap();
		let host = <_>::from_v8(scope, host)
			.map_err(|source| tg::error!(!source, "failed to deserialize the host"))?;

		Ok(Self {
			args,
			env,
			executable,
			host,
		})
	}
}

impl ToV8 for tg::command::Executable {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			tg::command::Executable::Artifact(artifact) => artifact.to_v8(scope),
			tg::command::Executable::Module(module) => module.to_v8(scope),
		}
	}
}

impl FromV8 for tg::command::Executable {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if let Ok(artifact) = <_>::from_v8(scope, value) {
			Ok(Self::Artifact(artifact))
		} else if let Ok(module) = <_>::from_v8(scope, value) {
			Ok(Self::Module(module))
		} else {
			Err(tg::error!("expected an artifact or a module"))
		}
	}
}

impl ToV8 for tg::command::Module {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let value = self.kind.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "referent".as_bytes()).unwrap();
		let value = self.referent.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::command::Module {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let kind = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let kind = value.get(scope, kind.into()).unwrap();
		let kind = <_>::from_v8(scope, kind)
			.map_err(|source| tg::error!(!source, "failed to deserialize the kind"))?;

		let referent =
			v8::String::new_external_onebyte_static(scope, "referent".as_bytes()).unwrap();
		let referent = value.get(scope, referent.into()).unwrap();
		let referent = <_>::from_v8(scope, referent)
			.map_err(|source| tg::error!(!source, "failed to deserialize the referent"))?;

		Ok(Self { kind, referent })
	}
}
