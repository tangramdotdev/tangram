use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Branch {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let branch = v8::String::new_external_onebyte_static(scope, "Branch".as_bytes()).unwrap();
		let branch = tangram.get(scope, branch.into()).unwrap();
		let branch = v8::Local::<v8::Function>::try_from(branch).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = branch
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Branch {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let branch = v8::String::new_external_onebyte_static(scope, "Branch".as_bytes()).unwrap();
		let branch = tangram.get(scope, branch.into()).unwrap();
		let branch = v8::Local::<v8::Function>::try_from(branch).unwrap();

		if !value.instance_of(scope, branch.into()).unwrap() {
			return Err(tg::error!("expected a branch"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::branch::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::branch::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::branch::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "children".as_bytes()).unwrap();
		let value = self.children.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::branch::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let children =
			v8::String::new_external_onebyte_static(scope, "children".as_bytes()).unwrap();
		let children = value.get(scope, children.into()).unwrap();
		let children = <_>::from_v8(scope, children)
			.map_err(|source| tg::error!(!source, "failed to deserialize the children"))?;

		Ok(Self { children })
	}
}

impl ToV8 for tg::branch::Child {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "blob".as_bytes()).unwrap();
		let value = self.blob.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "length".as_bytes()).unwrap();
		let value = self.length.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::branch::Child {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let blob = v8::String::new_external_onebyte_static(scope, "blob".as_bytes()).unwrap();
		let blob = value.get(scope, blob.into()).unwrap();
		let blob = <_>::from_v8(scope, blob)
			.map_err(|source| tg::error!(!source, "failed to deserialize the blob"))?;

		let length = v8::String::new_external_onebyte_static(scope, "length".as_bytes()).unwrap();
		let length = value.get(scope, length.into()).unwrap();
		let length = <_>::from_v8(scope, length)
			.map_err(|source| tg::error!(!source, "failed to deserialize the length"))?;

		Ok(Self { blob, length })
	}
}
