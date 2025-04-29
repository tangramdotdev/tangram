use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Blob {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let blob = v8::String::new_external_onebyte_static(scope, "Blob".as_bytes()).unwrap();
		let blob = tangram.get(scope, blob.into()).unwrap();
		let blob = v8::Local::<v8::Function>::try_from(blob).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = blob
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Blob {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let blob = v8::String::new_external_onebyte_static(scope, "Blob".as_bytes()).unwrap();
		let blob = tangram.get(scope, blob.into()).unwrap();
		let blob = v8::Local::<v8::Function>::try_from(blob).unwrap();

		if !value.instance_of(scope, blob.into()).unwrap() {
			return Err(tg::error!("expected a blob"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::blob::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::blob::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::blob::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		match self {
			tg::blob::Object::Leaf(leaf) => {
				let key =
					v8::String::new_external_onebyte_static(scope, "bytes".as_bytes()).unwrap();
				let value = leaf.bytes.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
			tg::blob::Object::Branch(branch) => {
				let key =
					v8::String::new_external_onebyte_static(scope, "children".as_bytes()).unwrap();
				let value = branch.children.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
		}

		Ok(object.into())
	}
}

impl FromV8 for tg::blob::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let bytes = v8::String::new_external_onebyte_static(scope, "bytes".as_bytes()).unwrap();
		let children =
			v8::String::new_external_onebyte_static(scope, "children".as_bytes()).unwrap();
		let blob = if let Some(bytes) = value.get(scope, bytes.into()) {
			let bytes = <_>::from_v8(scope, bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the bytes"))?;
			Self::Leaf(tg::blob::object::Leaf { bytes })
		} else if let Some(children) = value.get(scope, children.into()) {
			let children = <_>::from_v8(scope, children)
				.map_err(|source| tg::error!(!source, "failed to deserialize the children"))?;
			Self::Branch(tg::blob::object::Branch { children })
		} else {
			return Err(tg::error!("invalid object"));
		};

		Ok(blob)
	}
}

impl ToV8 for tg::blob::Child {
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

impl FromV8 for tg::blob::Child {
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
