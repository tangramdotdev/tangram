use super::{FromV8, ToV8, serde::Serde};
use tangram_client as tg;

impl ToV8 for tg::Module {
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

impl FromV8 for tg::Module {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
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

impl ToV8 for tg::module::Item {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			tg::module::Item::Path(path) => path.to_v8(scope),
			tg::module::Item::Object(object) => object.to_v8(scope),
		}
	}
}

impl FromV8 for tg::module::Item {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if value.is_string() {
			let path = <_>::from_v8(scope, value)?;
			Ok(tg::module::Item::Path(path))
		} else {
			let object = <_>::from_v8(scope, value)?;
			Ok(tg::module::Item::Object(object))
		}
	}
}

impl ToV8 for tg::module::Data {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde::new(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::module::Data {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.into_inner();
		Ok(value)
	}
}

impl ToV8 for tg::module::Kind {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::module::Kind {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}
