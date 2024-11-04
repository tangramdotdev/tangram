use super::{serde::Serde, FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Module {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let kind = match self.kind {
			tg::module::Kind::Js => "js",
			tg::module::Kind::Ts => "ts",
			tg::module::Kind::Dts => "dts",
			tg::module::Kind::Object => "object",
			tg::module::Kind::Blob => "blob",
			tg::module::Kind::Artifact => "artifact",
			tg::module::Kind::Leaf => "leaf",
			tg::module::Kind::Branch => "branch",
			tg::module::Kind::Directory => "directory",
			tg::module::Kind::File => "file",
			tg::module::Kind::Symlink => "symlink",
			tg::module::Kind::Graph => "graph",
			tg::module::Kind::Target => "target",
		};
		let key = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let value = v8::String::new_external_onebyte_static(scope, kind.as_bytes()).unwrap();
		object.set(scope, key.into(), value.into());

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

impl ToV8 for tg::module::Item {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Self::Path(path) => path.to_v8(scope),
			Self::Object(object) => object.to_v8(scope),
		}
	}
}

impl FromV8 for tg::module::Item {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if let Ok(path) = <_>::from_v8(scope, value) {
			Ok(Self::Path(path))
		} else if let Ok(object) = <_>::from_v8(scope, value) {
			Ok(Self::Object(object))
		} else {
			Err(tg::error!("expected a path or an object"))
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
