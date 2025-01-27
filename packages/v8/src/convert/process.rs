use super::{FromV8, ToV8};
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

impl ToV8 for tg::process::Sandbox {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let value = self.network.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "filesystem".as_bytes()).unwrap();
		let value = self.filesystem.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::process::Sandbox {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let network = v8::String::new_external_onebyte_static(scope, "network".as_bytes()).unwrap();
		let network = value.get(scope, network.into()).unwrap();
		let network = <_>::from_v8(scope, network)
			.map_err(|source| tg::error!(!source, "failed to deserialize the network"))?;

		let filesystem =
			v8::String::new_external_onebyte_static(scope, "filesystem".as_bytes()).unwrap();
		let filesystem = value.get(scope, filesystem.into()).unwrap();
		let filesystem = <_>::from_v8(scope, filesystem)
			.map_err(|source| tg::error!(!source, "failed to deserialize the filesystem"))?;

		Ok(Self {
			network,
			filesystem,
		})
	}
}
