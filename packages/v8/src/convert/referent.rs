use super::{FromV8, ToV8};
use std::path::PathBuf;
use tangram_client as tg;

impl<T> ToV8 for tg::Referent<T>
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"item").unwrap();
		let value = self.item.to_v8(scope)?;
		object.set(scope, key.into(), value);

		if let Some(path) = &self.path {
			let key = v8::String::new_external_onebyte_static(scope, b"path").unwrap();
			let value = path.to_v8(scope)?;
			object.set(scope, key.into(), value);
		}

		if let Some(tag) = &self.tag {
			let key = v8::String::new_external_onebyte_static(scope, b"tag").unwrap();
			let value = tag.to_string().to_v8(scope)?;
			object.set(scope, key.into(), value);
		}

		Ok(object.into())
	}
}

impl<T> FromV8 for tg::Referent<T>
where
	T: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let item = v8::String::new_external_onebyte_static(scope, b"item").unwrap();
		let item = value.get(scope, item.into()).unwrap();
		let item = <_>::from_v8(scope, item)
			.map_err(|source| tg::error!(!source, "failed to deserialize the item"))?;

		let path = v8::String::new_external_onebyte_static(scope, b"path").unwrap();
		let path = value.get(scope, path.into()).unwrap();
		let path: Option<PathBuf> = <_>::from_v8(scope, path)
			.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

		let tag = v8::String::new_external_onebyte_static(scope, b"tag").unwrap();
		let tag = value.get(scope, tag.into()).unwrap();
		let tag: Option<String> = <_>::from_v8(scope, tag)
			.map_err(|source| tg::error!(!source, "failed to deserialize the tag"))?;
		let tag = tag
			.map(|tag| tag.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;

		let referent = Self { item, path, tag };

		Ok(referent)
	}
}
