use super::{serde::Serde, FromV8, ToV8};
use std::{collections::BTreeMap, sync::Arc};
use tangram_client as tg;

impl ToV8 for tg::Error {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, "Error".as_bytes()).unwrap();
		let error = tg.get(scope, error.into()).unwrap();
		let error = v8::Local::<v8::Function>::try_from(error).unwrap();

		let message = self.message.to_v8(scope)?;
		let location = self.location.to_v8(scope)?;
		let stack = self.stack.to_v8(scope)?;
		let source = self.source.to_v8(scope)?;
		let values = self.values.to_v8(scope)?;

		let instance = error
			.new_instance(scope, &[message, location, stack, source, values])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Error {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, "Error".as_bytes()).unwrap();
		let error = tg.get(scope, error.into()).unwrap();
		let error = v8::Local::<v8::Function>::try_from(error).unwrap();

		if !value.instance_of(scope, error.into()).unwrap() {
			return Err(tg::error!("expected an error"));
		}
		let value = value.to_object(scope).unwrap();

		let message = v8::String::new_external_onebyte_static(scope, "message".as_bytes()).unwrap();
		let message = value.get(scope, message.into()).unwrap();
		let message = <_>::from_v8(scope, message)
			.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

		let location =
			v8::String::new_external_onebyte_static(scope, "location".as_bytes()).unwrap();
		let location = value.get(scope, location.into()).unwrap();
		let location = <_>::from_v8(scope, location)
			.map_err(|source| tg::error!(!source, "failed to deserialize the location"))?;

		let stack = v8::String::new_external_onebyte_static(scope, "stack".as_bytes()).unwrap();
		let stack = value.get(scope, stack.into()).unwrap();
		let stack = <_>::from_v8(scope, stack)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stack"))?;

		let source = v8::String::new_external_onebyte_static(scope, "source".as_bytes()).unwrap();
		let source = value.get(scope, source.into()).unwrap();
		let source = <Option<tg::Error>>::from_v8(scope, source)?.map(|error| Arc::new(error) as _);

		let values = v8::String::new_external_onebyte_static(scope, "values".as_bytes()).unwrap();
		let values: v8::Local<'_, v8::Value> = value.get(scope, values.into()).unwrap();
		let values =
			<Option<BTreeMap<String, String>>>::from_v8(scope, values)?.unwrap_or_default();

		Ok(tg::Error {
			message,
			location,
			stack,
			source,
			values,
		})
	}
}

impl ToV8 for tg::error::Location {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "symbol".as_bytes()).unwrap();
		let value = self.symbol.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "source".as_bytes()).unwrap();
		let value = self.source.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "line".as_bytes()).unwrap();
		let value = self.line.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, "column".as_bytes()).unwrap();
		let value = self.column.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::error::Location {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let symbol = v8::String::new_external_onebyte_static(scope, "symbol".as_bytes()).unwrap();
		let symbol = value.get(scope, symbol.into()).unwrap();
		let symbol = <_>::from_v8(scope, symbol)
			.map_err(|source| tg::error!(!source, "failed to deserialize the symbol"))?;

		let source = v8::String::new_external_onebyte_static(scope, "source".as_bytes()).unwrap();
		let source = value.get(scope, source.into()).unwrap();
		let source = <_>::from_v8(scope, source)
			.map_err(|source| tg::error!(!source, "failed to deserialize the source"))?;

		let line = v8::String::new_external_onebyte_static(scope, "line".as_bytes()).unwrap();
		let line = value.get(scope, line.into()).unwrap();
		let line = <_>::from_v8(scope, line)
			.map_err(|source| tg::error!(!source, "failed to deserialize the line"))?;

		let column = v8::String::new_external_onebyte_static(scope, "column".as_bytes()).unwrap();
		let column = value.get(scope, column.into()).unwrap();
		let column = <_>::from_v8(scope, column)
			.map_err(|source| tg::error!(!source, "failed to deserialize the column"))?;

		Ok(Self {
			symbol,
			source,
			line,
			column,
		})
	}
}

impl ToV8 for tg::error::Source {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde::new(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::error::Source {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.into_inner();
		Ok(value)
	}
}
