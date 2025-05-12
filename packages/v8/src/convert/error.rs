use super::{FromV8, ToV8, serde::Serde};
use tangram_client as tg;

impl ToV8 for tg::Error {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, b"Error").unwrap();
		let error = tangram.get(scope, error.into()).unwrap();
		let error = v8::Local::<v8::Function>::try_from(error).unwrap();

		let arg = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"message").unwrap();
		let value = self.message.to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"location").unwrap();
		let value = self.location.to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"stack").unwrap();
		let value = self.stack.to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"source").unwrap();
		let value = self.source.to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"values").unwrap();
		let value = self.values.to_v8(scope)?;
		arg.set(scope, key.into(), value);

		let instance = error
			.new_instance(scope, &[arg.into()])
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
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let error = v8::String::new_external_onebyte_static(scope, b"Error").unwrap();
		let error = tangram.get(scope, error.into()).unwrap();
		let error = v8::Local::<v8::Function>::try_from(error).unwrap();

		if !value.instance_of(scope, error.into()).unwrap() {
			return Err(tg::error!("expected an error"));
		}
		let value = value.to_object(scope).unwrap();

		let code = v8::String::new_external_onebyte_static(scope, b"code").unwrap();
		let code = value.get(scope, code.into()).unwrap();
		let code = <_>::from_v8(scope, code)
			.map_err(|source| tg::error!(!source, "failed to deserialize the code"))?;

		let message = v8::String::new_external_onebyte_static(scope, b"message").unwrap();
		let message = value.get(scope, message.into()).unwrap();
		let message = <_>::from_v8(scope, message)
			.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

		let location = v8::String::new_external_onebyte_static(scope, b"location").unwrap();
		let location = value.get(scope, location.into()).unwrap();
		let location = <_>::from_v8(scope, location)
			.map_err(|source| tg::error!(!source, "failed to deserialize the location"))?;

		let source = v8::String::new_external_onebyte_static(scope, b"source").unwrap();
		let source = value.get(scope, source.into()).unwrap();
		let source = <_>::from_v8(scope, source)
			.map_err(|source| tg::error!(!source, "failed to deserialize the source"))?;

		let stack = v8::String::new_external_onebyte_static(scope, b"stack").unwrap();
		let stack = value.get(scope, stack.into()).unwrap();
		let stack = <_>::from_v8(scope, stack)
			.map_err(|source| tg::error!(!source, "failed to deserialize the stack"))?;

		let values = v8::String::new_external_onebyte_static(scope, b"values").unwrap();
		let values: v8::Local<'_, v8::Value> = value.get(scope, values.into()).unwrap();
		let values = <_>::from_v8(scope, values)
			.map_err(|source| tg::error!(!source, "failed to deserialize the values"))?;

		Ok(tg::Error {
			code,
			message,
			location,
			stack,
			source,
			values,
		})
	}
}

impl ToV8 for tg::error::Code {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = Serde(self);
		let value = value.to_v8(scope)?;
		Ok(value)
	}
}

impl FromV8 for tg::error::Code {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = Serde::from_v8(scope, value)?.0;
		Ok(value)
	}
}

impl ToV8 for tg::error::Location {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"symbol").unwrap();
		let value = self.symbol.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"file").unwrap();
		let value = self.file.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"line").unwrap();
		let value = self.line.to_v8(scope)?;
		object.set(scope, key.into(), value);

		let key = v8::String::new_external_onebyte_static(scope, b"column").unwrap();
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
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let symbol = v8::String::new_external_onebyte_static(scope, b"symbol").unwrap();
		let symbol = value.get(scope, symbol.into()).unwrap();
		let symbol = <_>::from_v8(scope, symbol)
			.map_err(|source| tg::error!(!source, "failed to deserialize the symbol"))?;

		let file = v8::String::new_external_onebyte_static(scope, b"file").unwrap();
		let file = value.get(scope, file.into()).unwrap();
		let file = <_>::from_v8(scope, file)
			.map_err(|source| tg::error!(!source, "failed to deserialize the file"))?;

		let line = v8::String::new_external_onebyte_static(scope, b"line").unwrap();
		let line = value.get(scope, line.into()).unwrap();
		let line = <_>::from_v8(scope, line)
			.map_err(|source| tg::error!(!source, "failed to deserialize the line"))?;

		let column = v8::String::new_external_onebyte_static(scope, b"column").unwrap();
		let column = value.get(scope, column.into()).unwrap();
		let column = <_>::from_v8(scope, column)
			.map_err(|source| tg::error!(!source, "failed to deserialize the column"))?;

		Ok(Self {
			symbol,
			file,
			line,
			column,
		})
	}
}

impl ToV8 for tg::error::File {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let (kind, value) = match self {
			Self::Internal(path) => ("internal", path.to_v8(scope)?),
			Self::Module(module) => ("module", module.to_v8(scope)?),
		};
		let object = v8::Object::new(scope);
		let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
		let kind = v8::String::new_external_onebyte_static(scope, kind.as_bytes()).unwrap();
		object.set(scope, key.into(), kind.into());
		let key = v8::String::new_external_onebyte_static(scope, b"value").unwrap();
		object.set(scope, key.into(), value);
		Ok(object.into())
	}
}

impl FromV8 for tg::error::File {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();
		let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
		let kind = value.get(scope, key.into()).unwrap();
		let kind = String::from_v8(scope, kind).unwrap();
		let key = v8::String::new_external_onebyte_static(scope, b"value").unwrap();
		let value = value.get(scope, key.into()).unwrap();
		let value = match kind.as_str() {
			"internal" => Self::Internal(<_>::from_v8(scope, value)?),
			"module" => Self::Module(<_>::from_v8(scope, value)?),
			_ => {
				return Err(tg::error!(%kind, "invalid kind"));
			},
		};
		Ok(value)
	}
}
