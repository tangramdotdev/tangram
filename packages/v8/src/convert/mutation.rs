use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Mutation {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);
		match self {
			tg::Mutation::Unset => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "unset".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
			},
			tg::Mutation::Set { value: value_ } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "set".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value = value_.clone().to_v8(scope).unwrap();
				object.set(scope, key.into(), value);
			},
			tg::Mutation::SetIfUnset { value: value_ } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "set_if_unset".as_bytes())
						.unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value = value_.clone().to_v8(scope).unwrap();
				object.set(scope, key.into(), value);
			},
			tg::Mutation::Prepend { values } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "prepend".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "values".as_bytes()).unwrap();
				let value = values.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
			tg::Mutation::Append { values } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "append".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "values".as_bytes()).unwrap();
				let value = values.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
			tg::Mutation::Prefix {
				template,
				separator,
			} => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "prefix".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "template".as_bytes()).unwrap();
				let value = template.to_v8(scope)?;
				object.set(scope, key.into(), value);
				let key =
					v8::String::new_external_onebyte_static(scope, "separator".as_bytes()).unwrap();
				let value = separator.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
			tg::Mutation::Suffix {
				template,
				separator,
			} => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "suffix".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "template".as_bytes()).unwrap();
				let value = template.to_v8(scope)?;
				object.set(scope, key.into(), value);
				let key =
					v8::String::new_external_onebyte_static(scope, "separator".as_bytes()).unwrap();
				let value = separator.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
			tg::Mutation::Merge { value: value_ } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "set".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());
				let key =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value = value_.clone().to_v8(scope).unwrap();
				object.set(scope, key.into(), value);
			},
		}

		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let mutation =
			v8::String::new_external_onebyte_static(scope, "Mutation".as_bytes()).unwrap();
		let mutation = tangram.get(scope, mutation.into()).unwrap();
		let mutation = v8::Local::<v8::Function>::try_from(mutation).unwrap();

		let instance = mutation
			.new_instance(scope, &[object.into()])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Mutation {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let mutation =
			v8::String::new_external_onebyte_static(scope, "Mutation".as_bytes()).unwrap();
		let mutation = tangram.get(scope, mutation.into()).unwrap();
		let mutation = v8::Local::<v8::Function>::try_from(mutation).unwrap();

		if !value.instance_of(scope, mutation.into()).unwrap() {
			return Err(tg::error!("expected a mutation"));
		}
		let value = value.to_object(scope).unwrap();

		let inner = v8::String::new_external_onebyte_static(scope, "inner".as_bytes()).unwrap();
		let inner = value.get(scope, inner.into()).unwrap();
		if !inner.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let inner = inner.to_object(scope).unwrap();

		let kind = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let kind = inner.get(scope, kind.into()).unwrap();
		let kind = String::from_v8(scope, kind)?;

		let mutation = match kind.as_str() {
			"unset" => tg::Mutation::Unset,
			"set" => {
				let value_ =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value_ = inner.get(scope, value_.into()).unwrap();
				let value_ = <_>::from_v8(scope, value_)
					.map_err(|source| tg::error!(!source, "failed to deserialize the value_"))?;
				let value_ = Box::new(value_);
				tg::Mutation::Set { value: value_ }
			},
			"set_if_unset" => {
				let value_ =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value_ = inner.get(scope, value_.into()).unwrap();
				let value_ = <_>::from_v8(scope, value_)
					.map_err(|source| tg::error!(!source, "failed to deserialize the value_"))?;
				let value_ = Box::new(value_);
				tg::Mutation::SetIfUnset { value: value_ }
			},
			"prepend" => {
				let values =
					v8::String::new_external_onebyte_static(scope, "values".as_bytes()).unwrap();
				let values = inner.get(scope, values.into()).unwrap();
				let values = <_>::from_v8(scope, values)
					.map_err(|source| tg::error!(!source, "failed to deserialize the values"))?;
				tg::Mutation::Prepend { values }
			},
			"append" => {
				let values =
					v8::String::new_external_onebyte_static(scope, "values".as_bytes()).unwrap();
				let values = inner.get(scope, values.into()).unwrap();
				let values = <_>::from_v8(scope, values)
					.map_err(|source| tg::error!(!source, "failed to deserialize the values"))?;
				tg::Mutation::Append { values }
			},
			"prefix" => {
				let template =
					v8::String::new_external_onebyte_static(scope, "template".as_bytes()).unwrap();
				let template = inner.get(scope, template.into()).unwrap();
				let template = <_>::from_v8(scope, template)
					.map_err(|source| tg::error!(!source, "failed to deserialize the template"))?;
				let separator =
					v8::String::new_external_onebyte_static(scope, "separator".as_bytes()).unwrap();
				let separator = inner.get(scope, separator.into()).unwrap();
				let separator = <_>::from_v8(scope, separator)
					.map_err(|source| tg::error!(!source, "failed to deserialize the separator"))?;
				tg::Mutation::Prefix {
					template,
					separator,
				}
			},
			"suffix" => {
				let template =
					v8::String::new_external_onebyte_static(scope, "template".as_bytes()).unwrap();
				let template = inner.get(scope, template.into()).unwrap();
				let template = <_>::from_v8(scope, template)
					.map_err(|source| tg::error!(!source, "failed to deserialize the template"))?;
				let separator =
					v8::String::new_external_onebyte_static(scope, "separator".as_bytes()).unwrap();
				let separator = inner.get(scope, separator.into()).unwrap();
				let separator = <_>::from_v8(scope, separator)
					.map_err(|source| tg::error!(!source, "failed to deserialize the separator"))?;
				tg::Mutation::Suffix {
					template,
					separator,
				}
			},
			"merge" => {
				let value_ =
					v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
				let value_ = inner.get(scope, value_.into()).unwrap();
				let value_ = <_>::from_v8(scope, value_)
					.map_err(|source| tg::error!(!source, "failed to deserialize the value_"))?;
				tg::Mutation::Merge { value: value_ }
			},
			kind => {
				return Err(tg::error!(%kind, "invalid mutation kind"));
			},
		};

		Ok(mutation)
	}
}
