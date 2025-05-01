use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Template {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let template =
			v8::String::new_external_onebyte_static(scope, "Template".as_bytes()).unwrap();
		let template = tangram.get(scope, template.into()).unwrap();
		let template = v8::Local::<v8::Function>::try_from(template).unwrap();

		let components = self.components().to_v8(scope)?;

		let instance = template
			.new_instance(scope, &[components])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Template {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let template =
			v8::String::new_external_onebyte_static(scope, "Template".as_bytes()).unwrap();
		let template = tangram.get(scope, template.into()).unwrap();
		let template = v8::Local::<v8::Function>::try_from(template).unwrap();

		if !value.instance_of(scope, template.into()).unwrap() {
			return Err(tg::error!("expected a template"));
		}
		let value = value.to_object(scope).unwrap();

		let components =
			v8::String::new_external_onebyte_static(scope, "components".as_bytes()).unwrap();
		let components = value.get(scope, components.into()).unwrap();
		let components = <Vec<_>>::from_v8(scope, components)
			.map_err(|source| tg::error!(!source, "failed to deserialize the components"))?;

		let template = Self::with_components(components);

		Ok(template)
	}
}

impl ToV8 for tg::template::Component {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Self::String(string) => string.to_v8(scope),
			Self::Artifact(artifact) => artifact.to_v8(scope),
		}
	}
}

impl FromV8 for tg::template::Component {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if let Ok(string) = <_>::from_v8(scope, value) {
			Ok(Self::String(string))
		} else if let Ok(artifact) = <_>::from_v8(scope, value) {
			Ok(Self::Artifact(artifact))
		} else {
			Err(tg::error!("expected a string or an artifact"))
		}
	}
}
