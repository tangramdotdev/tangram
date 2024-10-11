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
		let components: Vec<tg::template::Component> = <_>::from_v8(scope, components)
			.map_err(|source| tg::error!(!source, "failed to deserialize the components"))?;

		Ok(Self::with_components(components))
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
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let directory =
			v8::String::new_external_onebyte_static(scope, "Directory".as_bytes()).unwrap();
		let directory = tangram.get(scope, directory.into()).unwrap();
		let directory = v8::Local::<v8::Function>::try_from(directory).unwrap();

		let file = v8::String::new_external_onebyte_static(scope, "File".as_bytes()).unwrap();
		let file = tangram.get(scope, file.into()).unwrap();
		let file = v8::Local::<v8::Function>::try_from(file).unwrap();

		let symlink = v8::String::new_external_onebyte_static(scope, "Symlink".as_bytes()).unwrap();
		let symlink = tangram.get(scope, symlink.into()).unwrap();
		let symlink = v8::Local::<v8::Function>::try_from(symlink).unwrap();

		let component = if value.is_string() {
			Self::String(<_>::from_v8(scope, value)?)
		} else if value.instance_of(scope, directory.into()).unwrap()
			|| value.instance_of(scope, file.into()).unwrap()
			|| value.instance_of(scope, symlink.into()).unwrap()
		{
			Self::Artifact(<_>::from_v8(scope, value)?)
		} else {
			return Err(tg::error!("expected a string or artifact"));
		};

		Ok(component)
	}
}
