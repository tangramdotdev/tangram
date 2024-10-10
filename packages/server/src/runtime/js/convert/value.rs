use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Value {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Self::Null => Ok(v8::undefined(scope).into()),
			Self::Bool(value) => value.to_v8(scope),
			Self::Number(value) => value.to_v8(scope),
			Self::String(value) => value.to_v8(scope),
			Self::Array(value) => value.to_v8(scope),
			Self::Map(value) => value.to_v8(scope),
			Self::Object(value) => value.to_v8(scope),
			Self::Bytes(value) => value.to_v8(scope),
			Self::Mutation(value) => value.to_v8(scope),
			Self::Template(value) => value.to_v8(scope),
		}
	}
}

impl FromV8 for tg::Value {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		let leaf = v8::String::new_external_onebyte_static(scope, "Leaf".as_bytes()).unwrap();
		let leaf = tg.get(scope, leaf.into()).unwrap();
		let leaf = v8::Local::<v8::Function>::try_from(leaf).unwrap();

		let branch = v8::String::new_external_onebyte_static(scope, "Branch".as_bytes()).unwrap();
		let branch = tg.get(scope, branch.into()).unwrap();
		let branch = v8::Local::<v8::Function>::try_from(branch).unwrap();

		let directory =
			v8::String::new_external_onebyte_static(scope, "Directory".as_bytes()).unwrap();
		let directory = tg.get(scope, directory.into()).unwrap();
		let directory = v8::Local::<v8::Function>::try_from(directory).unwrap();

		let file = v8::String::new_external_onebyte_static(scope, "File".as_bytes()).unwrap();
		let file = tg.get(scope, file.into()).unwrap();
		let file = v8::Local::<v8::Function>::try_from(file).unwrap();

		let symlink = v8::String::new_external_onebyte_static(scope, "Symlink".as_bytes()).unwrap();
		let symlink = tg.get(scope, symlink.into()).unwrap();
		let symlink = v8::Local::<v8::Function>::try_from(symlink).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, "Graph".as_bytes()).unwrap();
		let graph = tg.get(scope, graph.into()).unwrap();
		let graph = v8::Local::<v8::Function>::try_from(graph).unwrap();

		let target = v8::String::new_external_onebyte_static(scope, "Target".as_bytes()).unwrap();
		let target = tg.get(scope, target.into()).unwrap();
		let target = v8::Local::<v8::Function>::try_from(target).unwrap();

		let mutation =
			v8::String::new_external_onebyte_static(scope, "Mutation".as_bytes()).unwrap();
		let mutation = tg.get(scope, mutation.into()).unwrap();
		let mutation = v8::Local::<v8::Function>::try_from(mutation).unwrap();

		let template =
			v8::String::new_external_onebyte_static(scope, "Template".as_bytes()).unwrap();
		let template = tg.get(scope, template.into()).unwrap();
		let template = v8::Local::<v8::Function>::try_from(template).unwrap();

		if value.is_null_or_undefined() {
			Ok(Self::Null)
		} else if value.is_boolean() {
			Ok(Self::Bool(<_>::from_v8(scope, value)?))
		} else if value.is_number() {
			Ok(Self::Number(<_>::from_v8(scope, value)?))
		} else if value.is_string() {
			Ok(Self::String(<_>::from_v8(scope, value)?))
		} else if value.is_array() {
			Ok(Self::Array(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, leaf.into()).unwrap()
			|| value.instance_of(scope, branch.into()).unwrap()
			|| value.instance_of(scope, directory.into()).unwrap()
			|| value.instance_of(scope, file.into()).unwrap()
			|| value.instance_of(scope, symlink.into()).unwrap()
			|| value.instance_of(scope, graph.into()).unwrap()
			|| value.instance_of(scope, target.into()).unwrap()
		{
			Ok(Self::Object(<_>::from_v8(scope, value)?))
		} else if value.is_uint8_array() {
			Ok(Self::Bytes(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, mutation.into()).unwrap() {
			Ok(Self::Mutation(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, template.into()).unwrap() {
			Ok(Self::Template(<_>::from_v8(scope, value)?))
		} else if value.is_object() {
			Ok(Self::Map(<_>::from_v8(scope, value)?))
		} else {
			return Err(tg::error!("invalid value"));
		}
	}
}
