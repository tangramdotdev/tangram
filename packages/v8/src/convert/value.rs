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
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let blob = v8::String::new_external_onebyte_static(scope, "Blob".as_bytes()).unwrap();
		let blob = tangram.get(scope, blob.into()).unwrap();
		let blob = v8::Local::<v8::Function>::try_from(blob).unwrap();

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

		let graph = v8::String::new_external_onebyte_static(scope, "Graph".as_bytes()).unwrap();
		let graph = tangram.get(scope, graph.into()).unwrap();
		let graph = v8::Local::<v8::Function>::try_from(graph).unwrap();

		let command = v8::String::new_external_onebyte_static(scope, "Command".as_bytes()).unwrap();
		let command = tangram.get(scope, command.into()).unwrap();
		let command = v8::Local::<v8::Function>::try_from(command).unwrap();

		let mutation =
			v8::String::new_external_onebyte_static(scope, "Mutation".as_bytes()).unwrap();
		let mutation = tangram.get(scope, mutation.into()).unwrap();
		let mutation = v8::Local::<v8::Function>::try_from(mutation).unwrap();

		let template =
			v8::String::new_external_onebyte_static(scope, "Template".as_bytes()).unwrap();
		let template = tangram.get(scope, template.into()).unwrap();
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
		} else if value.instance_of(scope, blob.into()).unwrap()
			|| value.instance_of(scope, directory.into()).unwrap()
			|| value.instance_of(scope, file.into()).unwrap()
			|| value.instance_of(scope, symlink.into()).unwrap()
			|| value.instance_of(scope, graph.into()).unwrap()
			|| value.instance_of(scope, command.into()).unwrap()
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
