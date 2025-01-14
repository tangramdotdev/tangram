use super::{FromV8, ToV8};
use std::sync::Arc;
use tangram_client as tg;

impl ToV8 for tg::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			tg::Object::Leaf(leaf) => leaf.to_v8(scope),
			tg::Object::Branch(branch) => branch.to_v8(scope),
			tg::Object::Directory(directory) => directory.to_v8(scope),
			tg::Object::File(file) => file.to_v8(scope),
			tg::Object::Symlink(symlink) => symlink.to_v8(scope),
			tg::Object::Graph(graph) => graph.to_v8(scope),
			tg::Object::Command(command) => command.to_v8(scope),
		}
	}
}

impl FromV8 for tg::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let leaf = v8::String::new_external_onebyte_static(scope, "Leaf".as_bytes()).unwrap();
		let leaf = tangram.get(scope, leaf.into()).unwrap();
		let leaf = v8::Local::<v8::Function>::try_from(leaf).unwrap();

		let branch = v8::String::new_external_onebyte_static(scope, "Branch".as_bytes()).unwrap();
		let branch = tangram.get(scope, branch.into()).unwrap();
		let branch = v8::Local::<v8::Function>::try_from(branch).unwrap();

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
		let target = tangram.get(scope, command.into()).unwrap();
		let target = v8::Local::<v8::Function>::try_from(target).unwrap();

		if value.instance_of(scope, leaf.into()).unwrap() {
			Ok(Self::Leaf(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, branch.into()).unwrap() {
			Ok(Self::Branch(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, directory.into()).unwrap() {
			Ok(Self::Directory(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, file.into()).unwrap() {
			Ok(Self::File(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, symlink.into()).unwrap() {
			Ok(Self::Symlink(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, graph.into()).unwrap() {
			Ok(Self::Graph(<_>::from_v8(scope, value)?))
		} else if value.instance_of(scope, target.into()).unwrap() {
			Ok(Self::Command(<_>::from_v8(scope, value)?))
		} else {
			return Err(tg::error!("invalid object"));
		}
	}
}

impl ToV8 for tg::object::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::object::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::object::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let (kind, value) = match self {
			Self::Leaf(blob) => ("leaf", blob.to_v8(scope)?),
			Self::Branch(blob) => ("branch", blob.to_v8(scope)?),
			Self::Directory(directory) => ("directory", directory.to_v8(scope)?),
			Self::File(file) => ("file", file.to_v8(scope)?),
			Self::Symlink(symlink) => ("symlink", symlink.to_v8(scope)?),
			Self::Graph(graph) => ("graph", graph.to_v8(scope)?),
			Self::Command(command) => ("target", command.to_v8(scope)?),
		};
		let object = v8::Object::new(scope);
		let key = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let kind = v8::String::new_external_onebyte_static(scope, kind.as_bytes()).unwrap();
		object.set(scope, key.into(), kind.into());
		let key = v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
		object.set(scope, key.into(), value);
		Ok(object.into())
	}
}

impl FromV8 for tg::object::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();
		let key = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let kind = value.get(scope, key.into()).unwrap();
		let kind = String::from_v8(scope, kind).unwrap();
		let key = v8::String::new_external_onebyte_static(scope, "value".as_bytes()).unwrap();
		let value = value.get(scope, key.into()).unwrap();
		let value = match kind.as_str() {
			"leaf" => Self::Leaf(<_>::from_v8(scope, value)?),
			"branch" => Self::Branch(<_>::from_v8(scope, value)?),
			"directory" => Self::Directory(<_>::from_v8(scope, value)?),
			"file" => Self::File(<_>::from_v8(scope, value)?),
			"symlink" => Self::Symlink(<_>::from_v8(scope, value)?),
			"graph" => Self::Graph(<_>::from_v8(scope, value)?),
			"target" => Self::Command(<_>::from_v8(scope, value)?),
			_ => unreachable!(),
		};
		Ok(value)
	}
}

impl<I, O> ToV8 for tg::object::State<I, O>
where
	I: ToV8,
	O: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, "id".as_bytes()).unwrap();
		let value = self.id().to_v8(scope)?;
		object.set(scope, key.into(), value);

		if self.id().is_none() {
			let key = v8::String::new_external_onebyte_static(scope, "object".as_bytes()).unwrap();
			let value = self.object().to_v8(scope)?;
			object.set(scope, key.into(), value);
		}

		Ok(object.into())
	}
}

impl<I, O> FromV8 for tg::object::State<I, O>
where
	I: FromV8,
	O: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let id = v8::String::new_external_onebyte_static(scope, "id".as_bytes()).unwrap();
		let id = value.get(scope, id.into()).unwrap();
		let id = <Option<I>>::from_v8(scope, id)
			.map_err(|source| tg::error!(!source, "failed to deserialize the id"))?;

		let object = if id.is_none() {
			let object =
				v8::String::new_external_onebyte_static(scope, "object".as_bytes()).unwrap();
			let object = value.get(scope, object.into()).unwrap();
			<Option<O>>::from_v8(scope, object)
				.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?
				.map(Arc::new)
		} else {
			None
		};

		Ok(Self::new(id, object))
	}
}
