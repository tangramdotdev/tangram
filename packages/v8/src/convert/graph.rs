use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Graph {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, b"Graph").unwrap();
		let graph = tangram.get(scope, graph.into()).unwrap();
		let graph = v8::Local::<v8::Function>::try_from(graph).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = graph
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Graph {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, b"Graph").unwrap();
		let graph = tangram.get(scope, graph.into()).unwrap();
		let graph = v8::Local::<v8::Function>::try_from(graph).unwrap();

		if !value.instance_of(scope, graph.into()).unwrap() {
			return Err(tg::error!("expected a graph"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, b"state").unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::graph::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::graph::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::graph::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		let key = v8::String::new_external_onebyte_static(scope, b"nodes").unwrap();
		let value = self.nodes.to_v8(scope)?;
		object.set(scope, key.into(), value);

		Ok(object.into())
	}
}

impl FromV8 for tg::graph::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let nodes = v8::String::new_external_onebyte_static(scope, b"nodes").unwrap();
		let nodes = value.get(scope, nodes.into()).unwrap();
		let nodes = <_>::from_v8(scope, nodes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the nodes"))?;

		Ok(Self { nodes })
	}
}

impl ToV8 for tg::graph::Node {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		match self {
			tg::graph::Node::Directory(tg::graph::object::Directory { entries }) => {
				let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
				let value = v8::String::new_external_onebyte_static(scope, b"directory").unwrap();
				object.set(scope, key.into(), value.into());

				let key = v8::String::new_external_onebyte_static(scope, b"entries").unwrap();
				let value = entries.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::graph::Node::File(tg::graph::object::File {
				contents,
				dependencies,
				executable,
			}) => {
				let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
				let value = v8::String::new_external_onebyte_static(scope, b"file").unwrap();
				object.set(scope, key.into(), value.into());

				let key = v8::String::new_external_onebyte_static(scope, b"contents").unwrap();
				let value = contents.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key = v8::String::new_external_onebyte_static(scope, b"dependencies").unwrap();

				let value = dependencies.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key = v8::String::new_external_onebyte_static(scope, b"executable").unwrap();
				let value = executable.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::graph::Node::Symlink(tg::graph::object::Symlink::Target { target }) => {
				let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
				let value = v8::String::new_external_onebyte_static(scope, b"symlink").unwrap();
				object.set(scope, key.into(), value.into());

				let key = v8::String::new_external_onebyte_static(scope, b"target").unwrap();
				let value = target.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::graph::Node::Symlink(tg::graph::object::Symlink::Artifact {
				artifact,
				subpath,
			}) => {
				let key = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
				let value = v8::String::new_external_onebyte_static(scope, b"symlink").unwrap();
				object.set(scope, key.into(), value.into());

				let key = v8::String::new_external_onebyte_static(scope, b"artifact").unwrap();
				let value = artifact.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key = v8::String::new_external_onebyte_static(scope, b"subpath").unwrap();
				let value = subpath.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
		}

		Ok(object.into())
	}
}

impl FromV8 for tg::graph::Node {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_object() {
			return Err(tg::error!("expected an object"));
		}
		let value = value.to_object(scope).unwrap();

		let kind = v8::String::new_external_onebyte_static(scope, b"kind").unwrap();
		let kind = value.get(scope, kind.into()).unwrap();
		let kind = <_>::from_v8(scope, kind)
			.map_err(|source| tg::error!(!source, "failed to deserialize the kind"))?;

		match kind {
			tg::graph::object::Kind::Directory => {
				let entries = v8::String::new_external_onebyte_static(scope, b"entries").unwrap();
				let entries = value.get(scope, entries.into()).unwrap();
				let entries = <_>::from_v8(scope, entries)
					.map_err(|source| tg::error!(!source, "failed to deserialize the entries"))?;

				Ok(Self::Directory(tg::graph::object::Directory { entries }))
			},

			tg::graph::object::Kind::File => {
				let contents = v8::String::new_external_onebyte_static(scope, b"contents").unwrap();
				let contents = value.get(scope, contents.into()).unwrap();
				let contents = <_>::from_v8(scope, contents)
					.map_err(|source| tg::error!(!source, "failed to deserialize the contents"))?;

				let dependencies =
					v8::String::new_external_onebyte_static(scope, b"dependencies").unwrap();
				let dependencies = value.get(scope, dependencies.into()).unwrap();
				let dependencies = <_>::from_v8(scope, dependencies).map_err(|source| {
					tg::error!(!source, "failed to deserialize the dependencies")
				})?;

				let executable =
					v8::String::new_external_onebyte_static(scope, b"executable").unwrap();
				let executable = value.get(scope, executable.into()).unwrap();
				let executable = <_>::from_v8(scope, executable).map_err(|source| {
					tg::error!(!source, "failed to deserialize the executable")
				})?;

				Ok(Self::File(tg::graph::object::File {
					contents,
					dependencies,
					executable,
				}))
			},

			tg::graph::object::Kind::Symlink => {
				let target = v8::String::new_external_onebyte_static(scope, b"target").unwrap();
				let target = value.get(scope, target.into()).unwrap();
				let target = <_>::from_v8(scope, target)
					.map_err(|source| tg::error!(!source, "failed to deserialize the target"))?;
				if let Some(target) = target {
					return Ok(Self::Symlink(tg::graph::object::Symlink::Target { target }));
				}

				let artifact = v8::String::new_external_onebyte_static(scope, b"artifact").unwrap();
				let artifact = value.get(scope, artifact.into()).unwrap();
				let artifact = <_>::from_v8(scope, artifact)
					.map_err(|source| tg::error!(!source, "failed to deserialize the artifact"))?;

				let subpath = v8::String::new_external_onebyte_static(scope, b"subpath").unwrap();
				let subpath = value.get(scope, subpath.into()).unwrap();
				let subpath = <_>::from_v8(scope, subpath)
					.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

				Ok(Self::Symlink(tg::graph::object::Symlink::Artifact {
					artifact,
					subpath,
				}))
			},
		}
	}
}

impl ToV8 for tg::graph::object::Kind {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::graph::object::Kind {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}
