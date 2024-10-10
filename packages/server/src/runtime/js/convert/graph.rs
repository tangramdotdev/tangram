use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Graph {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, "Graph".as_bytes()).unwrap();
		let graph = tg.get(scope, graph.into()).unwrap();
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
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, "Graph".as_bytes()).unwrap();
		let graph = tg.get(scope, graph.into()).unwrap();
		let graph = v8::Local::<v8::Function>::try_from(graph).unwrap();

		if !value.instance_of(scope, graph.into()).unwrap() {
			return Err(tg::error!("expected a graph"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
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

		let key = v8::String::new_external_onebyte_static(scope, "nodes".as_bytes()).unwrap();
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
		let value = value.to_object(scope).unwrap();

		let nodes = v8::String::new_external_onebyte_static(scope, "nodes".as_bytes()).unwrap();
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
			tg::graph::Node::Directory(tg::graph::node::Directory { entries }) => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "directory".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());

				let key =
					v8::String::new_external_onebyte_static(scope, "entries".as_bytes()).unwrap();
				let value = entries.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::graph::Node::File(tg::graph::node::File {
				contents,
				dependencies,
				executable,
			}) => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "file".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());

				let key =
					v8::String::new_external_onebyte_static(scope, "contents".as_bytes()).unwrap();
				let value = contents.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key = v8::String::new_external_onebyte_static(scope, "dependencies".as_bytes())
					.unwrap();

				let value = dependencies.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key = v8::String::new_external_onebyte_static(scope, "executable".as_bytes())
					.unwrap();
				let value = executable.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::graph::Node::Symlink(tg::graph::node::Symlink { artifact, path }) => {
				let key =
					v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
				let value =
					v8::String::new_external_onebyte_static(scope, "symlink".as_bytes()).unwrap();
				object.set(scope, key.into(), value.into());

				let key =
					v8::String::new_external_onebyte_static(scope, "artifact".as_bytes()).unwrap();
				let value = artifact.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key =
					v8::String::new_external_onebyte_static(scope, "path".as_bytes()).unwrap();
				let value = path.to_v8(scope)?;
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
		let value = value.to_object(scope).unwrap();

		let kind = v8::String::new_external_onebyte_static(scope, "kind".as_bytes()).unwrap();
		let kind = value.get(scope, kind.into()).unwrap();
		let kind = <_>::from_v8(scope, kind)
			.map_err(|source| tg::error!(!source, "failed to deserialize the kind"))?;

		match kind {
			tg::graph::node::Kind::Directory => {
				let entries =
					v8::String::new_external_onebyte_static(scope, "entries".as_bytes()).unwrap();
				let entries = value.get(scope, entries.into()).unwrap();
				let entries = <_>::from_v8(scope, entries)
					.map_err(|source| tg::error!(!source, "failed to deserialize the entries"))?;

				Ok(Self::Directory(tg::graph::node::Directory { entries }))
			},

			tg::graph::node::Kind::File => {
				let contents =
					v8::String::new_external_onebyte_static(scope, "contents".as_bytes()).unwrap();
				let contents = value.get(scope, contents.into()).unwrap();
				let contents = <_>::from_v8(scope, contents)
					.map_err(|source| tg::error!(!source, "failed to deserialize the contents"))?;

				let dependencies =
					v8::String::new_external_onebyte_static(scope, "dependencies".as_bytes())
						.unwrap();
				let dependencies = value.get(scope, dependencies.into()).unwrap();
				let dependencies = <_>::from_v8(scope, dependencies).map_err(|source| {
					tg::error!(!source, "failed to deserialize the dependencies")
				})?;

				let executable =
					v8::String::new_external_onebyte_static(scope, "executable".as_bytes())
						.unwrap();
				let executable = value.get(scope, executable.into()).unwrap();
				let executable = <_>::from_v8(scope, executable).map_err(|source| {
					tg::error!(!source, "failed to deserialize the executable")
				})?;

				Ok(Self::File(tg::graph::node::File {
					contents,
					dependencies,
					executable,
				}))
			},

			tg::graph::node::Kind::Symlink => {
				let artifact =
					v8::String::new_external_onebyte_static(scope, "artifact".as_bytes()).unwrap();
				let artifact = value.get(scope, artifact.into()).unwrap();
				let artifact = <_>::from_v8(scope, artifact)
					.map_err(|source| tg::error!(!source, "failed to deserialize the artifact"))?;

				let path =
					v8::String::new_external_onebyte_static(scope, "path".as_bytes()).unwrap();
				let path = value.get(scope, path.into()).unwrap();
				let path = <_>::from_v8(scope, path)
					.map_err(|source| tg::error!(!source, "failed to deserialize the path"))?;

				Ok(Self::Symlink(tg::graph::node::Symlink { artifact, path }))
			},
		}
	}
}

impl ToV8 for tg::graph::node::Dependency {
	fn to_v8<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tangram_client::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);
		let key = v8::String::new_external_onebyte_static(scope, "object".as_bytes()).unwrap();
		let value = self.object.to_v8(scope)?;
		object.set(scope, key.into(), value);

		if let Some(tag) = &self.tag {
			let key = v8::String::new_external_onebyte_static(scope, "tag".as_bytes()).unwrap();
			let value = tag.to_string().to_v8(scope)?;
			object.set(scope, key.into(), value);
		}

		Ok(value)
	}
}

impl FromV8 for tg::graph::node::Dependency {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tangram_client::Result<Self> {
		let value = value.to_object(scope).unwrap();
		let object = v8::String::new_external_onebyte_static(scope, "object".as_bytes()).unwrap();
		let object = value.get(scope, object.into()).unwrap();
		let object = <_>::from_v8(scope, object)
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

		let tag = v8::String::new_external_onebyte_static(scope, "tag".as_bytes()).unwrap();
		let tag = value.get(scope, tag.into()).unwrap();
		let tag: Option<String> = <_>::from_v8(scope, tag)
			.map_err(|source| tg::error!(!source, "failed to deserialize the graph"))?;
		let tag = tag
			.map(|tag| tag.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse tag"))?;
		Ok(Self { object, tag })
	}
}

impl ToV8 for tg::graph::node::Kind {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::graph::node::Kind {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}
