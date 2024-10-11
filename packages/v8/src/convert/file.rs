use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::File {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let file = v8::String::new_external_onebyte_static(scope, "File".as_bytes()).unwrap();
		let file = tangram.get(scope, file.into()).unwrap();
		let file = v8::Local::<v8::Function>::try_from(file).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = file
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::File {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let file = v8::String::new_external_onebyte_static(scope, "File".as_bytes()).unwrap();
		let file = tangram.get(scope, file.into()).unwrap();
		let file = v8::Local::<v8::Function>::try_from(file).unwrap();

		if !value.instance_of(scope, file.into()).unwrap() {
			return Err(tg::error!("expected a file"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::file::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::file::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::file::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		match self {
			tg::file::Object::Normal {
				contents,
				dependencies,
				executable,
			} => {
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

			tg::file::Object::Graph { graph, node } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "graph".as_bytes()).unwrap();
				let value = graph.to_v8(scope)?;
				object.set(scope, key.into(), value);

				let key =
					v8::String::new_external_onebyte_static(scope, "node".as_bytes()).unwrap();
				let value = node.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},
		}

		Ok(object.into())
	}
}

impl FromV8 for tg::file::Object {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = value.to_object(scope).unwrap();

		let graph = v8::String::new_external_onebyte_static(scope, "graph".as_bytes()).unwrap();
		let graph = value.get(scope, graph.into()).unwrap();
		let graph = <_>::from_v8(scope, graph)
			.map_err(|source| tg::error!(!source, "failed to deserialize the graph"))?;
		if let Some(graph) = graph {
			let node = v8::String::new_external_onebyte_static(scope, "node".as_bytes()).unwrap();
			let node = value.get(scope, node.into()).unwrap();
			let node = <_>::from_v8(scope, node)
				.map_err(|source| tg::error!(!source, "failed to deserialize the node"))?;
			return Ok(Self::Graph { graph, node });
		}

		let contents =
			v8::String::new_external_onebyte_static(scope, "contents".as_bytes()).unwrap();
		let contents = value.get(scope, contents.into()).unwrap();
		let contents = <_>::from_v8(scope, contents)
			.map_err(|source| tg::error!(!source, "failed to deserialize the contents"))?;

		let dependencies =
			v8::String::new_external_onebyte_static(scope, "dependencies".as_bytes()).unwrap();
		let dependencies = value.get(scope, dependencies.into()).unwrap();
		let dependencies = <_>::from_v8(scope, dependencies)
			.map_err(|source| tg::error!(!source, "failed to deserialize the dependencies"))?;

		let executable =
			v8::String::new_external_onebyte_static(scope, "executable".as_bytes()).unwrap();
		let executable = value.get(scope, executable.into()).unwrap();
		let executable = <_>::from_v8(scope, executable)
			.map_err(|source| tg::error!(!source, "failed to deserialize the executable"))?;

		Ok(Self::Normal {
			contents,
			dependencies,
			executable,
		})
	}
}

impl ToV8 for tg::file::Dependency {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
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

impl FromV8 for tg::file::Dependency {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
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
