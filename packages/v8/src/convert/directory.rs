use super::{FromV8, ToV8};
use tangram_client as tg;

impl ToV8 for tg::Directory {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let context = scope.get_current_context();
		let global = context.global(scope);
		let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
		let tangram = global.get(scope, tangram.into()).unwrap();
		let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

		let directory =
			v8::String::new_external_onebyte_static(scope, "Directory".as_bytes()).unwrap();
		let directory = tangram.get(scope, directory.into()).unwrap();
		let directory = v8::Local::<v8::Function>::try_from(directory).unwrap();

		let state = self.state().read().unwrap().to_v8(scope)?;

		let instance = directory
			.new_instance(scope, &[state])
			.ok_or_else(|| tg::error!("the constructor failed"))?;

		Ok(instance.into())
	}
}

impl FromV8 for tg::Directory {
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

		if !value.instance_of(scope, directory.into()).unwrap() {
			return Err(tg::error!("expected a directory"));
		}
		let value = value.to_object(scope).unwrap();

		let state = v8::String::new_external_onebyte_static(scope, "state".as_bytes()).unwrap();
		let state = value.get(scope, state.into()).unwrap();
		let state = <_>::from_v8(scope, state)
			.map_err(|source| tg::error!(!source, "failed to deserialize the state"))?;

		Ok(Self::with_state(state))
	}
}

impl ToV8 for tg::directory::Id {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.to_string().to_v8(scope)
	}
}

impl FromV8 for tg::directory::Id {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		String::from_v8(scope, value)?.parse()
	}
}

impl ToV8 for tg::directory::Object {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let object = v8::Object::new(scope);

		match self {
			tg::directory::Object::Normal { entries } => {
				let key =
					v8::String::new_external_onebyte_static(scope, "entries".as_bytes()).unwrap();
				let value = entries.to_v8(scope)?;
				object.set(scope, key.into(), value);
			},

			tg::directory::Object::Graph { graph, node } => {
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

impl FromV8 for tg::directory::Object {
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

		let entries = v8::String::new_external_onebyte_static(scope, "entries".as_bytes()).unwrap();
		let entries = value.get(scope, entries.into()).unwrap();
		let entries = <_>::from_v8(scope, entries)
			.map_err(|source| tg::error!(!source, "failed to deserialize the entries"))?;

		Ok(Self::Normal { entries })
	}
}
