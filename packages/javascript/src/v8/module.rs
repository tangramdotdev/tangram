use {
	super::{Module, State, error},
	data_encoding::BASE64,
	num::ToPrimitive as _,
	sourcemap::SourceMap,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum ImportKind {
	Static,
	Dynamic,
}

/// Implement V8's dynamic import callback.
pub fn host_import_module_dynamically_callback<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	_host_defined_options: v8::Local<'s, v8::Data>,
	resource_name: v8::Local<'s, v8::Value>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	// Get the referrer.
	let resource_name = resource_name
		.to_string(scope)
		.map(|resource_name| resource_name.to_rust_string_lossy(scope));
	let Some(resource_name) = resource_name else {
		let error = tg::error!("failed to get the resource name");
		let exception = error::to_exception(scope, &error)?;
		scope.throw_exception(exception);
		return None;
	};
	let referrer = if resource_name == "main" {
		None
	} else if resource_name.is_empty() {
		if !matches!(
			&state.arg.module.referent.node,
			tg::module::data::Source::Path(path) if path == std::path::Path::new("<repl>")
		) {
			let error = tg::error!("expected the REPL executable");
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		}
		Some(tg::module::Data {
			kind: tg::module::Kind::JavaScript,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				state.arg.cwd.join("<repl>.tg.js"),
			)),
		})
	} else {
		let module: tg::module::Data = match resource_name
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the resource name"))
		{
			Ok(module) => module,
			Err(error) => {
				let exception = error::to_exception(scope, &error)?;
				scope.throw_exception(exception);
				return None;
			},
		};
		let module = state
			.module_index(&module)
			.map_or(module, |index| state.modules.borrow()[index].data.clone());
		Some(module)
	};

	// Parse the import.
	let import = parse_import(scope, specifier, attributes, ImportKind::Dynamic)?;

	// Preload the module on the main runtime, so that a connection the instance creates does not depend on this thread.
	let promise = state.create_promise(scope, {
		let arg = crate::module::Arg {
			graphs: state.graphs.clone(),
			import,
			instance: state.instance.clone(),
			known: state.preloaded.borrow().known(),
			referrer,
		};
		let state = state.clone();
		async move {
			let output = state
				.main_runtime_handle
				.spawn(crate::module::preload(arg))
				.await
				.map_err(|error| tg::error!(!error, "the module preload task failed"))??;
			state.preloaded.borrow_mut().extend(output.modules);
			Ok(Serde(output.module))
		}
	});

	// Compile, instantiate, and evaluate the module.
	let handler = v8::Function::builder(
		|scope: &mut v8::PinScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			let context = scope.get_current_context();
			let state = context.get_slot::<State>().unwrap().clone();

			// Deserialize the module.
			let value: v8::Local<v8::Value> = unsafe { std::mem::transmute(args.get(0)) };
			let Serde(module) = Serde::<tg::module::Data>::deserialize(scope, value).unwrap();

			// Get the module if it already exists. Otherwise, compile and instantiate it.
			let v8_module = if let Some(v8_module) = existing_module(scope, &state, &module) {
				v8_module
			} else {
				let Some(v8_module) = compile_preloaded_module(scope, &state, &module) else {
					return;
				};
				if v8_module
					.instantiate_module(scope, resolve_module_callback)
					.is_none()
				{
					return;
				}
				v8_module
			};

			// Evaluate the module.
			let Some(promise) = v8_module.evaluate(scope) else {
				return;
			};

			// Get the namespace.
			let namespace = v8_module.get_module_namespace();

			let result = v8::Array::new_with_elements(scope, &[promise, namespace]);
			return_value.set(result.into());
		},
	)
	.build(scope)
	.unwrap();
	let promise = promise.then(scope, handler).unwrap();

	// Await the evaluation and return the namespace.
	let handler = v8::Function::builder(
		|scope: &mut v8::PinScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			let array = v8::Local::<v8::Array>::try_from(args.get(0)).unwrap();
			let promise = array.get_index(scope, 0).unwrap();
			let promise = v8::Local::<v8::Promise>::try_from(promise).unwrap();
			let namespace = array.get_index(scope, 1).unwrap();
			let handler = v8::Function::builder(
				|_scope: &mut v8::PinScope,
				 args: v8::FunctionCallbackArguments,
				 mut return_value: v8::ReturnValue| {
					return_value.set(args.data());
				},
			)
			.data(namespace)
			.build(scope)
			.unwrap();
			let promise = promise.then(scope, handler).unwrap();
			return_value.set(promise.into());
		},
	)
	.build(scope)
	.unwrap();
	let promise = promise.then(scope, handler).unwrap();

	Some(promise)
}

/// Implement V8's module resolution callback.
fn resolve_module_callback<'s>(
	context: v8::Local<'s, v8::Context>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
	referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
	// Get a scope for the callback.
	v8::callback_scope!(unsafe scope, context);

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	// Get the module.
	let result = state
		.module_index_with_v8(referrer)
		.map(|index| state.modules.borrow()[index].data.clone())
		.ok_or_else(|| tg::error!("unable to find the module"));
	let module = match result {
		Ok(module) => module,
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		},
	};

	// Parse the import.
	let import = parse_import(scope, specifier, attributes, ImportKind::Static)?;

	// Get the module that the preload resolved the import to.
	let resolution = state.preloaded.borrow().resolution(Some(&module), &import);
	let Some(module) = resolution else {
		let error = tg::error!(
			import = ?import.without_token(),
			referrer = ?module.without_token(),
			"failed to find the preloaded import"
		);
		let exception = error::to_exception(scope, &error)?;
		scope.throw_exception(exception);
		return None;
	};

	// Get the module if it already exists. Otherwise, compile it.
	if let Some(v8_module) = existing_module(scope, &state, &module) {
		return Some(v8_module);
	}
	compile_preloaded_module(scope, &state, &module)
}

/// Get a compiled module, adding the tokens of the given module to it.
fn existing_module<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	state: &State,
	module: &tg::module::Data,
) -> Option<v8::Local<'s, v8::Module>> {
	let index = state.module_index(module)?;
	let mut modules = state.modules.borrow_mut();
	let entry = &mut modules[index];
	entry
		.data
		.referent
		.options
		.tokens
		.inherit(&module.referent.options.tokens);
	let v8_module = entry.v8.as_ref().unwrap().clone();
	Some(v8::Local::new(scope, v8_module))
}

/// Compile a preloaded module.
fn compile_preloaded_module<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	state: &State,
	module: &tg::module::Data,
) -> Option<v8::Local<'s, v8::Module>> {
	let entry = state.preloaded.borrow().entry(module);
	let Some(entry) = entry else {
		let error =
			tg::error!(module = ?module.without_token(), "failed to find the preloaded module");
		let exception = error::to_exception(scope, &error)?;
		scope.throw_exception(exception);
		return None;
	};
	compile_module(scope, &entry)
}

/// Compile a module.
fn compile_module<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	entry: &crate::module::Entry,
) -> Option<v8::Local<'s, v8::Module>> {
	// Get the context and state.
	let context = scope.get_current_context();
	let state = context.get_slot::<State>().unwrap().clone();
	let module = &entry.module;

	// Parse the source map.
	let source_map = match SourceMap::from_slice(entry.source_map.as_bytes())
		.map_err(|error| tg::error!(!error, "failed to parse the error map"))
	{
		Ok(source_map) => source_map,
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		},
	};

	// Set the module.
	let id = {
		let mut modules = state.modules.borrow_mut();
		let key = module.without_token().to_string();
		state.module_indices.borrow_mut().insert(key, modules.len());
		modules.push(Module {
			data: module.clone(),
			source_map: Some(source_map),
			v8: None,
		});
		modules.len()
	};

	// Define the module's origin.
	let resource_name = v8::String::new(scope, &module.without_token().to_string())
		.unwrap()
		.into();
	let resource_line_offset = 0;
	let resource_column_offset = 0;
	let resource_is_shared_cross_origin = false;
	let script_id = id.to_i32().unwrap();
	let source_map_url = if entry.transpiled {
		let source_map_url = format!(
			"data:application/json;charset=utf-8;base64,{}",
			BASE64.encode(entry.source_map.as_bytes())
		);
		v8::String::new(scope, &source_map_url).map(Into::into)
	} else {
		None
	};
	let resource_is_opaque = false;
	let is_wasm = false;
	let is_module = true;
	let host_defined_options = None;
	let origin = v8::ScriptOrigin::new(
		scope,
		resource_name,
		resource_line_offset,
		resource_column_offset,
		resource_is_shared_cross_origin,
		script_id,
		source_map_url,
		resource_is_opaque,
		is_wasm,
		is_module,
		host_defined_options,
	);

	// Compile the module.
	let source = v8::String::new(scope, &entry.text).unwrap();
	let mut source = v8::script_compiler::Source::new(source, Some(&origin));
	let module = v8::script_compiler::compile_module(scope, &mut source)?;
	let module_global = v8::Global::new(scope, module);

	// Update the module and record its index by its identity hash.
	state.modules.borrow_mut().get_mut(id - 1).unwrap().v8 = Some(module_global);
	state
		.module_indices_with_v8
		.borrow_mut()
		.entry(module.get_identity_hash())
		.or_default()
		.push(id - 1);

	Some(module)
}

fn parse_import<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
	kind: ImportKind,
) -> Option<tg::module::Import> {
	match parse_import_inner(scope, specifier, attributes, kind) {
		Ok(import) => Some(import),
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			None
		},
	}
}

fn parse_import_inner<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
	kind: ImportKind,
) -> tg::Result<tg::module::Import> {
	// Get the specifier.
	let specifier = specifier.to_rust_string_lossy(scope);

	// Get the attributes.
	let attributes = if attributes.length() > 0 {
		let mut map = BTreeMap::new();
		let mut i = 0;
		while i < attributes.length() {
			// Get the key.
			let key = attributes
				.get(scope, i)
				.ok_or_else(|| tg::error!("failed to get the key"))?;
			let key = v8::Local::<v8::Value>::try_from(key)
				.map_err(|error| tg::error!(!error, "failed to convert the key"))?;
			let key = key.to_rust_string_lossy(scope);
			i += 1;

			// Get the value.
			let value = attributes
				.get(scope, i)
				.ok_or_else(|| tg::error!(%key, "failed to get the attribute value"))?;
			let value = v8::Local::<v8::Value>::try_from(value)
				.map_err(|error| tg::error!(!error, "failed to convert the value"))?;
			let value = value.to_rust_string_lossy(scope);
			i += 1;

			// Static imports include the source offset in the attributes array. Skip it.
			if kind == ImportKind::Static {
				i += 1;
			}

			map.insert(key, value);
		}
		Some(map)
	} else {
		None
	};

	// Parse the import.
	let import = tg::module::Import::with_specifier_and_attributes(&specifier, attributes)?;

	Ok(import)
}

/// Implement V8's import.meta callback.
pub extern "C" fn host_initialize_import_meta_object_callback(
	context: v8::Local<v8::Context>,
	module: v8::Local<v8::Module>,
	meta: v8::Local<v8::Object>,
) {
	// Get the scope.
	v8::callback_scope!(unsafe scope, context);

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	// Get the module.
	let index = state.module_index_with_v8(module).unwrap();
	let module = state.modules.borrow()[index].data.clone();

	// Get the Tangram global.
	let context = scope.get_current_context();
	let global = context.global(scope);
	let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
	let tangram = global.get(scope, tangram.into()).unwrap();
	let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

	// Get the Module constructor.
	let module_constructor = v8::String::new_external_onebyte_static(scope, b"Module").unwrap();
	let module_constructor = tangram.get(scope, module_constructor.into()).unwrap();
	let module_constructor = v8::Local::<v8::Object>::try_from(module_constructor).unwrap();

	// Get the fromData method.
	let from_data = v8::String::new_external_onebyte_static(scope, b"fromData").unwrap();
	let from_data = module_constructor.get(scope, from_data.into()).unwrap();
	let from_data = v8::Local::<v8::Function>::try_from(from_data).unwrap();

	// Call fromData with the module data.
	let data = Serde(module).serialize(scope).unwrap();
	let undefined = v8::undefined(scope);
	let Some(value) = from_data.call(scope, undefined.into(), &[data]) else {
		return;
	};

	// Set import.meta.module.
	let key = v8::String::new_external_onebyte_static(scope, b"module").unwrap();
	meta.set(scope, key.into(), value).unwrap();
}
