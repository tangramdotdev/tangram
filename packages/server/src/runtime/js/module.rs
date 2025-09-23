use {
	super::{Module, State, error},
	crate::Server,
	num::ToPrimitive as _,
	sourcemap::SourceMap,
	std::{collections::BTreeMap, rc::Rc},
	tangram_client as tg,
	tangram_v8::{Serde, Serialize as _},
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum ImportKind {
	Static,
	Dynamic,
}

/// Implement V8's dynamic import callback.
pub fn host_import_module_dynamically_callback<'s>(
	scope: &mut v8::HandleScope<'s>,
	_host_defined_options: v8::Local<'s, v8::Data>,
	resource_name: v8::Local<'s, v8::Value>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Get the module.
	let module = if specifier.to_rust_string_lossy(scope) == "!" {
		Some(state.root.clone())
	} else {
		// Get the referrer's ID.
		let id = resource_name
			.to_integer(scope)
			.unwrap()
			.value()
			.to_usize()
			.unwrap();

		// Get the referrer.
		let referrer = state.modules.borrow().get(id - 1).unwrap().module.clone();

		// Parse the import.
		let import = parse_import(scope, specifier, attributes, ImportKind::Dynamic)?;

		// Resolve the module.
		let module = resolve_module_sync(scope, &referrer, &import)?;

		Some(module)
	}?;

	// Find a module with the same item if it already exists. Otherwise, load and compile the module.
	let option = state
		.modules
		.borrow()
		.iter()
		.find(|m| m.module == module)
		.cloned();
	let module = if let Some(module) = option {
		let module = v8::Local::new(scope, module.v8.as_ref().unwrap());
		Some(module)
	} else {
		// Load the module.
		let text = load_module_sync(scope, &module)?;

		// Compile the module.
		let module = compile_module(scope, &module, text)?;

		// Instantiate the module.
		module.instantiate_module(scope, resolve_module_callback)?;

		Some(module)
	}?;

	// Evaluate the module.
	let output = module.evaluate(scope)?;
	let output = v8::Local::<v8::Promise>::try_from(output).unwrap();

	// Get the module namespace.
	let namespace = module.get_module_namespace();
	let namespace = v8::Global::new(scope, namespace);
	let namespace = v8::Local::new(scope, namespace);

	// Create a promise that resolves to the module namespace when evaluation completes.
	let handler = v8::Function::builder(
		|_scope: &mut v8::HandleScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			return_value.set(args.data());
		},
	)
	.data(namespace)
	.build(scope)
	.unwrap();
	let promise = output.then(scope, handler).unwrap();

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
	let scope = unsafe { &mut v8::CallbackScope::new(context) };

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Get the module.
	let result = state
		.modules
		.borrow()
		.iter()
		.find(|module| module.v8.as_ref().unwrap() == &referrer)
		.cloned()
		.map(|module| module.module)
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

	// Resolve the module.
	let module = resolve_module_sync(scope, &module, &import)?;

	// Get the module if it already exists. Otherwise, load and compile it.
	let option = state
		.modules
		.borrow()
		.iter()
		.find(|m| m.module == module)
		.cloned();
	let module = if let Some(module) = option {
		let module = v8::Local::new(scope, module.v8.as_ref().unwrap());
		Some(module)
	} else {
		// Load the module.
		let text = load_module_sync(scope, &module)?;

		// Compile the module.
		let module = compile_module(scope, &module, text)?;

		Some(module)
	}?;

	Some(module)
}

/// Resolve a module synchronously.
fn resolve_module_sync(
	scope: &mut v8::HandleScope,
	referrer: &tg::module::Data,
	import: &tg::module::Import,
) -> Option<tg::module::Data> {
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>().unwrap().clone();
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let server = state.server.clone();
		let referrer = referrer.clone();
		let import = import.clone();
		async move {
			let result = server.resolve_module(&referrer, &import).await;
			sender.send(result).unwrap();
		}
	});
	let module = match receiver
		.recv()
		.unwrap()
		.map_err(|source| tg::error!(!source, ?referrer, ?import, "failed to resolve"))
	{
		Ok(module) => module,
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		},
	};
	Some(module)
}

// Load a module synchronously.
fn load_module_sync(scope: &mut v8::HandleScope, module: &tg::module::Data) -> Option<String> {
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>().unwrap().clone();
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let server = state.server.clone();
		let module = module.clone();
		async move {
			let result = server.load_module(&module).await;
			sender.send(result).unwrap();
		}
	});
	let result = receiver
		.recv()
		.unwrap()
		.map_err(|source| tg::error!(!source, ?module, "failed to load the module"));
	let text = match result {
		Ok(text) => text,
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		},
	};
	Some(text)
}

/// Compile a module.
fn compile_module<'s>(
	scope: &mut v8::HandleScope<'s>,
	module: &tg::module::Data,
	text: String,
) -> Option<v8::Local<'s, v8::Module>> {
	// Get the context and state.
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Transpile the module.
	let crate::module::transpile::Output {
		transpiled_text,
		source_map,
	} = match Server::transpile_module(text, module)
		.map_err(|source| tg::error!(!source, "failed to transpile the module"))
	{
		Ok(output) => output,
		Err(error) => {
			let exception = error::to_exception(scope, &error)?;
			scope.throw_exception(exception);
			return None;
		},
	};

	// Parse the source map.
	let source_map = match SourceMap::from_slice(source_map.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to parse the source map"))
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
		modules.push(Module {
			module: module.clone(),
			source_map: Some(source_map),
			v8: None,
		});
		modules.len()
	};

	// Define the module's origin.
	let resource_name = v8::Integer::new(scope, id.to_i32().unwrap()).into();
	let resource_line_offset = 0;
	let resource_column_offset = 0;
	let resource_is_shared_cross_origin = false;
	let script_id = id.to_i32().unwrap();
	let source_map_url = None;
	let resource_is_opaque = true;
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
	let source = v8::String::new(scope, &transpiled_text).unwrap();
	let mut source = v8::script_compiler::Source::new(source, Some(&origin));
	let module = v8::script_compiler::compile_module(scope, &mut source)?;
	let module_global = v8::Global::new(scope, module);

	// Update the module.
	state.modules.borrow_mut().get_mut(id - 1).unwrap().v8 = Some(module_global);

	Some(module)
}

fn parse_import<'s>(
	scope: &mut v8::HandleScope<'s>,
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
	scope: &mut v8::HandleScope<'s>,
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
				.map_err(|source| tg::error!(!source, "failed to convert the key"))?;
			let key = key.to_rust_string_lossy(scope);
			i += 1;

			// Get the value.
			let value = attributes
				.get(scope, i)
				.ok_or_else(|| tg::error!(%key, "failed to get the attribute value"))?;
			let value = v8::Local::<v8::Value>::try_from(value)
				.map_err(|source| tg::error!(!source, "failed to convert the value"))?;
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
	let scope = unsafe { &mut v8::CallbackScope::new(context) };

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Get the module.
	let module = state
		.modules
		.borrow()
		.iter()
		.find(|m| m.v8.as_ref().unwrap() == &module)
		.unwrap()
		.module
		.clone();

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
