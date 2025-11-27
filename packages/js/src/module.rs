use {
	super::{Module, State, error},
	num::ToPrimitive as _,
	sourcemap::SourceMap,
	std::{collections::BTreeMap, rc::Rc},
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

	// Determine the referrer and import.
	let (referrer, import) = if specifier.to_rust_string_lossy(scope) == "!" {
		// Root module.
		(None, None)
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

		(Some(referrer), Some(import))
	};

	// Resolve the module.
	let promise = state.create_promise(scope, {
		let handle = state.handle.clone();
		let root = state.root.clone();
		async move {
			let module = if let (Some(referrer), Some(import)) = (referrer, import) {
				let arg = tg::module::resolve::Arg {
					referrer: referrer.clone(),
					import: import.clone(),
				};
				let output = handle.resolve_module(arg).await.map_err(|source| {
					tg::error!(!source, ?referrer, ?import, "failed to resolve the module")
				})?;
				output.module
			} else {
				root
			};
			Ok(Serde(module))
		}
	});

	// Load the module if necessary.
	let handler = v8::Function::builder(
		|scope: &mut v8::HandleScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			let context = scope.get_current_context();
			let state = context.get_slot::<Rc<State>>().unwrap().clone();

			// Deserialize the module.
			let value: v8::Local<v8::Value> = unsafe { std::mem::transmute(args.get(0)) };
			let Serde(module) = Serde::<tg::module::Data>::deserialize(scope, value).unwrap();

			// Check if the module already exists.
			let index = state
				.modules
				.borrow()
				.iter()
				.position(|m| m.module == module);

			if let Some(index) = index {
				let index = v8::Integer::new(scope, index.to_i32().unwrap());
				return_value.set(index.into());
			} else {
				let promise = state.create_promise(scope, {
					let handle = state.handle.clone();
					async move {
						let arg = tg::module::load::Arg {
							module: module.clone(),
						};
						let output = handle.load_module(arg).await.map_err(|source| {
							tg::error!(!source, ?module, "failed to load the module")
						})?;
						Ok(Serde((module, output.text)))
					}
				});
				return_value.set(promise.into());
			}
		},
	)
	.build(scope)
	.unwrap();
	let promise = promise.then(scope, handler).unwrap();

	// Compile, instantiate, and evaluate the module.
	let handler = v8::Function::builder(
		|scope: &mut v8::HandleScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			let arg = args.get(0);
			let context = scope.get_current_context();
			let state = context.get_slot::<Rc<State>>().unwrap().clone();

			let v8_module = if let Ok(index) = v8::Local::<v8::Integer>::try_from(arg) {
				let index = index.value().to_usize().unwrap();
				let modules = state.modules.borrow();
				let module = &modules[index];
				let v8_module = module.v8.as_ref().unwrap();
				v8::Local::new(scope, v8_module)
			} else {
				let value: v8::Local<v8::Value> = unsafe { std::mem::transmute(arg) };
				let Serde((module, text)) =
					Serde::<(tg::module::Data, String)>::deserialize(scope, value).unwrap();

				// Compile the module.
				let Some(v8_module) = compile_module(scope, &module, &text) else {
					return;
				};

				// Instantiate the module.
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
		|scope: &mut v8::HandleScope,
		 args: v8::FunctionCallbackArguments,
		 mut return_value: v8::ReturnValue| {
			let array = v8::Local::<v8::Array>::try_from(args.get(0)).unwrap();
			let promise = array.get_index(scope, 0).unwrap();
			let promise = v8::Local::<v8::Promise>::try_from(promise).unwrap();
			let namespace = array.get_index(scope, 1).unwrap();
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
		let module = compile_module(scope, &module, &text)?;

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
		let handle = state.handle.clone();
		let referrer = referrer.clone();
		let import = import.clone();
		async move {
			let arg = tg::module::resolve::Arg { referrer, import };
			let result = handle.resolve_module(arg).await.map(|output| output.module);
			sender.send(result).unwrap();
		}
	});
	let result = receiver
		.recv()
		.unwrap()
		.map_err(|source| tg::error!(!source, ?referrer, ?import, "failed to resolve"));
	let module = match result {
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
		let handle = state.handle.clone();
		let module = module.clone();
		async move {
			let arg = tg::module::load::Arg { module };
			let result = handle.load_module(arg).await.map(|output| output.text);
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
	text: &str,
) -> Option<v8::Local<'s, v8::Module>> {
	// Get the context and state.
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Transpile the module.
	let output = tangram_compiler::Compiler::transpile(text, module);
	if !output.diagnostics.is_empty() {
		let diagnostics = output
			.diagnostics
			.into_iter()
			.filter_map(|diagnostic| diagnostic.try_into().ok())
			.collect();
		let error = tg::error!(diagnostics = diagnostics, "failed to transpile the module");
		let exception = error::to_exception(scope, &error)?;
		scope.throw_exception(exception);
		return None;
	}

	// Parse the source map.
	let source_map = match SourceMap::from_slice(output.source_map.as_bytes())
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
	let source = v8::String::new(scope, &output.text).unwrap();
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
