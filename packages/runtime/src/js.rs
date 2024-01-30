use self::{
	convert::{from_v8, ToV8},
	syscall::syscall,
};
use futures::{future::LocalBoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use num::ToPrimitive;
use sourcemap::SourceMap;
use std::{
	cell::RefCell, collections::BTreeMap, future::poll_fn, num::NonZeroI32, rc::Rc, str::FromStr,
	task::Poll,
};
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

mod convert;
mod error;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runtime.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

struct State {
	build: tg::Build,
	futures: RefCell<Futures>,
	global_source_map: Option<SourceMap>,
	log_sender: RefCell<Option<tokio::sync::mpsc::UnboundedSender<String>>>,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<ModuleInfo>>,
	options: tg::build::Options,
	tg: Box<dyn tg::Handle>,
}

type Futures = FuturesUnordered<
	LocalBoxFuture<'static, (Result<Box<dyn ToV8>>, v8::Global<v8::PromiseResolver>)>,
>;

struct ModuleInfo {
	module: tangram_language::Module,
	source_map: Option<SourceMap>,
	metadata: Option<tg::package::Metadata>,
	v8_identity_hash: NonZeroI32,
	v8_module: v8::Global<v8::Module>,
}

/// Start a JS build.
pub async fn build(
	tg: &dyn tg::Handle,
	build: &tg::Build,
	options: &tg::build::Options,
	mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Option<tg::Value>> {
	// Create a channel to receive the isolate handle.
	let (isolate_handle_sender, isolate_handle_receiver) = tokio::sync::oneshot::channel();

	// Create a channel to receive the result.
	let (result_sender, result_receiver) = tokio::sync::oneshot::channel();

	let thread = std::thread::spawn({
		let tg = tg.clone_box();
		let build = build.clone();
		let options = options.clone();
		let stop = stop.clone();
		let main_runtime_handle = tokio::runtime::Handle::current();
		move || {
			let future = build_inner(
				tg.as_ref(),
				&build,
				options,
				stop,
				main_runtime_handle.clone(),
				isolate_handle_sender,
			);
			let result = main_runtime_handle.block_on(future);
			result_sender.send(result).ok();
		}
	});

	// Wait to receive the isolate handle.
	let isolate_handle = isolate_handle_receiver
		.await
		.wrap_err("Failed to receive the isolate handle.")?;

	// Wait for either the result or the stop signal to be received.
	let value = tokio::select! {
		result = result_receiver => {
			result
				.wrap_err("Failed to receive the result.")?
				.wrap_err("The build failed.")?
		}

		() = stop.wait_for(|stop| *stop).map(|_| ()) => {
			isolate_handle.terminate_execution();
			thread
				.join()
				.map_err(|_| error!("Failed to join the thread."))?;
			return Ok(None);
		}
	};

	// Join the thread.
	thread
		.join()
		.map_err(|_| error!("Failed to join the thread."))?;

	Ok(value)
}

#[allow(clippy::too_many_lines)]
async fn build_inner(
	tg: &dyn tg::Handle,
	build: &tg::Build,
	options: tg::build::Options,
	mut stop: tokio::sync::watch::Receiver<bool>,
	main_runtime_handle: tokio::runtime::Handle,
	isolate_handle_sender: tokio::sync::oneshot::Sender<v8::IsolateHandle>,
) -> Result<Option<tg::Value>> {
	// Create the isolate params.
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT);

	// Create the isolate.
	let mut isolate = v8::Isolate::new(params);

	// Set the host import module dynamically callback.
	isolate.set_host_import_module_dynamically_callback(host_import_module_dynamically_callback);

	// Set the host initialize import meta object callback.
	isolate.set_host_initialize_import_meta_object_callback(
		host_initialize_import_meta_object_callback,
	);

	// Send the isolate handle.
	isolate_handle_sender
		.send(isolate.thread_safe_handle())
		.unwrap();

	// Get the target.
	let target = build.target(tg).await?;

	// Start the log task.
	let (log_sender, mut log_receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
	let log_task = tokio::spawn({
		let build = build.clone();
		let tg = tg.clone_box();
		async move {
			while let Some(string) = log_receiver.recv().await {
				build.add_log(tg.as_ref(), string.into()).await.ok();
			}
		}
	});

	// Create the state.
	let state = Rc::new(State {
		build: build.clone(),
		futures: RefCell::new(FuturesUnordered::new()),
		global_source_map: Some(SourceMap::from_slice(SOURCE_MAP).unwrap()),
		log_sender: RefCell::new(Some(log_sender)),
		options,
		main_runtime_handle,
		modules: RefCell::new(Vec::new()),
		tg: tg.clone_box(),
	});

	// Create the context.
	let context = {
		// Create and enter the context.
		let scope = &mut v8::HandleScope::new(&mut isolate);
		let context = v8::Context::new(scope);
		let scope = &mut v8::ContextScope::new(scope, context);

		// Set the state on the context.
		context.set_slot(scope, state.clone());

		// Create the syscall function.
		let syscall_string =
			v8::String::new_external_onebyte_static(scope, "syscall".as_bytes()).unwrap();
		let syscall = v8::Function::new(scope, syscall).unwrap();
		let global = context.global(scope);
		global
			.set(scope, syscall_string.into(), syscall.into())
			.unwrap();

		v8::Global::new(scope, context)
	};

	let value = {
		// Enter the context.
		let scope = &mut v8::HandleScope::new(&mut isolate);
		let context = v8::Local::new(scope, context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);

		// Get the tg global.
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		// Get the start function.
		let start = v8::String::new_external_onebyte_static(scope, "start".as_bytes()).unwrap();
		let start = tg.get(scope, start.into()).unwrap();
		let start = v8::Local::<v8::Function>::try_from(start).unwrap();

		// Call the start function.
		let undefined = v8::undefined(scope);
		let target = target.to_v8(scope).unwrap();
		let value = start.call(scope, undefined.into(), &[target]).unwrap();

		v8::Global::new(scope, value)
	};

	// Await the output.
	let stop = stop.changed();
	tokio::pin!(stop);
	let value = poll_fn(|cx| {
		loop {
			if stop.poll_unpin(cx).is_ready() {
				return Poll::Ready(Ok(None));
			}

			// Poll the futures.
			let (result, promise_resolver) = match state.futures.borrow_mut().poll_next_unpin(cx) {
				// If there is a result, then resolve or reject the promise.
				Poll::Ready(Some((result, promise_resolver))) => (result, promise_resolver),

				// If there are no more results, then break.
				Poll::Ready(None) => break,

				// If the futures are not ready, then return pending.
				Poll::Pending => return Poll::Pending,
			};

			// Enter the context.
			let scope = &mut v8::HandleScope::new(&mut isolate);
			let context = v8::Local::new(scope, context.clone());
			let scope = &mut v8::ContextScope::new(scope, context);

			// Resolve or reject the promise.
			let promise_resolver = v8::Local::new(scope, promise_resolver);
			match result.and_then(|value| value.to_v8(scope)) {
				Ok(value) => {
					// Resolve the promise.
					promise_resolver.resolve(scope, value);
				},
				Err(error) => {
					// Reject the promise.
					let exception = error::to_exception(scope, &error);
					promise_resolver.reject(scope, exception);
				},
			};
		}

		// Get the result from the value.
		let scope = &mut v8::HandleScope::new(&mut isolate);
		let context = v8::Local::new(scope, context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);
		let value = v8::Local::new(scope, value.clone());
		let result = if let Ok(promise) = v8::Local::<v8::Promise>::try_from(value) {
			// If the output is a promise, check its state.
			match promise.state() {
				// If the promise is fulfilled, then return the result.
				v8::PromiseState::Fulfilled => {
					let output = promise.result(scope);
					let output = match from_v8(scope, output) {
						Ok(output) => output,
						Err(error) => {
							return Poll::Ready(Err(error));
						},
					};
					Ok(Some(output))
				},

				// If the promise is rejected, then return the error.
				v8::PromiseState::Rejected => {
					let exception = promise.result(scope);
					let state = state.clone();
					let error = self::error::from_exception(&state, scope, exception);
					Err(error)
				},

				// At this point, the promise must not be pending.
				v8::PromiseState::Pending => unreachable!(),
			}
		} else {
			// If the output is not a promise, then return it.
			let output: tg::Value = match from_v8(scope, value) {
				Ok(output) => output,
				Err(error) => {
					return Poll::Ready(Err(error));
				},
			};
			Ok(Some(output))
		};

		Poll::Ready(result)
	})
	.await?;

	// Wait for the log task to complete.
	state.log_sender.borrow_mut().take();
	log_task.await.wrap_err("Failed to join the log task.")?;

	Ok(value)
}

/// Implement V8's dynamic import callback.
fn host_import_module_dynamically_callback<'s>(
	scope: &mut v8::HandleScope<'s>,
	_host_defined_options: v8::Local<'s, v8::Data>,
	resource_name: v8::Local<'s, v8::Value>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
	// Get the resource name.
	let resource_name = resource_name.to_string(scope).unwrap();
	let resource_name = resource_name.to_rust_string_lossy(scope);

	// Get the module.
	let module = if resource_name == "[runtime]" {
		let module = specifier.to_rust_string_lossy(scope);
		match tangram_language::Module::from_str(&module) {
			Ok(module) => module,
			Err(error) => {
				let exception = error::to_exception(scope, &error);
				scope.throw_exception(exception);
				return None;
			},
		}
	} else {
		// Get the module.
		let module = match tangram_language::Module::from_str(&resource_name) {
			Ok(module) => module,
			Err(error) => {
				let exception = error::to_exception(scope, &error);
				scope.throw_exception(exception);
				return None;
			},
		};

		// Parse the import.
		let Some(import) = parse_import(scope, specifier, attributes) else {
			return None;
		};

		match resolve_module(scope, &module, &import) {
			Some(module) => module,
			None => {
				return None;
			},
		}
	};

	// Load the module.
	let Some(module) = load_module(scope, &module) else {
		return None;
	};

	// Instantiate the module.
	module.instantiate_module(scope, resolve_module_callback)?;

	// Evaluate the module.
	let Some(output) = module.evaluate(scope) else {
		return None;
	};

	let promise = v8::Local::<v8::Promise>::try_from(output).unwrap();

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
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Get the module.
	let identity_hash = referrer.get_identity_hash();
	let module = match state
		.modules
		.borrow()
		.iter()
		.find(|module| module.v8_identity_hash == identity_hash)
		.map(|module| module.module.clone())
		.wrap_err_with(|| {
			format!(r#"Unable to find the module with identity hash "{identity_hash}"."#)
		}) {
		Ok(module) => module,
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			return None;
		},
	};

	// Parse the import.
	let Some(import) = parse_import(scope, specifier, attributes) else {
		return None;
	};

	// Resolve the module.
	let Some(module) = resolve_module(scope, &module, &import) else {
		return None;
	};

	// Load the module.
	let Some(module) = load_module(scope, &module) else {
		return None;
	};

	Some(module)
}

/// Resolve a module.
fn resolve_module(
	scope: &mut v8::HandleScope,
	module: &tangram_language::Module,
	import: &tangram_language::Import,
) -> Option<tangram_language::Module> {
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let tg = state.tg.clone_box();
		let module = module.clone();
		let import = import.clone();
		async move {
			let module = module.resolve(tg.as_ref(), None, &import).await;
			sender.send(module).unwrap();
		}
	});

	let module = match receiver
		.recv()
		.unwrap()
		.wrap_err_with(|| format!(r#"Failed to resolve "{import}" relative to "{module}"."#))
	{
		Ok(module) => module,
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			return None;
		},
	};
	Some(module)
}

/// Load a module.
#[allow(clippy::too_many_lines)]
fn load_module<'s>(
	scope: &mut v8::HandleScope<'s>,
	module: &tangram_language::Module,
) -> Option<v8::Local<'s, v8::Module>> {
	// Get the context and state.
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Return a cached module if this module has already been loaded.
	if let Some(module) = state
		.modules
		.borrow()
		.iter()
		.find(|cached_module| &cached_module.module == module)
	{
		let module = v8::Local::new(scope, &module.v8_module);
		return Some(module);
	}

	// Define the module's origin.
	let resource_name = v8::String::new(scope, &module.to_string()).unwrap();
	let resource_line_offset = 0;
	let resource_column_offset = 0;
	let resource_is_shared_cross_origin = false;
	let script_id = state.modules.borrow().len().to_i32().unwrap() + 1;
	let source_map_url = v8::undefined(scope).into();
	let resource_is_opaque = true;
	let is_wasm = false;
	let is_module = true;
	let origin = v8::ScriptOrigin::new(
		scope,
		resource_name.into(),
		resource_line_offset,
		resource_column_offset,
		resource_is_shared_cross_origin,
		script_id,
		source_map_url,
		resource_is_opaque,
		is_wasm,
		is_module,
	);

	// Load the module.
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let tg = state.tg.clone_box();
		let module = module.clone();
		async move {
			let result = module.load(tg.as_ref(), None).await;
			sender.send(result).unwrap();
		}
	});
	let text = match receiver
		.recv()
		.unwrap()
		.wrap_err_with(|| format!(r#"Failed to load module "{module}"."#))
	{
		Ok(text) => text,
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			return None;
		},
	};

	// Transpile the module.
	let tangram_language::transpile::Output {
		transpiled_text,
		source_map,
	} =
		match tangram_language::Module::transpile(text).wrap_err("Failed to transpile the module.")
		{
			Ok(output) => output,
			Err(error) => {
				let exception = error::to_exception(scope, &error);
				scope.throw_exception(exception);
				return None;
			},
		};

	// Parse the source map.
	let source_map = match SourceMap::from_slice(source_map.as_bytes())
		.wrap_err("Failed to parse the source map.")
	{
		Ok(source_map) => source_map,
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			return None;
		},
	};

	// Compile the module.
	let source = v8::String::new(scope, &transpiled_text).unwrap();
	let source = v8::script_compiler::Source::new(source, Some(&origin));
	let Some(v8_module) = v8::script_compiler::compile_module(scope, source) else {
		return None;
	};

	// Get the metadata.
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let tg = state.tg.clone_box();
		let module = module.clone();
		async move {
			let module = module.unwrap_normal_ref();
			let package = tg::Directory::with_id(module.package.clone());
			let metadata = tangram_package::get_metadata(tg.as_ref(), &package)
				.await
				.ok();
			sender.send(metadata).unwrap();
		}
	});
	let metadata = receiver.recv().unwrap();

	// Cache the module.
	state.modules.borrow_mut().push(ModuleInfo {
		module: module.clone(),
		metadata,
		source_map: Some(source_map),
		v8_identity_hash: v8_module.get_identity_hash(),
		v8_module: v8::Global::new(scope, v8_module),
	});

	Some(v8_module)
}

/// Implement V8's import.meta callback.
extern "C" fn host_initialize_import_meta_object_callback(
	context: v8::Local<v8::Context>,
	module: v8::Local<v8::Module>,
	meta: v8::Local<v8::Object>,
) {
	// Create the scope.
	let scope = unsafe { &mut v8::CallbackScope::new(context) };

	// Get the state.
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Get the module.
	let identity_hash = module.get_identity_hash();
	let module = state
		.modules
		.borrow()
		.iter()
		.find(|module| module.v8_identity_hash == identity_hash)
		.unwrap()
		.module
		.clone();

	// Set import.meta.url.
	let key = v8::String::new_external_onebyte_static(scope, "url".as_bytes()).unwrap();
	let value = v8::String::new(scope, &module.to_string()).unwrap();
	meta.set(scope, key.into(), value.into()).unwrap();
}

fn parse_import<'s>(
	scope: &mut v8::HandleScope<'s>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<tangram_language::Import> {
	match parse_import_inner(scope, specifier, attributes) {
		Ok(import) => Some(import),
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			None
		},
	}
}

fn parse_import_inner<'s>(
	scope: &mut v8::HandleScope<'s>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Result<tangram_language::Import> {
	// Get the specifier.
	let specifier = specifier.to_rust_string_lossy(scope);

	// Get the attributes.
	let attributes = if attributes.length() > 0 {
		let mut map = BTreeMap::default();
		let mut i = 0;
		while i < attributes.length() % 2 {
			let key = attributes
				.get(scope, i)
				.wrap_err("Failed to get the key.")?;
			let key =
				v8::Local::<v8::Value>::try_from(key).wrap_err("Failed to convert the key.")?;
			let key = key.to_rust_string_lossy(scope);
			i += 1;
			let value = attributes
				.get(scope, i)
				.wrap_err("Failed to get the value.")?;
			let value =
				v8::Local::<v8::Value>::try_from(value).wrap_err("Failed to convert the value.")?;
			let value = value.to_rust_string_lossy(scope);
			i += 1;
			map.insert(key, value);
		}
		Some(map)
	} else {
		None
	};

	// Parse the import.
	let import =
		tangram_language::Import::with_specifier_and_attributes(&specifier, attributes.as_ref())?;

	Ok(import)
}
