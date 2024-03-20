use self::{
	convert::{from_v8, ToV8},
	syscall::syscall,
};
use crate::Server;
use futures::{
	future::{self, LocalBoxFuture},
	stream::FuturesUnordered,
	FutureExt, StreamExt,
};
use num::ToPrimitive;
use sourcemap::SourceMap;
use std::{
	cell::RefCell, collections::BTreeMap, future::poll_fn, num::NonZeroI32, pin::pin, rc::Rc,
	str::FromStr, task::Poll,
};
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tokio::io::AsyncWriteExt;

mod convert;
mod error;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runtime.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runtime.js.map"));

struct State {
	build: tg::Build,
	futures: RefCell<Futures>,
	global_source_map: Option<SourceMap>,
	language_server: crate::language::Server,
	log_sender: RefCell<Option<tokio::sync::mpsc::UnboundedSender<String>>>,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	rejection: tokio::sync::watch::Sender<Option<Error>>,
	server: Server,
}

type Futures = FuturesUnordered<
	LocalBoxFuture<'static, (Result<Box<dyn ToV8>>, v8::Global<v8::PromiseResolver>)>,
>;

#[allow(clippy::struct_field_names)]
struct Module {
	module: tg::Module,
	source_map: Option<SourceMap>,
	metadata: Option<tg::package::Metadata>,
	v8_identity_hash: NonZeroI32,
	v8_module: v8::Global<v8::Module>,
}

/// Start a JS build.
pub async fn build(server: &Server, build: &tg::Build) -> Result<tg::Value> {
	// Create a handle to the main runtime.
	let main_runtime_handle = tokio::runtime::Handle::current();

	// Create a channel to receive the isolate handle.
	let (isolate_handle_sender, isolate_handle_receiver) = tokio::sync::watch::channel(None);

	// Spawn the task.
	let task = server.inner.local_pool_handle.spawn_pinned({
		let server = server.clone();
		let build = build.clone();
		move || async move {
			build_inner(
				&server,
				&build,
				main_runtime_handle.clone(),
				isolate_handle_sender,
			)
			.await
		}
	});
	let abort_handle = task.abort_handle();

	scopeguard::defer! {
		abort_handle.abort();
		if let Some(isolate_handle) = isolate_handle_receiver.borrow().as_ref() {
			isolate_handle.terminate_execution();
		}
	};

	task.await.unwrap()
}

async fn build_inner(
	server: &Server,
	build: &tg::Build,
	main_runtime_handle: tokio::runtime::Handle,
	isolate_handle_sender: tokio::sync::watch::Sender<Option<v8::IsolateHandle>>,
) -> Result<tg::Value> {
	// Get the target.
	let target = build.target(server).await?;

	// Start the log task.
	let (log_sender, mut log_receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
	let log_task = tokio::spawn({
		let build = build.clone();
		let server = server.clone();
		async move {
			while let Some(string) = log_receiver.recv().await {
				if server.inner.options.advanced.write_build_logs_to_stderr {
					tokio::io::stderr()
						.write_all(string.as_bytes())
						.await
						.inspect_err(|e| {
							tracing::error!(?e, "failed to write build log to stderr");
						})
						.ok();
				}
				build.add_log(&server, string.into()).await.ok();
			}
		}
	});

	// Create the state.
	let state = Rc::new(State {
		build: build.clone(),
		futures: RefCell::new(FuturesUnordered::new()),
		global_source_map: Some(SourceMap::from_slice(SOURCE_MAP).unwrap()),
		language_server: crate::language::Server::new(server, main_runtime_handle.clone()),
		log_sender: RefCell::new(Some(log_sender)),
		main_runtime_handle,
		modules: RefCell::new(Vec::new()),
		rejection: tokio::sync::watch::channel(None).0,
		server: server.clone(),
	});

	// Create the isolate params.
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT);

	// Create the isolate.
	let mut isolate = scopeguard::guard(v8::Isolate::new(params), |mut isolate| unsafe {
		isolate.enter();
	});
	unsafe { isolate.exit() };

	// Enter the isolate.
	unsafe { isolate.enter() };

	// Send the isolate handle.
	isolate_handle_sender.send_replace(Some(isolate.thread_safe_handle()));

	// Set the host import module dynamically callback.
	isolate.set_host_import_module_dynamically_callback(host_import_module_dynamically_callback);

	// Set the host initialize import meta object callback.
	isolate.set_host_initialize_import_meta_object_callback(
		host_initialize_import_meta_object_callback,
	);

	// Set the promise reject callback.
	isolate.set_promise_reject_callback(promise_reject_callback);

	// Create the context.
	let context = {
		// Create the context.
		let scope = &mut v8::HandleScope::new(isolate.as_mut());
		let context = v8::Context::new(scope);
		let scope = &mut v8::ContextScope::new(scope, context);

		// Set the state on the context.
		context.set_slot(scope, state.clone());

		// Create the syscall function.
		let syscall_string =
			v8::String::new_external_onebyte_static(scope, "syscall".as_bytes()).unwrap();
		let syscall = v8::Function::new(scope, syscall).unwrap();
		context
			.global(scope)
			.set(scope, syscall_string.into(), syscall.into())
			.unwrap();

		v8::Global::new(scope, context)
	};

	// Call the start function.
	let value = {
		// Create a scope for the context.
		let scope = &mut v8::HandleScope::new(isolate.as_mut());
		let context = v8::Local::new(scope, context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);

		// Get the tg global.
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = context.global(scope).get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

		// Get the start function.
		let start = v8::String::new_external_onebyte_static(scope, "start".as_bytes()).unwrap();
		let start = tg.get(scope, start.into()).unwrap();
		let start = v8::Local::<v8::Function>::try_from(start).unwrap();

		// Call the start function.
		let undefined = v8::undefined(scope);
		let target = target
			.to_v8(scope)
			.map_err(|error| error!(source = error, "failed to serialize the target"))?;
		let value = start.call(scope, undefined.into(), &[target]).unwrap();

		// Make the value global.
		v8::Global::new(scope, value)
	};

	// Exit the isolate.
	unsafe { isolate.exit() };

	// Run the event loop.
	let future = poll_fn(|cx| {
		loop {
			// Poll the futures.
			let mut futures = state.futures.borrow_mut();
			match futures.poll_next_unpin(cx) {
				// If the futures are not ready, then return pending.
				Poll::Pending => return Poll::Pending,

				// If there is a result, then resolve or reject the promise.
				Poll::Ready(Some((result, promise_resolver))) => {
					drop(futures);

					// Enter the isolate.
					unsafe { isolate.enter() };

					{
						// Create a scope for the context.
						let scope = &mut v8::HandleScope::new(isolate.as_mut());
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

					// Exit the isolate.
					unsafe { isolate.exit() };
				},

				// If there are no more results, then return the result.
				Poll::Ready(None) => {
					// Enter the isolate.
					unsafe { isolate.enter() };

					// Get the result.
					let result = {
						// Create a scope for the context.
						let scope = &mut v8::HandleScope::new(isolate.as_mut());
						let context = v8::Local::new(scope, context.clone());
						let scope = &mut v8::ContextScope::new(scope, context);

						// Make the value local.
						let value = v8::Local::new(scope, value.clone());

						// Get the result.
						match v8::Local::<v8::Promise>::try_from(value) {
							Err(_) => from_v8(scope, value),
							Ok(promise) => {
								match promise.state() {
									// If the promise is fulfilled, then return the result.
									v8::PromiseState::Fulfilled => {
										let value = promise.result(scope);
										from_v8(scope, value)
									},

									// If the promise is rejected, then return the error.
									v8::PromiseState::Rejected => {
										let exception = promise.result(scope);
										let error =
											self::error::from_exception(&state, scope, exception);
										Err(error)
									},

									// If the promise is pending, then execution was terminated.
									v8::PromiseState::Pending => {
										Err(error!("execution was terminated"))
									},
								}
							},
						}
					};

					// Exit the isolate.
					unsafe { isolate.exit() };

					return Poll::Ready(result);
				},
			};
		}
	});
	let mut rejection = state.rejection.subscribe();
	let rejection = rejection
		.wait_for(Option::is_some)
		.map(|result| result.map(|option| option.as_ref().unwrap().clone()));
	let result = match future::select(pin!(future), pin!(rejection)).await {
		future::Either::Left((result, _)) => result,
		future::Either::Right((result, _)) => {
			let error = result.unwrap();
			Err(error!(
				source = error,
				"an unhandled promise rejection occurred"
			))
		},
	};

	// Join the log task.
	state.log_sender.borrow_mut().take().unwrap();
	log_task
		.await
		.map_err(|error| error!(source = error, "failed to join the log task"))?;

	result
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
	let module = if resource_name == "[global]" {
		let module = specifier.to_rust_string_lossy(scope);
		match tg::Module::from_str(&module) {
			Ok(module) => module,
			Err(error) => {
				let exception = error::to_exception(scope, &error);
				scope.throw_exception(exception);
				return None;
			},
		}
	} else {
		// Get the module.
		let module = match tg::Module::from_str(&resource_name) {
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
		.ok_or_else(|| error!(%identity_hash, "unable to find the module with identity hash"))
	{
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
	module: &tg::Module,
	import: &tg::Import,
) -> Option<tg::Module> {
	let context = scope.get_current_context();
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let language_server = state.language_server.clone();
		let module = module.clone();
		let import = import.clone();
		async move {
			let module = language_server.resolve_module(&module, &import).await;
			sender.send(module).unwrap();
		}
	});

	let module = match receiver.recv().unwrap().map_err(
		|error| error!(source = error, %import, %module, "failed to resolve import relative to module"),
	) {
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
fn load_module<'s>(
	scope: &mut v8::HandleScope<'s>,
	module: &tg::Module,
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
		let language_server = state.language_server.clone();
		let module = module.clone();
		async move {
			let result = language_server.load_module(&module).await;
			sender.send(result).unwrap();
		}
	});
	let text = match receiver
		.recv()
		.unwrap()
		.map_err(|error| error!(source = error, %module, "failed to load module"))
	{
		Ok(text) => text,
		Err(error) => {
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
			return None;
		},
	};

	// Transpile the module.
	let crate::language::transpile::Output {
		transpiled_text,
		source_map,
	} = match crate::language::Server::transpile_module(text)
		.map_err(|error| error!(source = error, "failed to transpile the module"))
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
		.map_err(|error| error!(source = error, "failed to parse the source map"))
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
		let server = state.server.clone();
		let module = module.clone();
		async move {
			let module = module.unwrap_normal_ref();
			let package = tg::Directory::with_id(module.package.clone());
			let metadata = tg::package::get_metadata(&server, &package).await.ok();
			sender.send(metadata).unwrap();
		}
	});
	let metadata = receiver.recv().unwrap();

	// Cache the module.
	state.modules.borrow_mut().push(Module {
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
	// Get the scope.
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

/// Implement V8's promise rejection callback.
extern "C" fn promise_reject_callback(message: v8::PromiseRejectMessage) {
	// Get the scope.
	let scope = &mut unsafe { v8::CallbackScope::new(&message) };

	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	match message.get_event() {
		v8::PromiseRejectEvent::PromiseRejectWithNoHandler => {
			let exception = message.get_promise().result(scope);
			let error = error::from_exception(&state, scope, exception);
			state.rejection.send_replace(Some(error));
		},
		v8::PromiseRejectEvent::PromiseHandlerAddedAfterReject
		| v8::PromiseRejectEvent::PromiseRejectAfterResolved
		| v8::PromiseRejectEvent::PromiseResolveAfterResolved => {},
	}
}

fn parse_import<'s>(
	scope: &mut v8::HandleScope<'s>,
	specifier: v8::Local<'s, v8::String>,
	attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<tg::Import> {
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
) -> Result<tg::Import> {
	// Get the specifier.
	let specifier = specifier.to_rust_string_lossy(scope);

	// Get the attributes.
	let attributes = if attributes.length() > 0 {
		let mut map = BTreeMap::default();
		let mut i = 0;
		while i < attributes.length() % 2 {
			let key = attributes
				.get(scope, i)
				.ok_or_else(|| error!("failed to get the key"))?;
			let key = v8::Local::<v8::Value>::try_from(key)
				.map_err(|error| error!(source = error, "failed to convert the key"))?;
			let key = key.to_rust_string_lossy(scope);
			i += 1;
			let value = attributes
				.get(scope, i)
				.ok_or_else(|| error!("failed to get the value"))?;
			let value = v8::Local::<v8::Value>::try_from(value)
				.map_err(|error| error!(source = error, "failed to convert the value"))?;
			let value = value.to_rust_string_lossy(scope);
			i += 1;
			map.insert(key, value);
		}
		Some(map)
	} else {
		None
	};

	// Parse the import.
	let import = tg::Import::with_specifier_and_attributes(&specifier, attributes.as_ref())?;

	Ok(import)
}
