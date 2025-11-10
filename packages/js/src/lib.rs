use {
	self::{
		module::{
			host_import_module_dynamically_callback, host_initialize_import_meta_object_callback,
		},
		syscall::syscall,
	},
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _,
		future::{self, BoxFuture, LocalBoxFuture},
		stream::FuturesUnordered,
	},
	sourcemap::SourceMap,
	std::{cell::RefCell, future::poll_fn, path::PathBuf, pin::pin, rc::Rc, sync::Arc, task::Poll},
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

mod error;
mod module;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

pub type Logger = Arc<
	dyn Fn(tg::process::log::Stream, String) -> BoxFuture<'static, tg::Result<()>>
		+ Send
		+ Sync
		+ 'static,
>;

struct State {
	process: Option<tg::Process>,
	promises: RefCell<FuturesUnordered<LocalBoxFuture<'static, PromiseOutput>>>,
	global_source_map: Option<SourceMap>,
	logger: Logger,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	rejection: tokio::sync::watch::Sender<Option<tg::Error>>,
	root: tg::module::Data,
	handle: tg::handle::dynamic::Handle,
}

struct PromiseOutput {
	resolver: v8::Global<v8::PromiseResolver>,
	result: tg::Result<Box<dyn tangram_v8::Serialize>>,
}

#[expect(clippy::struct_field_names)]
#[derive(Clone, Debug)]
struct Module {
	module: tg::module::Data,
	source_map: Option<SourceMap>,
	v8: Option<v8::Global<v8::Module>>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

#[allow(clippy::too_many_arguments)]
pub async fn run<H>(
	handle: &H,
	process: Option<&tg::Process>,
	args: tg::value::data::Array,
	cwd: PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
	logger: Logger,
	main_runtime_handle: tokio::runtime::Handle,
	isolate_handle_sender: Option<tokio::sync::watch::Sender<Option<v8::IsolateHandle>>>,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	// Extract the module from the executable.
	let module = if let tg::command::data::Executable::Module(executable) = &executable {
		executable.module.clone()
	} else {
		return Err(tg::error!("expected a module executable"));
	};

	// Create the state.
	let (rejection, _) = tokio::sync::watch::channel(None);
	let state = Rc::new(State {
		process: process.cloned(),
		promises: RefCell::new(FuturesUnordered::new()),
		global_source_map: Some(SourceMap::from_slice(SOURCE_MAP).unwrap()),
		logger,
		main_runtime_handle,
		modules: RefCell::new(Vec::new()),
		rejection,
		root: module.clone(),
		handle: tg::handle::dynamic::Handle::new(handle.clone()),
	});

	// Create the isolate params.
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT);

	// Create the isolate.
	let isolate = v8::Isolate::new(params);
	let mut isolate = scopeguard::guard(isolate, |mut isolate| unsafe {
		isolate.enter();
	});
	unsafe { isolate.exit() };

	// Enter the isolate.
	unsafe { isolate.enter() };

	// Send the isolate handle.
	if let Some(isolate_handle_sender) = isolate_handle_sender {
		isolate_handle_sender.send_replace(Some(isolate.thread_safe_handle()));
	}

	// Set the microtask policy.
	isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);

	// Set the host import module dynamically callback.
	isolate.set_host_import_module_dynamically_callback(host_import_module_dynamically_callback);

	// Set the host initialize import meta object callback.
	isolate.set_host_initialize_import_meta_object_callback(
		host_initialize_import_meta_object_callback,
	);

	// Set the prepare stack trace callback.
	isolate.set_prepare_stack_trace_callback(self::error::prepare_stack_trace_callback);

	// Set the promise reject callback.
	isolate.set_promise_reject_callback(promise_reject_callback);

	// Create the context.
	let context = {
		// Create the context.
		let scope = &mut v8::HandleScope::new(isolate.as_mut());
		let context = v8::Context::new(scope, v8::ContextOptions::default());
		let scope = &mut v8::ContextScope::new(scope, context);

		// Set the state on the context.
		context.set_slot(state.clone());

		// Create the syscall function.
		let syscall_string = v8::String::new_external_onebyte_static(scope, b"syscall").unwrap();
		let syscall = v8::Function::new(scope, syscall).unwrap();
		let syscall_descriptor = v8::PropertyDescriptor::new_from_value(syscall.into());
		context
			.global(scope)
			.define_property(scope, syscall_string.into(), &syscall_descriptor)
			.unwrap();

		v8::Global::new(scope, context)
	};

	// Call the start function.
	let value = {
		// Create a scope for the context.
		let scope = &mut v8::HandleScope::new(isolate.as_mut());
		let context = v8::Local::new(scope, context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);

		// Create the arg.
		let arg = v8::Object::new(scope);

		// Set args.
		let key = v8::String::new_external_onebyte_static(scope, b"args").unwrap();
		let value = Serde(&args).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set cwd.
		let key = v8::String::new_external_onebyte_static(scope, b"cwd").unwrap();
		let value =
			Serde(cwd.to_str().ok_or_else(|| tg::error!("invalid cwd"))?).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set env.
		let key = v8::String::new_external_onebyte_static(scope, b"env").unwrap();
		let value = Serde(&env).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set executable.
		let key = v8::String::new_external_onebyte_static(scope, b"executable").unwrap();
		let value = Serde(&executable).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Get the start function.
		let start = v8::String::new_external_onebyte_static(scope, b"start").unwrap();
		let start = context.global(scope).get(scope, start.into()).unwrap();
		let start = v8::Local::<v8::Function>::try_from(start).unwrap();

		// Call the start function.
		let scope = &mut v8::TryCatch::new(scope);
		let undefined = v8::undefined(scope);
		let value = start.call(scope, undefined.into(), &[arg.into()]);
		if scope.has_caught() {
			if !scope.can_continue() {
				if scope.has_terminated() {
					unsafe { scope.exit() };
					return Err(tg::error!("execution terminated"));
				}
				unsafe { scope.exit() };
				return Err(tg::error!("unrecoverable error"));
			}
			let exception = scope.exception().unwrap();
			let error = self::error::from_exception(&state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			unsafe { scope.exit() };
			return Err(error);
		}
		let value = value.unwrap();

		// Make the value global.
		v8::Global::new(scope, value)
	};

	// Exit the isolate.
	unsafe { isolate.exit() };

	// Run the event loop.
	let future = poll_fn(|cx| {
		loop {
			// Poll the promises.
			let poll = state.promises.borrow_mut().poll_next_unpin(cx);

			match poll {
				// If no promises are ready, then return pending.
				Poll::Pending => {
					return Poll::Pending;
				},

				// If there is a promise to fulfill, then resolve or reject it and run microtasks.
				Poll::Ready(Some(output)) => {
					let PromiseOutput {
						resolver: promise_resolver,
						result,
					} = output;

					// Enter the isolate.
					unsafe { isolate.enter() };

					{
						// Create a scope for the context.
						let scope = &mut v8::HandleScope::new(isolate.as_mut());
						let context = v8::Local::new(scope, context.clone());
						let scope = &mut v8::ContextScope::new(scope, context);
						let scope = &mut v8::TryCatch::new(scope);

						// Resolve or reject the promise.
						let promise_resolver = v8::Local::new(scope, promise_resolver);
						match result.and_then(|value| value.serialize(scope)) {
							Ok(value) => {
								// Resolve the promise.
								promise_resolver.resolve(scope, value).unwrap();
							},
							Err(error) => {
								// Reject the promise.
								let exception = error::to_exception(scope, &error);
								if let Some(exception) = exception {
									promise_resolver.reject(scope, exception).unwrap();
								}
							},
						}

						// Run microtasks.
						scope.perform_microtask_checkpoint();

						// Handle an exception.
						if scope.has_caught() {
							if !scope.can_continue() {
								if scope.has_terminated() {
									unsafe { scope.exit() };
									return Poll::Ready(Err(tg::error!("execution terminated")));
								}
								unsafe { scope.exit() };
								return Poll::Ready(Err(tg::error!("unrecoverable error")));
							}
							let exception = scope.exception().unwrap();
							let error = self::error::from_exception(&state, scope, exception)
								.unwrap_or_else(|| tg::error!("failed to get the exception"));
							unsafe { scope.exit() };
							return Poll::Ready(Err(error));
						}
					}

					// Exit the isolate.
					unsafe { isolate.exit() };

					// Continue.
					continue;
				},

				// If there are no more promises to resolve or reject, then do not continue.
				Poll::Ready(None) => (),
			}

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
					Err(_) => {
						<Serde<tg::value::Data>>::deserialize(scope, value).map(|value| value.0)
					},
					Ok(promise) => {
						match promise.state() {
							// If the promise is fulfilled, then return the result.
							v8::PromiseState::Fulfilled => {
								let value = promise.result(scope);
								<Serde<tg::value::Data>>::deserialize(scope, value)
									.map(|value| value.0)
							},

							// If the promise is rejected, then return the error.
							v8::PromiseState::Rejected => {
								let exception = promise.result(scope);
								let error = self::error::from_exception(&state, scope, exception)
									.unwrap_or_else(|| tg::error!("failed to get the exception"));
								Err(error)
							},

							// The promise is expected to be fulfilled or rejected at this point.
							v8::PromiseState::Pending => Err(tg::error!("unreachable")),
						}
					},
				}
			};

			// Exit the isolate.
			unsafe { isolate.exit() };

			return Poll::Ready(result);
		}
	});
	let mut rejection = state.rejection.subscribe();
	let rejection = rejection
		.wait_for(Option::is_some)
		.map_ok(|option| option.as_ref().unwrap().clone())
		.map(Result::unwrap);
	let rejection = pin!(rejection);
	let output = match future::select(pin!(future), rejection).await {
		future::Either::Left((Ok(output), _)) => Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(tg::Value::try_from(output)?),
		},
		future::Either::Left((Err(error), _)) | future::Either::Right((error, _)) => Output {
			checksum: None,
			error: Some(error),
			exit: 1,
			output: None,
		},
	};

	Ok(output)
}

impl State {
	pub fn create_promise<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
		future: impl Future<Output = tg::Result<impl tangram_v8::Serialize + 'static>> + 'static,
	) -> v8::Local<'a, v8::Promise> {
		// Create the promise.
		let resolver = v8::PromiseResolver::new(scope).unwrap();
		let promise = resolver.get_promise(scope);

		// Move the promise resolver to the global scope.
		let resolver = v8::Global::new(scope, resolver);

		// Create the future.
		let future = {
			async move {
				let result = future
					.await
					.map(|value| Box::new(value) as Box<dyn tangram_v8::Serialize>);
				PromiseOutput { resolver, result }
			}
			.boxed_local()
		};

		// Add the promise.
		self.promises.borrow_mut().push(future);

		promise
	}
}

/// Implement V8's promise rejection callback.
extern "C" fn promise_reject_callback(message: v8::PromiseRejectMessage) {
	// Get the scope.
	let scope = &mut unsafe { v8::CallbackScope::new(&message) };

	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	match message.get_event() {
		v8::PromiseRejectEvent::PromiseRejectWithNoHandler => {
			let exception = message.get_promise().result(scope);
			let error = error::from_exception(&state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			state.rejection.send_replace(Some(error));
		},
		v8::PromiseRejectEvent::PromiseHandlerAddedAfterReject
		| v8::PromiseRejectEvent::PromiseRejectAfterResolved
		| v8::PromiseRejectEvent::PromiseResolveAfterResolved => {},
	}
}
