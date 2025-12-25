use {
	self::{
		module::{
			host_import_module_dynamically_callback, host_initialize_import_meta_object_callback,
		},
		syscall::syscall,
	},
	crate::{Logger, Output},
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _,
		future::{self, LocalBoxFuture},
		stream::FuturesUnordered,
	},
	sourcemap::SourceMap,
	std::{cell::RefCell, future::poll_fn, path::PathBuf, pin::pin, rc::Rc, task::Poll},
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

mod error;
mod module;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

struct State {
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

#[derive(Clone, Debug)]
#[expect(clippy::struct_field_names)]
struct Module {
	module: tg::module::Data,
	source_map: Option<SourceMap>,
	v8: Option<v8::Global<v8::Module>>,
}

pub struct Abort(v8::IsolateHandle);

#[expect(clippy::too_many_arguments)]
pub async fn run<H>(
	handle: &H,
	args: tg::value::data::Array,
	cwd: PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
	logger: Logger,
	main_runtime_handle: tokio::runtime::Handle,
	abort_sender: Option<tokio::sync::watch::Sender<Option<Abort>>>,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	// Convert the executable to a module.
	let module = match &executable {
		tg::command::data::Executable::Artifact(_) => {
			return Err(tg::error!("invalid executable"));
		},

		tg::command::data::Executable::Module(executable) => executable.module.clone(),

		tg::command::data::Executable::Path(executable) => {
			let kind = executable
				.path
				.extension()
				.and_then(|ext| ext.to_str())
				.and_then(|ext| match ext {
					"js" => Some(tg::module::Kind::Js),
					"ts" => Some(tg::module::Kind::Ts),
					_ => None,
				})
				.ok_or_else(|| tg::error!("invalid executable"))?;
			let item = tg::module::data::Item::Path(executable.path.clone());
			let options = tg::referent::Options {
				path: Some(executable.path.clone()),
				..Default::default()
			};
			tg::module::Data {
				kind,
				referent: tg::Referent { item, options },
			}
		},
	};

	// Create the state.
	let (rejection, _) = tokio::sync::watch::channel(None);
	let state = Rc::new(State {
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
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT.into());

	// Create the isolate.
	let isolate = v8::Isolate::new(params);
	let mut isolate = scopeguard::guard(isolate, |isolate| unsafe {
		isolate.enter();
	});
	unsafe { isolate.exit() };

	// Enter the isolate.
	unsafe { isolate.enter() };

	// Send the abort handle.
	if let Some(abort_sender) = abort_sender {
		abort_sender.send_replace(Some(Abort::new(isolate.thread_safe_handle())));
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
		v8::scope!(scope, isolate.as_mut());
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
		v8::scope!(scope, isolate.as_mut());
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
		v8::tc_scope!(scope, scope);
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
						v8::scope!(scope, isolate.as_mut());
						let context = v8::Local::new(scope, context.clone());
						let scope = &mut v8::ContextScope::new(scope, context);
						v8::tc_scope!(scope, scope);

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
				v8::scope!(scope, isolate.as_mut());
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
	pub fn create_promise<'s>(
		&self,
		scope: &mut v8::PinScope<'s, '_>,
		future: impl Future<Output = tg::Result<impl tangram_v8::Serialize + 'static>> + 'static,
	) -> v8::Local<'s, v8::Promise> {
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
	v8::callback_scope!(unsafe scope, &message);

	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

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

impl Abort {
	#[must_use]
	pub fn new(handle: v8::IsolateHandle) -> Self {
		Self(handle)
	}

	pub fn abort(&self) {
		self.0.terminate_execution();
	}
}
