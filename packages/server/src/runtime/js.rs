use {
	self::{
		module::{
			host_import_module_dynamically_callback, host_initialize_import_meta_object_callback,
		},
		syscall::syscall,
	},
	crate::Server,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
		future::{self, LocalBoxFuture},
		stream::FuturesUnordered,
	},
	sourcemap::SourceMap,
	std::{cell::RefCell, future::poll_fn, pin::pin, rc::Rc, task::Poll},
	tangram_client as tg,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

mod error;
mod module;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runtime.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/runtime.js.map"));

#[derive(Clone)]
pub struct Runtime {
	local_pool_handle: tokio_util::task::LocalPoolHandle,
	pub(super) handle: Server,
}

struct State {
	process: tg::Process,
	promises: RefCell<FuturesUnordered<LocalBoxFuture<'static, PromiseOutput>>>,
	global_source_map: Option<SourceMap>,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	rejection: tokio::sync::watch::Sender<Option<tg::Error>>,
	root: tg::module::Data,
	handle: Server,
}

struct PromiseOutput {
	resolver: v8::Global<v8::PromiseResolver>,
	result: tg::Result<Box<dyn tangram_v8::Serialize>>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug)]
struct Module {
	module: tg::module::Data,
	source_map: Option<SourceMap>,
	v8: Option<v8::Global<v8::Module>>,
}

impl Runtime {
	pub fn new(handle: Server, concurrency: usize) -> Self {
		// Create the local pool handle.
		let local_pool_handle = tokio_util::task::LocalPoolHandle::new(concurrency);

		Self {
			local_pool_handle,
			handle,
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		// Create a handle to the main runtime.
		let main_runtime_handle = tokio::runtime::Handle::current();

		// Create a channel to receive the isolate handle.
		let (isolate_handle_sender, isolate_handle_receiver) = tokio::sync::watch::channel(None);

		// Spawn the task.
		let task = self.local_pool_handle.spawn_pinned({
			let runtime = self.clone();
			let process = process.clone();
			move || async move {
				runtime
					.run_inner(&process, main_runtime_handle.clone(), isolate_handle_sender)
					.boxed_local()
					.await
			}
		});

		let abort_handle = task.abort_handle();
		scopeguard::defer! {
			abort_handle.abort();
			if let Some(isolate_handle) = isolate_handle_receiver.borrow().as_ref() {
				tracing::trace!("terminating execution");
				isolate_handle.terminate_execution();
			}
		};

		// Get the output.
		match task.await.unwrap() {
			Ok(output) => output,
			Err(error) => super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
		}
	}

	async fn run_inner(
		&self,
		process: &tg::Process,
		main_runtime_handle: tokio::runtime::Handle,
		isolate_handle_sender: tokio::sync::watch::Sender<Option<v8::IsolateHandle>>,
	) -> tg::Result<super::Output> {
		// Get the root module.
		let command = process.command(&self.handle).await?;
		let executable = command
			.executable(&self.handle)
			.await?
			.clone()
			.try_unwrap_module()
			.ok()
			.ok_or_else(|| tg::error!("expected the executable to be a module"))?;

		// Create the signal task.
		let (signal_sender, mut signal_receiver) =
			tokio::sync::mpsc::channel::<tg::process::Signal>(1);
		let signal_task = tokio::spawn({
			let handle = self.handle.clone();
			let process = process.clone();
			async move {
				let arg = tg::process::signal::get::Arg {
					remote: process.remote().cloned(),
				};
				let Ok(Some(stream)) = handle
					.try_get_process_signal_stream(process.id(), arg)
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to get signal stream"))
				else {
					return;
				};
				let mut stream = pin!(stream);
				while let Ok(Some(tg::process::signal::get::Event::Signal(signal))) =
					stream.try_next().await
				{
					signal_sender.send(signal).await.ok();
				}
			}
		});
		scopeguard::defer! {
			signal_task.abort();
		}

		// Create the state.
		let (rejection, _) = tokio::sync::watch::channel(None);
		let state = Rc::new(State {
			process: process.clone(),
			promises: RefCell::new(FuturesUnordered::new()),
			global_source_map: Some(SourceMap::from_slice(SOURCE_MAP).unwrap()),
			main_runtime_handle,
			modules: RefCell::new(Vec::new()),
			rejection,
			root: executable.module.to_data(),
			handle: self.handle.clone(),
		});
		scopeguard::defer! {
			state.promises.borrow_mut().clear();
		}

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
		isolate_handle_sender.send_replace(Some(isolate.thread_safe_handle()));

		// Set the microtask policy.
		isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);

		// Set the host import module dynamically callback.
		isolate
			.set_host_import_module_dynamically_callback(host_import_module_dynamically_callback);

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
			let syscall_string =
				v8::String::new_external_onebyte_static(scope, b"syscall").unwrap();
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

			// Set the id.
			let key = v8::String::new_external_onebyte_static(scope, b"id").unwrap();
			let value = Serde(process.id()).serialize(scope)?;
			arg.set(scope, key.into(), value);

			// Set the remote.
			if let Some(remote) = process.remote() {
				let key = v8::String::new_external_onebyte_static(scope, b"remote").unwrap();
				let value = remote.serialize(scope)?;
				arg.set(scope, key.into(), value);
			}

			// Get the Tangram global.
			let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
			let tangram = context.global(scope).get(scope, tangram.into()).unwrap();
			let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

			// Get the Process constructor.
			let process_constructor =
				v8::String::new_external_onebyte_static(scope, b"Process").unwrap();
			let process_constructor = tangram.get(scope, process_constructor.into()).unwrap();
			let process_constructor =
				v8::Local::<v8::Function>::try_from(process_constructor).unwrap();

			// Create the Tangram.Process.
			let process = process_constructor
				.new_instance(scope, &[arg.into()])
				.unwrap();

			// Get the start function.
			let start = v8::String::new_external_onebyte_static(scope, b"start").unwrap();
			let start = tangram.get(scope, start.into()).unwrap();
			let start = v8::Local::<v8::Function>::try_from(start).unwrap();

			// Call the start function.
			let scope = &mut v8::TryCatch::new(scope);
			let undefined = v8::undefined(scope);
			let value = start.call(scope, undefined.into(), &[process.into()]);
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
										return Poll::Ready(Err(tg::error!(
											"execution terminated"
										)));
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
									let error =
										self::error::from_exception(&state, scope, exception)
											.unwrap_or_else(|| {
												tg::error!("failed to get the exception")
											});
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
		let signal = signal_receiver.recv();
		let rejection = pin!(rejection);
		let signal = pin!(signal);
		let error_or_signal = future::select(rejection, signal);
		let output = match future::select(pin!(future), pin!(error_or_signal)).await {
			future::Either::Left((Ok(output), _)) => super::Output {
				checksum: None,
				error: None,
				exit: 0,
				output: Some(tg::Value::try_from(output)?),
			},
			future::Either::Left((Err(error), _))
			| future::Either::Right((future::Either::Left((error, _)), _)) => super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
			future::Either::Right((future::Either::Right((signal, _)), _)) => super::Output {
				checksum: None,
				error: Some(tg::error!(?signal, "process terminated with signal")),
				exit: signal.map_or(1, |signal| 128u8 + signal as u8),
				output: None,
			},
		};

		Ok(output)
	}
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
