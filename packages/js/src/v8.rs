use {
	self::{
		module::{
			host_import_module_dynamically_callback, host_initialize_import_meta_object_callback,
		},
		promise::promise_reject_callback,
		syscall::syscall,
	},
	crate::Output,
	futures::{StreamExt as _, future::LocalBoxFuture, stream::FuturesUnordered},
	sourcemap::SourceMap,
	std::{
		cell::RefCell,
		future::poll_fn,
		path::{Path, PathBuf},
		rc::Rc,
		task::Poll,
	},
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

mod error;
mod module;
mod promise;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

struct Runtime {
	context: v8::Global<v8::Context>,
	isolate: v8::OwnedIsolate,
	state: Rc<State>,
}

struct State {
	global_source_map: Option<SourceMap>,
	handle: tg::handle::dynamic::Handle,
	host: crate::host::Host,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	promises: RefCell<FuturesUnordered<LocalBoxFuture<'static, self::promise::Output>>>,
	rejection: RefCell<Option<tg::Error>>,
	stdio: crate::stdio::Stdio,
}

#[derive(Clone, Debug)]
struct Module {
	data: tg::module::Data,
	source_map: Option<SourceMap>,
	v8: Option<v8::Global<v8::Module>>,
}

pub async fn run(
	handle: tg::handle::dynamic::Handle,
	main_runtime_handle: tokio::runtime::Handle,
	args: tg::value::data::Array,
	cwd: PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
) -> tg::Result<Output> {
	let mut runtime = Runtime::new(handle, &args, &cwd, &env, &executable, main_runtime_handle)?;
	let value = runtime.start()?;
	let result = runtime.resolve(&value).await;
	let output = match result {
		Ok(output) => Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		},
		Err(error) => Output {
			checksum: None,
			error: Some(error),
			exit: 1,
			output: None,
		},
	};
	Ok(output)
}

impl Runtime {
	pub fn new(
		handle: tg::handle::dynamic::Handle,
		args: &tg::value::data::Array,
		cwd: &Path,
		env: &tg::value::data::Map,
		executable: &tg::command::data::Executable,
		main_runtime_handle: tokio::runtime::Handle,
	) -> tg::Result<Self> {
		// Create the isolate params.
		let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT.into());

		// Create the isolate.
		let mut isolate = v8::Isolate::new(params);
		unsafe { isolate.exit() };

		// Enter the isolate.
		unsafe { isolate.enter() };

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
			v8::scope!(scope, &mut isolate);
			let context = v8::Context::new(scope, v8::ContextOptions::default());
			let scope = &mut v8::ContextScope::new(scope, context);
			v8::Global::new(scope, context)
		};

		// Create the state.
		let state = Rc::new(State {
			global_source_map: Some(SourceMap::from_slice(SOURCE_MAP).unwrap()),
			handle: handle.clone(),
			host: crate::host::Host::default(),
			main_runtime_handle: main_runtime_handle.clone(),
			modules: RefCell::new(Vec::new()),
			promises: RefCell::new(FuturesUnordered::new()),
			rejection: RefCell::new(None),
			stdio: crate::stdio::Stdio::new(handle, main_runtime_handle),
		});

		// Init.
		let result = Self::init(&mut isolate, &context, &state, args, cwd, env, executable);
		match result {
			Ok(()) => (),
			Err(error) => {
				return Err(error);
			},
		}

		// Exit the isolate.
		unsafe { isolate.exit() };

		let runtime = Self {
			context,
			isolate,
			state,
		};

		Ok(runtime)
	}

	fn init(
		isolate: &mut v8::Isolate,
		context: &v8::Global<v8::Context>,
		state: &Rc<State>,
		args: &tg::value::data::Array,
		cwd: &Path,
		env: &tg::value::data::Map,
		executable: &tg::command::data::Executable,
	) -> tg::Result<()> {
		// Create a scope for the context.
		v8::scope!(scope, isolate);
		let context = v8::Local::new(scope, context.clone());
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

		// Create the arg.
		let arg = v8::Object::new(scope);

		// Set args.
		let key = v8::String::new_external_onebyte_static(scope, b"args").unwrap();
		let value = Serde(&args).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set cwd.
		let key = v8::String::new_external_onebyte_static(scope, b"cwd").unwrap();
		let value = cwd.to_str().ok_or_else(|| tg::error!("invalid cwd"))?;
		let value = Serde(value).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set env.
		let key = v8::String::new_external_onebyte_static(scope, b"env").unwrap();
		let value = Serde(&env).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Set executable.
		let key = v8::String::new_external_onebyte_static(scope, b"executable").unwrap();
		let value = Serde(&executable).serialize(scope)?;
		arg.set(scope, key.into(), value);

		// Get the init function.
		let init = v8::String::new_external_onebyte_static(scope, b"init").unwrap();
		let init = context.global(scope).get(scope, init.into()).unwrap();
		let init = v8::Local::<v8::Function>::try_from(init).unwrap();

		// Call the init function.
		v8::tc_scope!(scope, scope);
		let undefined = v8::undefined(scope);
		init.call(scope, undefined.into(), &[arg.into()]);
		if scope.has_caught() {
			if !scope.can_continue() {
				if scope.has_terminated() {
					return Err(tg::error!("execution terminated"));
				}
				return Err(tg::error!("unrecoverable error"));
			}
			let exception = scope.exception().unwrap();
			let error = self::error::from_exception(state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			return Err(error);
		}

		Ok(())
	}

	fn start(&mut self) -> tg::Result<v8::Global<v8::Value>> {
		unsafe { self.isolate.enter() };
		let result = self.start_inner();
		unsafe { self.isolate.exit() };
		let output = result?;
		Ok(output)
	}

	fn start_inner(&mut self) -> tg::Result<v8::Global<v8::Value>> {
		// Create a scope for the context.
		v8::scope!(scope, &mut self.isolate);
		let context = v8::Local::new(scope, self.context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);

		// Get the start function.
		let start = v8::String::new_external_onebyte_static(scope, b"start").unwrap();
		let start = context.global(scope).get(scope, start.into()).unwrap();
		let start = v8::Local::<v8::Function>::try_from(start).unwrap();

		// Call the start function.
		v8::tc_scope!(scope, scope);
		let undefined = v8::undefined(scope);
		let value = start.call(scope, undefined.into(), &[]);
		if scope.has_caught() {
			if !scope.can_continue() {
				if scope.has_terminated() {
					return Err(tg::error!("execution terminated"));
				}
				return Err(tg::error!("unrecoverable error"));
			}
			let exception = scope.exception().unwrap();
			let error = self::error::from_exception(&self.state, scope, exception)
				.unwrap_or_else(|| tg::error!("failed to get the exception"));
			return Err(error);
		}
		let value = value.unwrap();

		// Make the value global.
		let value = v8::Global::new(scope, value);

		Ok(value)
	}

	pub async fn resolve(&mut self, value: &v8::Global<v8::Value>) -> tg::Result<tg::Value> {
		poll_fn(|cx| {
			loop {
				let done = match self.poll_event_loop(cx) {
					Poll::Pending => {
						return Poll::Pending;
					},
					Poll::Ready(Err(error)) => {
						return Poll::Ready(Err(error));
					},
					Poll::Ready(Ok(done)) => done,
				};
				if let Some(result) = self.try_resolve_value(value) {
					return Poll::Ready(result);
				}
				if done {
					return Poll::Pending;
				}
			}
		})
		.await
	}

	fn poll_event_loop(&mut self, cx: &mut std::task::Context<'_>) -> Poll<tg::Result<bool>> {
		if let Some(error) = self.state.rejection.borrow().clone() {
			return Poll::Ready(Err(error));
		}

		let poll = self.state.promises.borrow_mut().poll_next_unpin(cx);

		let output = match poll {
			Poll::Pending => {
				return Poll::Pending;
			},
			Poll::Ready(None) => {
				return Poll::Ready(Ok(true));
			},
			Poll::Ready(Some(output)) => output,
		};

		let result = self.resolve_or_reject_promise(output);
		if let Err(error) = result {
			return Poll::Ready(Err(error));
		}

		Poll::Ready(Ok(false))
	}

	fn try_resolve_value(
		&mut self,
		value: &v8::Global<v8::Value>,
	) -> Option<tg::Result<tg::Value>> {
		unsafe { self.isolate.enter() };
		let result = self.try_resolve_value_inner(value);
		unsafe { self.isolate.exit() };
		result
	}

	fn try_resolve_value_inner(
		&mut self,
		value: &v8::Global<v8::Value>,
	) -> Option<tg::Result<tg::Value>> {
		v8::scope!(scope, &mut self.isolate);
		let context = v8::Local::new(scope, self.context.clone());
		let scope = &mut v8::ContextScope::new(scope, context);
		let value = v8::Local::new(scope, value.clone());
		match v8::Local::<v8::Promise>::try_from(value) {
			Err(_) => {
				let value = <Serde<tg::value::Data>>::deserialize(scope, value)
					.and_then(|value| tg::Value::try_from_data(value.0));
				Some(value)
			},
			Ok(promise) => match promise.state() {
				v8::PromiseState::Fulfilled => {
					let value = promise.result(scope);
					let value = <Serde<tg::value::Data>>::deserialize(scope, value)
						.and_then(|value| tg::Value::try_from_data(value.0));
					Some(value)
				},
				v8::PromiseState::Rejected => {
					let exception = promise.result(scope);
					let error = self::error::from_exception(&self.state, scope, exception)
						.unwrap_or_else(|| tg::error!("failed to get the exception"));
					Some(Err(error))
				},
				v8::PromiseState::Pending => None,
			},
		}
	}
}

impl Drop for Runtime {
	fn drop(&mut self) {
		unsafe { self.isolate.enter() };
	}
}
