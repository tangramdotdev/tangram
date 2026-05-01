use {
	self::{
		module::{Loader, Resolver},
		serde::Serde,
		syscall::syscall,
	},
	crate::Output,
	futures::future,
	rquickjs::{self as qjs, CatchResultExt as _},
	sourcemap::SourceMap,
	std::{cell::RefCell, pin::pin, rc::Rc},
	tangram_client::prelude::*,
};

mod error;
mod module;
mod serde;
mod syscall;
mod types;

const BYTECODE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.bytecode"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

pub struct Runtime {
	context: qjs::AsyncContext,
	repl: Option<crate::repl::Receiver>,
	#[expect(clippy::struct_field_names, dead_code)]
	runtime: qjs::AsyncRuntime,
	state: Rc<State>,
}

struct State {
	arg: crate::Arg,
	global_source_map: Option<SourceMap>,
	handle: tg::handle::dynamic::Handle,
	host: crate::host::Host,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	rejection: tokio::sync::watch::Sender<Option<tg::Error>>,
	stdio: crate::stdio::Stdio,
}

#[derive(Clone)]
struct StateHandle(Rc<State>);

#[derive(Clone, Debug)]
struct Module {
	module: tg::module::Data,
	source_map: Option<SourceMap>,
}

pub async fn run(arg: crate::Arg) -> tg::Result<Output> {
	let runtime = Runtime::new(arg).await?;
	let value = runtime.start().await?;
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
	pub async fn new(mut arg: crate::Arg) -> tg::Result<Self> {
		let repl = arg.repl.take();

		// Create the runtime.
		let runtime = qjs::AsyncRuntime::new()
			.map_err(|source| tg::error!(!source, "failed to create the QuickJS runtime"))?;

		// Set the resolver and loader.
		runtime.set_loader(Resolver, Loader).await;

		// Create the rejection channel.
		let (rejection, _) = tokio::sync::watch::channel(None);

		// Set the promise rejection tracker.
		runtime
			.set_host_promise_rejection_tracker(Some(Box::new(move |ctx, _promise, reason, _| {
				let Some(state) = ctx.userdata::<StateHandle>().map(|state| state.clone()) else {
					return;
				};
				let error = self::error::from_exception(&state, &ctx, &reason)
					.unwrap_or_else(|| tg::error!("failed to get the exception"));
				state.rejection.send_replace(Some(error));
			})))
			.await;

		// Create the context.
		let context = qjs::AsyncContext::full(&runtime)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the context"))?;

		// Create the state.
		let global_source_map = SourceMap::from_slice(SOURCE_MAP).ok();
		let handle = arg.handle.clone();
		let host = crate::host::Host::default();
		let main_runtime_handle = arg.main_runtime_handle.clone();
		let modules = RefCell::new(Vec::new());
		let rejection = rejection.clone();
		let stdio = crate::stdio::Stdio::new(arg.handle.clone(), arg.main_runtime_handle.clone());
		let state = Rc::new(State {
			arg,
			global_source_map,
			handle,
			host,
			main_runtime_handle,
			modules,
			rejection,
			stdio,
		});

		// Init.
		Self::init(&context, &state).await?;

		let runtime = Self {
			context,
			repl,
			runtime,
			state,
		};

		Ok(runtime)
	}

	async fn init(context: &qjs::AsyncContext, state: &Rc<State>) -> tg::Result<()> {
		let state = state.clone();
		context
			.with(move |ctx| {
				// Store the state in the context's userdata.
				ctx.store_userdata(StateHandle(state.clone()))
					.map_err(|_| tg::error!("failed to store the state in the context"))?;

				// Load the bytecode.
				let main_module = unsafe { qjs::Module::load(ctx.clone(), BYTECODE) }
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Evaluate the module.
				let (_evaluated_module, _promise) = main_module
					.eval()
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Register the syscall function.
				let globals = ctx.globals();
				let syscall_function = qjs::Function::new(ctx.clone(), syscall)
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				globals
					.set("syscall", syscall_function)
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Register the prepareStackTrace callback on Error.
				let error_constructor = globals
					.get::<_, qjs::Object>("Error")
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				let prepare_stack_trace_function =
					qjs::Function::new(ctx.clone(), self::error::prepare_stack_trace)
						.catch(&ctx)
						.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				error_constructor
					.set("prepareStackTrace", prepare_stack_trace_function)
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Build the arg object.
				let arg = qjs::Object::new(ctx.clone())
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				arg.set("args", Serde(&state.arg.args))
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				arg.set(
					"cwd",
					state
						.arg
						.cwd
						.to_str()
						.ok_or_else(|| tg::error!("invalid cwd"))?,
				)
				.catch(&ctx)
				.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				arg.set("env", Serde(&state.arg.env))
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				arg.set("executable", Serde(&state.arg.executable))
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Get the init function.
				let init: qjs::Function = globals
					.get("init")
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Call the init function.
				init.call::<_, ()>((arg,))
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				Ok(())
			})
			.await
	}

	async fn start(&self) -> tg::Result<qjs::Persistent<qjs::Value<'static>>> {
		let state = self.state.clone();
		self.context
			.with(move |ctx| {
				let globals = ctx.globals();

				// Get the start function.
				let start: qjs::Function = globals
					.get("start")
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				// Call the start function.
				let value: qjs::Value = start
					.call(())
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

				Ok(qjs::Persistent::save(&ctx, value))
			})
			.await
	}

	pub async fn resolve(
		&self,
		value: &qjs::Persistent<qjs::Value<'static>>,
	) -> tg::Result<tg::Value> {
		let context = self.context.clone();
		let state = self.state.clone();
		let value = value.clone();
		let future = context.async_with(async move |ctx| {
			let value = value
				.restore(&ctx)
				.catch(&ctx)
				.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

			// Get the promise and await its result.
			let promise = value
				.as_promise()
				.ok_or_else(|| tg::error!("expected a promise"))?;
			let result = promise
				.clone()
				.into_future::<Serde<tg::value::Data>>()
				.await;

			if let Ok(value) = result {
				Ok(value)
			} else {
				let exception = ctx.catch();
				let error = self::error::from_exception(&state, &ctx, &exception)
					.unwrap_or_else(|| tg::error!("promise rejected"));
				Err(error)
			}
		});
		let mut rejection = self.state.rejection.subscribe();
		let rejection = async move {
			let error = rejection
				.wait_for(Option::is_some)
				.await
				.map_err(|source| tg::error!(!source, "failed to receive the promise rejection"))?;
			Ok::<_, tg::Error>(error.as_ref().unwrap().clone())
		};
		let result = match future::select(pin!(future), pin!(rejection)).await {
			future::Either::Left((result, _)) => result,
			future::Either::Right((Ok(error) | Err(error), _)) => Err(error),
		};

		let Serde(data) = result?;
		tg::Value::try_from(data)
	}

	pub async fn run(&mut self) -> tg::Result<()> {
		if let Some(mut repl) = self.repl.take() {
			while let Some(command) = repl.recv().await {
				let crate::repl::Command { source, response } = command;
				let result = self.evaluate_repl(source).await;
				let _ = response.send(result);
			}
		}
		Ok(())
	}

	async fn evaluate_repl(&self, source: String) -> tg::Result<()> {
		let state = self.state.clone();
		self.context
			.async_with(async move |ctx| {
				let promise = {
					let mut options = qjs::context::EvalOptions::default();
					options.strict = false;
					options.backtrace_barrier = true;
					options.promise = true;
					let module = tg::module::Data {
						kind: tg::module::Kind::Js,
						referent: tg::Referent::with_item(tg::module::data::Item::Path(
							state.arg.cwd.join("<repl>.tg.js"),
						)),
					};
					options.filename = Some(module.to_string());
					ctx.eval_with_options::<qjs::Promise, _>(source, options)
				}
				.catch(&ctx)
				.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				let value = promise
					.into_future::<qjs::Value>()
					.await
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				let value = if let Some(object) = value.as_object()
					&& object
						.contains_key("value")
						.catch(&ctx)
						.map_err(|error| self::error::from_catch(&state, &ctx, error))?
				{
					object
						.get("value")
						.catch(&ctx)
						.map_err(|error| self::error::from_catch(&state, &ctx, error))?
				} else {
					value
				};
				let globals = ctx.globals();
				globals
					.set("_", value.clone())
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				let console = globals
					.get::<_, qjs::Object>("console")
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				let log = console
					.get::<_, qjs::Function>("log")
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				log.call::<_, ()>((value,))
					.catch(&ctx)
					.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
				Ok(())
			})
			.await
	}
}

impl std::ops::Deref for StateHandle {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

unsafe impl qjs::JsLifetime<'_> for StateHandle {
	type Changed<'to> = StateHandle;
}
