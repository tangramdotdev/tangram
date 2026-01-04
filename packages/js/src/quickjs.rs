use {
	self::{
		module::{Loader, Resolver},
		serde::Serde,
		syscall::syscall,
	},
	crate::{Logger, Output},
	rquickjs::{self as qjs, CatchResultExt as _},
	sourcemap::SourceMap,
	std::{cell::RefCell, path::PathBuf, rc::Rc},
	tangram_client::prelude::*,
};

mod error;
mod module;
mod serde;
mod syscall;
mod types;

const BYTECODE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.bytecode"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

struct State {
	global_source_map: Option<SourceMap>,
	logger: Logger,
	main_runtime_handle: tokio::runtime::Handle,
	modules: RefCell<Vec<Module>>,
	root: tg::module::Data,
	handle: tg::handle::dynamic::Handle,
}

#[derive(Clone)]
struct StateHandle(Rc<State>);

#[derive(Clone, Debug)]
struct Module {
	module: tg::module::Data,
	source_map: Option<SourceMap>,
}

pub struct Abort {
	sender: tokio::sync::watch::Sender<bool>,
}

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
			let kind = tg::package::module_kind_for_path(&executable.path)
				.map_err(|_| tg::error!("invalid executable"))?;
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

	// Create the runtime.
	let runtime = qjs::AsyncRuntime::new()
		.map_err(|source| tg::error!(!source, "failed to create the QuickJS runtime"))?;

	// Create the abort channel.
	let (abort_channel_sender, abort_channel_receiver) = tokio::sync::watch::channel(false);

	// Send the abort handle if requested.
	if let Some(abort_sender) = abort_sender {
		let abort = Abort {
			sender: abort_channel_sender,
		};
		abort_sender.send_replace(Some(abort));
	}

	// Set the interrupt handler.
	runtime
		.set_interrupt_handler(Some(Box::new(move || *abort_channel_receiver.borrow())))
		.await;

	// Set the resolver and loader.
	runtime.set_loader(Resolver, Loader).await;

	// Create the context.
	let context = qjs::AsyncContext::full(&runtime)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the context"))?;

	// Create the state.
	let state = Rc::new(State {
		global_source_map: SourceMap::from_slice(SOURCE_MAP).ok(),
		logger,
		main_runtime_handle,
		modules: RefCell::new(Vec::new()),
		root: module.clone(),
		handle: tg::handle::dynamic::Handle::new(handle.clone()),
	});

	// Initialize the context and await the result.
	let result = qjs::async_with!(context => |ctx| {
		let state = state.clone();
		let args = args.clone();
		let cwd = cwd.clone();
		let env = env.clone();
		let executable = executable.clone();

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

		// Get the start function.
		let start: qjs::Function = globals
			.get("start")
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

		// Build the arg object.
		let arg = qjs::Object::new(ctx.clone())
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
		arg.set("args", Serde(&args))
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
		arg.set(
			"cwd",
			cwd.to_str().ok_or_else(|| tg::error!("invalid cwd"))?,
		)
		.catch(&ctx)
		.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
		arg.set("env", Serde(&env))
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;
		arg.set("executable", Serde(&executable))
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

		// Call the start function.
		let value: qjs::Value = start
			.call((arg,))
			.catch(&ctx)
			.map_err(|error| self::error::from_catch(&state, &ctx, error))?;

		// Get the promise and await its result.
		let promise = value
			.as_promise()
			.ok_or_else(|| tg::error!("expected a promise"))?;
		let result =
			promise.clone().into_future::<Serde<tg::value::Data>>().await;

		if let Ok(value) = result {
			Ok(value)
		} else {
			let exception = ctx.catch();
			let error = self::error::from_exception(&state, &ctx, &exception)
				.unwrap_or_else(|| tg::error!("promise rejected"));
			Err(error)
		}
	})
	.await;

	let output = match result {
		Ok(Serde(data)) => Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(tg::Value::try_from(data)?),
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

impl Abort {
	pub fn abort(&self) {
		let _ = self.sender.send(true);
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
