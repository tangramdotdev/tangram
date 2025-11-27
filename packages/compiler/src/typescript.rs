use {
	self::syscall::syscall,
	crate::{Compiler, Request, Response},
	std::sync::Mutex,
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

mod error;
mod syscall;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.heapsnapshot"));

pub(crate) const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

pub struct Typescript {
	request_sender: Mutex<Option<RequestSender>>,
	thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Typescript {
	pub fn new() -> Self {
		Self {
			request_sender: Mutex::new(None),
			thread: Mutex::new(None),
		}
	}

	pub async fn request(&self, compiler: &Compiler, request: Request) -> tg::Result<Response> {
		// Spawn the thread if necessary.
		{
			let mut thread = self.thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) =
					tokio::sync::mpsc::unbounded_channel::<(Request, ResponseSender)>();
				self.request_sender.lock().unwrap().replace(sender);
				thread.replace(std::thread::spawn({
					let compiler = compiler.clone();
					move || run(&compiler, receiver)
				}));
			}
		}

		// Create a oneshot channel for the response.
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

		// Send the request.
		self.request_sender
			.lock()
			.unwrap()
			.as_ref()
			.unwrap()
			.send((request, response_sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;

		// Receive the response.
		let response = response_receiver.await.map_err(|error| {
			tg::error!(
				source = error,
				"failed to receive a response for the request"
			)
		})??;

		Ok(response)
	}

	pub fn stop(&self) {
		self.request_sender.lock().unwrap().take();
	}

	pub async fn join(&self) {
		let thread = self.thread.lock().unwrap().take();
		if let Some(thread) = thread {
			tokio::task::spawn_blocking(move || thread.join().unwrap())
				.await
				.unwrap();
		}
	}
}

fn run(compiler: &Compiler, mut request_receiver: RequestReceiver) {
	// Create the isolate.
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT);
	let mut isolate = v8::Isolate::new(params);

	// Set the prepare stack trace callback.
	isolate.set_prepare_stack_trace_callback(self::error::prepare_stack_trace_callback);

	// Create the context.
	let scope = &mut v8::HandleScope::new(&mut isolate);
	let context = v8::Context::new(scope, v8::ContextOptions::default());
	let scope = &mut v8::ContextScope::new(scope, context);

	// Set the server on the context.
	context.set_slot(compiler.clone());

	// Add the syscall function to the global.
	let syscall_string = v8::String::new_external_onebyte_static(scope, b"syscall").unwrap();
	let syscall_function = v8::Function::new(scope, syscall).unwrap();
	context
		.global(scope)
		.set(scope, syscall_string.into(), syscall_function.into())
		.unwrap();

	// Get the handle function.
	let global = context.global(scope);
	let handle = v8::String::new_external_onebyte_static(scope, b"handle").unwrap();
	let handle = global.get(scope, handle.into()).unwrap();
	let handle = v8::Local::<v8::Function>::try_from(handle).unwrap();

	while let Some((request, response_sender)) = request_receiver.blocking_recv() {
		// Create a try catch scope.
		let scope = &mut v8::TryCatch::new(scope);

		// Serialize the request.
		let result = Serde(request)
			.serialize(scope)
			.map_err(|source| tg::error!(!source, "failed to serialize the request"));
		let request = match result {
			Ok(request) => request,
			Err(error) => {
				response_sender.send(Err(error)).ok();
				continue;
			},
		};

		// Call the handle function.
		let receiver = v8::undefined(scope).into();
		let response = handle.call(scope, receiver, &[request]);

		// Handle an error.
		if let Some(exception) = scope.exception() {
			let error = error::from_exception(scope, exception);
			response_sender.send(Err(error)).ok();
			continue;
		}
		let response = response.unwrap();

		// Deserialize the response.
		let result = Serde::deserialize(scope, response)
			.map(|output| output.0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"));
		let response = match result {
			Ok(response) => response,
			Err(error) => {
				response_sender.send(Err(error)).ok();
				continue;
			},
		};

		// Send the response.
		response_sender.send(Ok(response)).ok();
	}
}
