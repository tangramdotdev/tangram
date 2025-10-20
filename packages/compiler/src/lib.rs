use {
	self::{document::Document, syscall::syscall},
	dashmap::DashMap,
	futures::{FutureExt as _, TryStreamExt as _, future},
	lsp_types::{self as lsp, notification::Notification as _, request::Request as _},
	std::{
		collections::{BTreeSet, HashMap},
		ops::Deref,
		path::{Path, PathBuf},
		pin::pin,
		sync::{Arc, Mutex, RwLock},
	},
	tangram_client::{self as tg, prelude::*},
	tangram_futures::task::Stop,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
	tokio::io::{
		AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
	},
	tokio_util::task::TaskTracker,
};

pub mod check;
pub mod completion;
pub mod definition;
pub mod diagnostics;
pub mod document;
pub mod error;
pub mod format;
pub mod hover;
pub mod initialize;
pub mod jsonrpc;
pub mod references;
pub mod rename;
pub mod symbols;
pub mod syscall;
pub mod version;
pub mod workspace;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/main.js.map"));

#[derive(Clone)]
pub struct Handle(Arc<Inner>);

pub struct Inner {
	compiler: Compiler,
	task: tangram_futures::task::Shared<()>,
}

#[derive(Clone)]
pub struct Compiler(Arc<State>);

pub struct State {
	/// The cache path.
	cache_path: PathBuf,

	/// The documents.
	documents: DashMap<tg::module::Data, Document, fnv::FnvBuildHasher>,

	/// The server.
	handle: tg::handle::dynamic::Handle,

	/// The library path.
	library_path: PathBuf,

	/// A handle to the main tokio runtime.
	main_runtime_handle: tokio::runtime::Handle,

	/// The sender.
	sender: RwLock<Option<tokio::sync::mpsc::UnboundedSender<jsonrpc::Message>>>,

	/// The serve task.
	serve_task: Mutex<Option<tangram_futures::task::Shared<()>>>,

	/// The typescript request sender.
	typescript_request_sender: Mutex<Option<RequestSender>>,

	/// The typescript request thread.
	typescript_thread: Mutex<Option<std::thread::JoinHandle<()>>>,

	/// The version.
	version: String,

	/// The workspaces.
	workspaces: tokio::sync::RwLock<BTreeSet<PathBuf>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "request")]
enum Request {
	Check(check::Request),
	Completion(completion::Request),
	Definition(definition::Request),
	DocumentDiagnostics(diagnostics::DocumentRequest),
	Document(document::Request),
	Hover(hover::Request),
	References(references::Request),
	Rename(rename::Request),
	Symbols(symbols::Request),
	TypeDefinition(definition::Request),
}

#[derive(Debug, derive_more::Unwrap, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "response")]
enum Response {
	Check(check::Response),
	Completion(completion::Response),
	Definition(definition::Response),
	DocumentDiagnostics(diagnostics::DocumentResponse),
	Document(document::Response),
	Hover(hover::Response),
	References(references::Response),
	Rename(rename::Response),
	Symbols(symbols::Response),
	TypeDefinition(definition::Response),
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<Response>>;

impl Handle {
	pub fn stop(&self) {
		self.task.stop();
	}

	pub async fn wait(&self) -> Result<(), Arc<tokio::task::JoinError>> {
		self.task.wait().await
	}
}

impl Compiler {
	#[must_use]
	pub fn start(
		handle: tg::handle::dynamic::Handle,
		cache_path: PathBuf,
		library_path: PathBuf,
		main_runtime_handle: tokio::runtime::Handle,
		version: String,
	) -> Handle {
		let documents = DashMap::default();
		let typescript_request_sender = Mutex::new(None);
		let typescript_thread = Mutex::new(None);
		let sender = std::sync::RwLock::new(None);
		let serve_task = Mutex::new(None);
		let workspaces = tokio::sync::RwLock::new(BTreeSet::new());

		// Create the compiler.
		let compiler = Self(Arc::new(State {
			cache_path,
			documents,
			handle,
			library_path,
			main_runtime_handle,
			sender,
			serve_task,
			typescript_request_sender,
			typescript_thread,
			version,
			workspaces,
		}));

		let shutdown = {
			let compiler = compiler.clone();
			async move {
				// Stop and await the serve task.
				let serve_task = compiler.serve_task.lock().unwrap().clone();
				if let Some(serve_task) = serve_task {
					serve_task.stop();
					serve_task.wait().await.unwrap();
				}

				// Stop and await the typescript thread.
				compiler.typescript_request_sender.lock().unwrap().take();
				let thread = compiler.typescript_thread.lock().unwrap().take();
				if let Some(thread) = thread {
					tokio::task::spawn_blocking(move || thread.join().unwrap())
						.await
						.unwrap();
				}
			}
		};

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn({
			move |stop| async move {
				stop.wait().await;
				shutdown.await;
			}
		});

		Handle(Arc::new(Inner { compiler, task }))
	}

	pub async fn serve(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		let task = tangram_futures::task::Shared::spawn(|stop| {
			let compiler = self.clone();
			async move {
				compiler.serve_inner(input, output, stop).await;
			}
		});
		self.serve_task.lock().unwrap().replace(task.clone());
		task.wait()
			.await
			.map_err(|source| tg::error!(!source, "the serve task panicked"))?;
		Ok(())
	}

	async fn serve_inner(
		&self,
		mut input: impl AsyncBufRead + Send + Unpin + 'static,
		mut output: impl AsyncWrite + Send + Unpin + 'static,
		stop: Stop,
	) {
		// Create the task tracker.
		let tasks = TaskTracker::new();

		// Create a channel to send outgoing messages.
		let (outgoing_message_sender, mut outgoing_message_receiver) =
			tokio::sync::mpsc::unbounded_channel::<jsonrpc::Message>();

		// Create a task to send outgoing messages.
		let outgoing_message_task = tokio::spawn(async move {
			while let Some(outgoing_message) = outgoing_message_receiver.recv().await {
				let body = serde_json::to_string(&outgoing_message)
					.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
				let head = format!("Content-Length: {}\r\n\r\n", body.len());
				output
					.write_all(head.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the head"))?;
				output
					.write_all(body.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the body"))?;
				output
					.flush()
					.await
					.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
			}
			Ok::<_, tg::Error>(())
		});

		// Set the outgoing message sender.
		self.sender
			.write()
			.unwrap()
			.replace(outgoing_message_sender);

		// Read incoming messages.
		loop {
			// Read a message.
			let read = Self::read_incoming_message(&mut input);
			let result = match future::select(pin!(read), pin!(stop.wait())).await {
				future::Either::Left((result, _)) => result,
				future::Either::Right(((), _)) => {
					break;
				},
			};
			let message = match result {
				Ok(Some(message)) => message,
				Ok(None) => {
					break;
				},
				Err(error) => {
					tracing::error!(?error, "failed to read an incoming message");
					break;
				},
			};

			// If the message is the exit notification, then break.
			if matches!(&message,
				jsonrpc::Message::Notification(jsonrpc::Notification {
					method,
					..
				}) if method == <lsp::notification::Exit as lsp::notification::Notification>::METHOD
			) {
				break;
			}

			// Handle the message.
			self.handle_message(message, &tasks).await;
		}

		// Wait for all tasks to complete.
		tasks.close();
		tasks.wait().await;

		// Drop the outgoing message sender.
		self.sender.write().unwrap().take().unwrap();

		// Wait for the outgoing message task to complete.
		outgoing_message_task
			.await
			.unwrap()
			.inspect_err(|error| {
				tracing::error!(?error, "the outgoing message task failed");
			})
			.ok();
	}

	async fn read_incoming_message<R>(reader: &mut R) -> tg::Result<Option<jsonrpc::Message>>
	where
		R: AsyncBufRead + Unpin,
	{
		// Read the headers.
		let mut headers = HashMap::new();
		loop {
			let mut line = String::new();
			let n = reader
				.read_line(&mut line)
				.await
				.map_err(|source| tg::error!(!source, "failed to read a line"))?;
			if n == 0 {
				return Ok(None);
			}
			if !line.ends_with("\r\n") {
				return Err(tg::error!(?line, "unexpected line ending"));
			}
			let line = &line[..line.len() - 2];
			if line.is_empty() {
				break;
			}
			let mut components = line.split(": ");
			let key = components
				.next()
				.ok_or_else(|| tg::error!("expected a header name"))?;
			let value = components
				.next()
				.ok_or_else(|| tg::error!("expected a header value"))?;
			headers.insert(key.to_owned(), value.to_owned());
		}

		// Read and deserialize the message.
		let content_length: usize = headers
			.get("Content-Length")
			.ok_or_else(|| tg::error!("expected a Content-Length header"))?
			.parse()
			.map_err(|error| {
				tg::error!(
					source = error,
					"failed to parse the Content-Length header value"
				)
			})?;
		let mut message: Vec<u8> = vec![0; content_length];
		reader
			.read_exact(&mut message)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the message"))?;
		let message = serde_json::from_slice(&message)
			.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

		Ok(message)
	}

	async fn handle_message(&self, message: jsonrpc::Message, tasks: &TaskTracker) {
		match message {
			// Handle a request.
			jsonrpc::Message::Request(request) => {
				let compiler = self.clone();
				tasks.spawn(async move {
					compiler.handle_request(request).await;
				});
			},

			// Handle a response.
			jsonrpc::Message::Response(_) => (),

			// Handle a notification.
			jsonrpc::Message::Notification(notification) => {
				self.handle_notification(notification).await;
			},
		}
	}

	async fn handle_request(&self, request: jsonrpc::Request) {
		match request.method.as_str() {
			lsp::request::Completion::METHOD => self
				.handle_request_with::<lsp::request::Completion, _, _>(request, |params| {
					self.handle_completion_request(params)
				})
				.boxed(),

			lsp::request::DocumentDiagnosticRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentDiagnosticRequest, _, _>(
					request,
					|params| self.handle_document_diagnostic_request(params),
				)
				.boxed(),

			lsp::request::DocumentSymbolRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentSymbolRequest, _, _>(
					request,
					|params| self.handle_document_symbol_request(params),
				)
				.boxed(),

			lsp::request::GotoDefinition::METHOD => self
				.handle_request_with::<lsp::request::GotoDefinition, _, _>(request, |params| {
					self.handle_definition_request(params)
				})
				.boxed(),

			lsp::request::GotoTypeDefinition::METHOD => self
				.handle_request_with::<lsp::request::GotoTypeDefinition, _, _>(request, |params| {
					self.handle_type_definition_request(params)
				})
				.boxed(),

			lsp::request::Formatting::METHOD => self
				.handle_request_with::<lsp::request::Formatting, _, _>(request, |params| {
					self.handle_format_request(params)
				})
				.boxed(),

			lsp::request::HoverRequest::METHOD => self
				.handle_request_with::<lsp::request::HoverRequest, _, _>(request, |params| {
					self.handle_hover_request(params)
				})
				.boxed(),

			lsp::request::Initialize::METHOD => self
				.handle_request_with::<lsp::request::Initialize, _, _>(request, |params| {
					self.handle_initialize_request(params)
				})
				.boxed(),

			lsp::request::References::METHOD => self
				.handle_request_with::<lsp::request::References, _, _>(request, |params| {
					self.handle_references_request(params)
				})
				.boxed(),

			lsp::request::Rename::METHOD => self
				.handle_request_with::<lsp::request::Rename, _, _>(request, |params| {
					self.handle_rename_request(params)
				})
				.boxed(),

			lsp::request::Shutdown::METHOD => self
				.handle_request_with::<lsp::request::Shutdown, _, _>(request, |()| async move {
					Ok::<_, tg::Error>(())
				})
				.boxed(),

			// If the request method does not have a handler, then send a method not found response.
			_ => {
				let error = jsonrpc::ResponseError {
					code: jsonrpc::ResponseErrorCode::MethodNotFound,
					message: "method not found".to_owned(),
				};
				self.send_response::<()>(request.id, None, Some(error));
				future::ready(()).boxed()
			},
		}
		.await;
	}

	async fn handle_notification(&self, notification: jsonrpc::Notification) {
		match notification.method.as_str() {
			lsp::notification::DidOpenTextDocument::METHOD => self
				.handle_notification_with::<lsp::notification::DidOpenTextDocument, _, _>(
					notification,
					|params| self.handle_did_open_notification(params),
				)
				.boxed(),

			lsp::notification::DidChangeTextDocument::METHOD => self
				.handle_notification_with::<lsp::notification::DidChangeTextDocument, _, _>(
					notification,
					|params| self.handle_did_change_notification(params),
				)
				.boxed(),

			lsp::notification::DidCloseTextDocument::METHOD => self
				.handle_notification_with::<lsp::notification::DidCloseTextDocument, _, _>(
					notification,
					|params| self.handle_did_close_notification(params),
				)
				.boxed(),

			lsp::notification::DidSaveTextDocument::METHOD => self
				.handle_notification_with::<lsp::notification::DidSaveTextDocument, _, _>(
					notification,
					|params| self.handle_did_save_notification(params),
				)
				.boxed(),

			lsp::notification::DidChangeWorkspaceFolders::METHOD => self
				.handle_notification_with::<lsp::notification::DidChangeWorkspaceFolders, _, _>(
					notification,
					|params| self.handle_did_change_workspace_folders(params),
				)
				.boxed(),

			// If the notification method does not have a handler, then do nothing.
			_ => future::ready(()).boxed(),
		}
		.await;
	}

	async fn handle_request_with<T, F, Fut>(&self, request: jsonrpc::Request, handler: F)
	where
		T: lsp::request::Request,
		F: Fn(T::Params) -> Fut,
		Fut: Future<Output = tg::Result<T::Result>>,
	{
		// Deserialize the params.
		let Ok(params) = serde_json::from_value(request.params.unwrap_or(serde_json::Value::Null))
		else {
			let error = jsonrpc::ResponseError {
				code: jsonrpc::ResponseErrorCode::InvalidParams,
				message: "invalid params".to_owned(),
			};
			self.send_response::<()>(request.id, None, Some(error));
			return;
		};

		// Call the handler.
		let result = handler(params).await;

		// Get the result and error.
		let (result, error) = match result {
			Ok(result) => {
				let result = serde_json::to_value(result).unwrap();
				(Some(result), None)
			},
			Err(error) => {
				let message = error.to_string();
				let error = jsonrpc::ResponseError {
					code: jsonrpc::ResponseErrorCode::InternalError,
					message,
				};
				(None, Some(error))
			},
		};

		// Send the response.
		self.send_response(request.id, result, error);
	}

	async fn handle_notification_with<T, F, Fut>(&self, request: jsonrpc::Notification, handler: F)
	where
		T: lsp::notification::Notification,
		F: Fn(T::Params) -> Fut,
		Fut: Future<Output = tg::Result<()>>,
	{
		let params = serde_json::from_value(request.params.unwrap_or(serde_json::Value::Null))
			.map_err(|source| tg::error!(!source, "failed to deserialize the request params"))
			.unwrap();
		handler(params)
			.await
			.inspect_err(|error| {
				tracing::error!(?error, "the notification handler failed");
			})
			.ok();
	}

	fn send_response<T>(
		&self,
		id: jsonrpc::Id,
		result: Option<T>,
		error: Option<jsonrpc::ResponseError>,
	) where
		T: serde::Serialize,
	{
		let result = result.map(|result| serde_json::to_value(result).unwrap());
		let message = jsonrpc::Message::Response(jsonrpc::Response {
			jsonrpc: jsonrpc::VERSION.to_owned(),
			id,
			result,
			error,
		});
		self.sender
			.read()
			.unwrap()
			.as_ref()
			.unwrap()
			.send(message)
			.ok();
	}

	#[allow(dead_code)]
	fn send_notification<T>(&self, params: T::Params)
	where
		T: lsp::notification::Notification,
	{
		let params = serde_json::to_value(params).unwrap();
		let message = jsonrpc::Message::Notification(jsonrpc::Notification {
			jsonrpc: jsonrpc::VERSION.to_owned(),
			method: T::METHOD.to_owned(),
			params: Some(params),
		});
		self.sender
			.read()
			.unwrap()
			.as_ref()
			.unwrap()
			.send(message)
			.ok();
	}

	async fn request(&self, request: Request) -> tg::Result<Response> {
		// Spawn the typescript thread if necessary.
		{
			let mut thread = self.typescript_thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) =
					tokio::sync::mpsc::unbounded_channel::<(Request, ResponseSender)>();
				self.typescript_request_sender
					.lock()
					.unwrap()
					.replace(sender);
				thread.replace(std::thread::spawn({
					let compiler = self.clone();
					move || compiler.run_typescript(receiver)
				}));
			}
		}

		// Create a oneshot channel for the response.
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

		// Send the request.
		self.typescript_request_sender
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

	/// Run typescript.
	fn run_typescript(&self, mut request_receiver: RequestReceiver) {
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
		context.set_slot(self.clone());

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
					response_sender.send(Err(error)).unwrap();
					continue;
				},
			};

			// Call the handle function.
			let receiver = v8::undefined(scope).into();
			let response = handle.call(scope, receiver, &[request]);

			// Handle an error.
			if let Some(exception) = scope.exception() {
				let error = error::from_exception(scope, exception);
				response_sender.send(Err(error)).unwrap();
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
					response_sender.send(Err(error)).unwrap();
					continue;
				},
			};

			// Send the response.
			response_sender.send(Ok(response)).unwrap();
		}
	}

	async fn module_for_lsp_uri(&self, uri: &lsp::Uri) -> tg::Result<tg::module::Data> {
		// Verify the scheme and get the path.
		if uri.scheme().unwrap().as_str() != "file" {
			return Err(tg::error!("invalid scheme"));
		}
		let path = Path::new(uri.path().as_str());

		// Handle a path in the library temp.
		if let Ok(path) = path.strip_prefix(&self.library_path) {
			let kind = tg::module::Kind::Dts;
			let item = tg::module::data::Item::Path(path.to_owned());
			let referent = tg::Referent::with_item(item);
			let module = tg::module::Data { kind, referent };
			return Ok(module);
		}

		// Handle a path in the cache directory.
		if let Ok(path) = path.strip_prefix(&self.cache_path) {
			#[allow(clippy::case_sensitive_file_extension_comparisons)]
			let kind = if path.extension().is_some_and(|extension| extension == "js") {
				tg::module::Kind::Js
			} else if path.extension().is_some_and(|extension| extension == "ts") {
				tg::module::Kind::Ts
			} else {
				let metadata = tokio::fs::symlink_metadata(path)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
				if metadata.is_dir() {
					tg::module::Kind::Directory
				} else if metadata.is_file() {
					tg::module::Kind::File
				} else if metadata.is_symlink() {
					tg::module::Kind::Symlink
				} else {
					return Err(tg::error!("expected a directory, file, or symlink"));
				}
			};
			let object = path
				.components()
				.next()
				.ok_or_else(|| tg::error!("invalid path"))?
				.as_os_str()
				.to_str()
				.ok_or_else(|| tg::error!("invalid path"))?
				.parse::<tg::object::Id>()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let path = path.components().skip(1).collect::<PathBuf>();
			let object = if path.as_os_str().is_empty() {
				object
			} else {
				let directory = tg::Object::with_id(object)
					.try_unwrap_directory()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				directory.get(&self.handle, path).await?.id().into()
			};
			let item = tg::module::data::Item::Object(object);
			let referent = tg::Referent::with_item(item);
			let module = tg::module::Data { kind, referent };
			return Ok(module);
		}

		// Create the module.
		let file_name = path
			.file_name()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?
			.to_str()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?;
		if !tg::package::is_module_path(file_name.as_ref()) {
			return Err(tg::error!(%path = path.display(), "expected a module path"));
		}
		let kind = tg::package::module_kind_for_path(file_name)?;
		let module = tg::module::Data {
			kind,
			referent: tg::Referent::with_item(tg::module::data::Item::Path(path.to_owned())),
		};

		Ok(module)
	}

	async fn lsp_uri_for_module(&self, module: &tg::module::Data) -> tg::Result<lsp::Uri> {
		match module {
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
				..
			} => {
				let path = path.strip_prefix("./").unwrap_or(path);
				let path = self.library_path.join(path);
				let exists = tokio::fs::try_exists(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to stat the path"))?;
				if !exists {
					tokio::fs::create_dir_all(&self.library_path)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed create the library temp directory")
						})?;
					let contents = tangram_module::load::LIBRARY
						.get_file(&path)
						.ok_or_else(|| tg::error!("invalid path"))?
						.contents();
					tokio::fs::write(&path, contents)
						.await
						.map_err(|source| tg::error!(!source, "failed to write the library"))?;
					let metadata = tokio::fs::metadata(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to write the library"))?;
					let mut permissions = metadata.permissions();
					permissions.set_readonly(true);
					tokio::fs::set_permissions(&path, permissions)
						.await
						.map_err(|source| tg::error!(!source, "failed to write the library"))?;
				}
				let path = path.display();
				let uri = format!("file://{path}").parse().unwrap();
				Ok(uri)
			},

			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Object(object),
						..
					},
				..
			} => {
				let artifact = object
					.clone()
					.try_into()
					.map_err(|_| tg::error!("expected an artifact"))?;
				let arg = tg::cache::Arg {
					artifacts: vec![artifact],
				};
				self.handle
					.cache(arg)
					.await?
					.map_ok(|_| ())
					.try_collect::<()>()
					.await?;
				let path = self.cache_path.join(object.to_string());
				let uri = format!("file://{}", path.display()).parse().unwrap();
				Ok(uri)
			},

			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
				..
			} => {
				let path = path.display();
				let uri = format!("file://{path}").parse().unwrap();
				Ok(uri)
			},
		}
	}

	/// Load a module.
	pub async fn load_module(&self, module: &tg::module::Data) -> tg::Result<String> {
		// If there is an opened document, then return its contents.
		if let Some(document) = self.documents.get(module)
			&& document.open
		{
			return Ok(document.text.clone().unwrap());
		}

		// Otherwise, load the module.
		let module = tangram_module::load(&self.handle, module).await?;

		Ok(module)
	}
}

impl Deref for Handle {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Deref for Inner {
	type Target = Compiler;

	fn deref(&self) -> &Self::Target {
		&self.compiler
	}
}

impl Deref for Compiler {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for Handle {
	fn drop(&mut self) {
		self.compiler
			.typescript_request_sender
			.lock()
			.unwrap()
			.take();
	}
}
