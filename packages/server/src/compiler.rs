use self::{document::Document, syscall::syscall};
use crate::Server;
use dashmap::DashMap;
use futures::{future, Future, FutureExt as _, TryFutureExt as _};
use lsp::{notification::Notification as _, request::Request as _};
use lsp_types as lsp;
use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	path::PathBuf,
	pin::pin,
	sync::{Arc, Mutex},
};
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::{
	AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
};

pub mod analysis;
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
pub mod load;
pub mod parse;
pub mod references;
pub mod rename;
pub mod resolve;
pub mod symbols;
pub mod syscall;
pub mod transpile;
pub mod version;
pub mod virtual_text_document;
pub mod workspace;

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/compiler.heapsnapshot"));

const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/compiler.js.map"));

#[derive(Clone)]
pub struct Compiler(Arc<Inner>);

pub struct Inner {
	/// The published diagnostics.
	diagnostics: tokio::sync::RwLock<BTreeMap<tg::Module, Vec<tg::Diagnostic>>>,

	/// The documents.
	documents: DashMap<tg::Module, Document, fnv::FnvBuildHasher>,

	/// A handle to the main tokio runtime.
	main_runtime_handle: tokio::runtime::Handle,

	/// The request sender.
	request_sender: Mutex<Option<RequestSender>>,

	/// The request thread.
	request_thread: Mutex<Option<std::thread::JoinHandle<()>>>,

	/// The sender.
	sender: std::sync::RwLock<Option<tokio::sync::mpsc::UnboundedSender<jsonrpc::Message>>>,

	/// The server.
	server: Server,

	/// The workspaces.
	workspaces: tokio::sync::RwLock<BTreeSet<PathBuf>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "request")]
enum Request {
	Check(check::Request),
	Completion(completion::Request),
	Definition(definition::Request),
	TypeDefinition(definition::Request),
	Diagnostics(diagnostics::Request),
	Document(document::Request),
	Hover(hover::Request),
	References(references::Request),
	Rename(rename::Request),
	Symbols(symbols::Request),
}

#[derive(Debug, derive_more::Unwrap, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "response")]
enum Response {
	Check(check::Response),
	Completion(completion::Response),
	Definition(definition::Response),
	TypeDefinition(definition::Response),
	Diagnostics(diagnostics::Response),
	Document(document::Response),
	Hover(hover::Response),
	References(references::Response),
	Rename(rename::Response),
	Symbols(symbols::Response),
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<Response>>;

impl Compiler {
	#[must_use]
	pub fn new(server: &crate::Server, main_runtime_handle: tokio::runtime::Handle) -> Self {
		// Create the published diagnostics.
		let diagnostics = tokio::sync::RwLock::new(BTreeMap::new());

		// Create the documents.
		let documents = DashMap::default();

		// Create the request sender.
		let request_sender = Mutex::new(None);

		// Create the request thread.
		let request_thread = Mutex::new(None);

		// Create the sender.
		let sender = std::sync::RwLock::new(None);

		// Create the workspaces.
		let workspaces = tokio::sync::RwLock::new(BTreeSet::new());

		// Create the server.
		Self(Arc::new(Inner {
			diagnostics,
			documents,
			main_runtime_handle,
			request_sender,
			request_thread,
			sender,
			server: server.clone(),
			workspaces,
		}))
	}

	pub async fn serve(
		self,
		mut input: impl AsyncBufRead + Send + Unpin + 'static,
		mut output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
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
			let message = Self::read_incoming_message(&mut input).await?;

			// If the message is the exit notification, then break.
			if matches!(&message,
				jsonrpc::Message::Notification(jsonrpc::Notification {
					method,
					..
				}) if method == <lsp::notification::Exit as lsp::notification::Notification>::METHOD
			) {
				break;
			};

			// Spawn a task to handle the message.
			tokio::spawn({
				let server = self.clone();
				async move {
					server.handle_message(message).await;
				}
			});
		}

		// Drop the outgoing message sender.
		self.sender.write().unwrap().take().unwrap();

		// Wait for the outgoing message task to complete.
		outgoing_message_task.await.unwrap()?;

		Ok(())
	}

	async fn read_incoming_message<R>(reader: &mut R) -> tg::Result<jsonrpc::Message>
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
				break;
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

	async fn handle_message(&self, message: jsonrpc::Message) {
		match message {
			// Handle a request.
			jsonrpc::Message::Request(request) => {
				self.handle_request(request).await;
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

			lsp::request::DocumentSymbolRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentSymbolRequest, _, _>(
					request,
					|params| self.handle_symbols_request(params),
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

			self::virtual_text_document::VirtualTextDocument::METHOD => self
				.handle_request_with::<self::virtual_text_document::VirtualTextDocument, _, _>(
					request,
					|params| self.handle_virtual_text_document_request(params),
				)
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
				let options = tg::error::TraceOptions {
					internal: true,
					..Default::default()
				};
				let trace = error.trace(&options);
				tracing::error!("{error} {trace}");
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
		// Spawn the request handler thread if necessary.
		{
			let mut request_thread = self.request_thread.lock().unwrap();
			if request_thread.is_none() {
				let (request_sender, request_receiver) =
					tokio::sync::mpsc::unbounded_channel::<(Request, ResponseSender)>();
				self.request_sender.lock().unwrap().replace(request_sender);
				request_thread.replace(std::thread::spawn({
					let server = self.clone();
					move || Self::run_request_handler(server, request_receiver)
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

	/// Run the request handler.
	fn run_request_handler(self, mut request_receiver: RequestReceiver) {
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
		context.set_slot(self);

		// Add the syscall function to the global.
		let syscall_string =
			v8::String::new_external_onebyte_static(scope, "syscall".as_bytes()).unwrap();
		let syscall_function = v8::Function::new(scope, syscall).unwrap();
		context
			.global(scope)
			.set(scope, syscall_string.into(), syscall_function.into())
			.unwrap();

		// Get the handle function.
		let global = context.global(scope);
		let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
		let tg = global.get(scope, tg.into()).unwrap();
		let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();
		let handle = v8::String::new_external_onebyte_static(scope, "handle".as_bytes()).unwrap();
		let handle = tg.get(scope, handle.into()).unwrap();
		let handle = v8::Local::<v8::Function>::try_from(handle).unwrap();

		while let Some((request, response_sender)) = request_receiver.blocking_recv() {
			// Create a try catch scope.
			let scope = &mut v8::TryCatch::new(scope);

			// Serialize the request.
			let request = match serde_v8::to_v8(scope, &request)
				.map_err(|source| tg::error!(!source, "failed to serialize the request"))
			{
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
			let response = match serde_v8::from_v8(scope, response)
				.map_err(|source| tg::error!(!source, "failed to deserialize the response"))
			{
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

	async fn module_for_uri(&self, uri: &lsp::Uri) -> tg::Result<tg::Module> {
		match uri.scheme().unwrap().as_str() {
			"file" => {
				let path = uri.path().as_str().parse::<tg::Path>()?;
				#[allow(clippy::case_sensitive_file_extension_comparisons)]
				let kind = if path.as_str().ends_with(".d.ts") {
					tg::module::Kind::Dts
				} else if path.as_str().ends_with(".js") {
					tg::module::Kind::Js
				} else if path.as_str().ends_with(".ts") {
					tg::module::Kind::Ts
				} else {
					tg::module::Kind::Artifact
				};
				let object = tg::module::Object::Path(path);
				Ok(tg::Module { kind, object })
			},

			_ => uri.as_str().parse(),
		}
	}

	#[allow(clippy::unused_self)]
	#[must_use]
	fn uri_for_module(&self, module: &tg::Module) -> lsp::Uri {
		match module {
			tg::Module {
				kind:
					tg::module::Kind::Js
					| tg::module::Kind::Ts
					| tg::module::Kind::Artifact
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink,
				object: tg::module::Object::Path(path),
				..
			} => format!("file://{path}").parse().unwrap(),
			module => module.to_string().parse().unwrap(),
		}
	}
}

impl crate::Server {
	pub async fn format(&self, text: String) -> tg::Result<String> {
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());
		let text = compiler.format(text).await?;
		Ok(text)
	}

	pub async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		let compiler = crate::compiler::Compiler::new(self, tokio::runtime::Handle::current());
		compiler.serve(input, output).await?;
		Ok(())
	}
}

impl crate::Server {
	pub(crate) async fn handle_lsp_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Ensure the connection header is set correctly.
		if !request
			.headers()
			.get(http::header::CONNECTION)
			.is_some_and(|value| value == "upgrade")
		{
			return Err(tg::error!("expected connection header set to upgrade"));
		}

		// Ensure the upgrade header is set correctly.
		if !request
			.headers()
			.get(http::header::UPGRADE)
			.is_some_and(|value| value == "lsp")
		{
			return Err(tg::error!("expected upgrade header set to lsp"));
		}

		// Spawn the LSP.
		let handle = handle.clone();
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|source| tg::error!(!source, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = tokio::io::BufReader::new(input);
				let task = handle.lsp(input, output);
				future::select(pin!(task), pin!(stop.stopped()))
					.map(|output| match output {
						future::Either::Left((Err(error), _)) => Err(error),
						_ => Ok(()),
					})
					.await?;
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| tracing::error!(?error)),
		);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::SWITCHING_PROTOCOLS)
			.header(http::header::CONNECTION, "upgrade")
			.header(http::header::UPGRADE, "lsp")
			.empty()
			.unwrap();

		Ok(response)
	}
}

impl std::ops::Deref for Compiler {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
