use self::syscall::syscall;
use crate::{
	util::http::{empty, full, Incoming, Outgoing},
	Http,
};
use derive_more::Unwrap;
use futures::{future, Future, FutureExt, TryFutureExt};
use http_body_util::BodyExt;
use lsp_types as lsp;
use std::{
	collections::{BTreeSet, HashMap},
	path::{Path, PathBuf},
	pin::pin,
	sync::Arc,
};
use tangram_client as tg;
use tokio::io::{
	AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};
use url::Url;

pub mod analyze;
pub mod check;
pub mod completion;
pub mod definition;
pub mod diagnostics;
pub mod doc;
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

const SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/language.heapsnapshot"));

pub const SOURCE_MAP: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/language.js.map"));

type _Receiver = tokio::sync::mpsc::UnboundedReceiver<jsonrpc::Message>;
type Sender = tokio::sync::mpsc::UnboundedSender<jsonrpc::Message>;

#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	/// The published diagnostics.
	diagnostics: tokio::sync::RwLock<Vec<tg::Diagnostic>>,

	/// The documents.
	documents: tokio::sync::RwLock<HashMap<tg::Document, tg::document::State, fnv::FnvBuildHasher>>,

	/// A handle to the main tokio runtime.
	main_runtime_handle: tokio::runtime::Handle,

	/// The request receiver.
	request_receiver: std::sync::Mutex<Option<RequestReceiver>>,

	/// The request sender.
	request_sender: RequestSender,

	/// The Tangram server.
	server: crate::Server,

	/// The request handler thread.
	thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,

	/// The workspaces.
	workspaces: tokio::sync::RwLock<BTreeSet<PathBuf>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "request")]
pub enum Request {
	Check(check::Request),
	Completion(completion::Request),
	Definition(definition::Request),
	Diagnostics(diagnostics::Request),
	Doc(doc::Request),
	Hover(hover::Request),
	References(references::Request),
	Rename(rename::Request),
	Symbols(symbols::Request),
}

#[derive(Debug, serde::Deserialize, Unwrap)]
#[serde(rename_all = "snake_case", tag = "kind", content = "response")]
pub enum Response {
	Check(check::Response),
	Completion(completion::Response),
	Definition(definition::Response),
	Diagnostics(diagnostics::Response),
	Doc(doc::Response),
	Hover(hover::Response),
	References(references::Response),
	Rename(rename::Response),
	Symbols(symbols::Response),
}

pub type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
pub type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
pub type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;
pub type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<Response>>;

impl Server {
	#[must_use]
	pub fn new(server: &crate::Server, main_runtime_handle: tokio::runtime::Handle) -> Self {
		// Create the published diagnostics.
		let diagnostics = tokio::sync::RwLock::new(Vec::new());

		// Create the documents.
		let documents = tokio::sync::RwLock::new(HashMap::default());

		// Create the request sender and receiver.
		let (request_sender, request_receiver) =
			tokio::sync::mpsc::unbounded_channel::<(Request, ResponseSender)>();

		// Create the thread.
		let thread = std::sync::Mutex::new(None);

		// Create the workspaces.
		let workspaces = tokio::sync::RwLock::new(BTreeSet::new());

		// Create the inner.
		let inner = Arc::new(Inner {
			diagnostics,
			documents,
			main_runtime_handle,
			request_receiver: std::sync::Mutex::new(Some(request_receiver)),
			request_sender,
			server: server.clone(),
			thread,
			workspaces,
		});

		Self { inner }
	}

	pub async fn request(&self, request: Request) -> tg::Result<Response> {
		// Spawn the request handler thread if necessary.
		{
			let mut thread = self.inner.thread.lock().unwrap();
			if thread.is_none() {
				let request_receiver = self.inner.request_receiver.lock().unwrap().take().unwrap();
				thread.replace(std::thread::spawn({
					let server = self.clone();
					move || run_request_handler(server, request_receiver)
				}));
			}
		}

		// Create a oneshot channel for the response.
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

		// Send the request.
		self.inner
			.request_sender
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

	pub async fn serve(
		self,
		input: impl AsyncRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		let mut input = tokio::io::BufReader::new(input);
		let mut output = tokio::io::BufWriter::new(output);

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

		// Read incoming messages.
		loop {
			// Read a message.
			let message = read_incoming_message(&mut input).await?;

			// If the message is the exit notification, then break.
			if matches!(message,
				jsonrpc::Message::Notification(jsonrpc::Notification {
					ref method,
					..
				}) if method == <lsp::notification::Exit as lsp::notification::Notification>::METHOD
			) {
				break;
			};

			// Spawn a task to handle the message.
			tokio::spawn({
				let server = self.clone();
				let sender = outgoing_message_sender.clone();
				async move {
					handle_message(&server, &sender, message).await;
				}
			});
		}

		// Wait for the outgoing message task to complete.
		outgoing_message_task.await.unwrap()?;

		Ok(())
	}

	pub async fn module_for_url(&self, url: &Url) -> tg::Result<tg::Module> {
		match url.scheme() {
			"file" => {
				// Find the package path by searching the path's ancestors for a root module.
				let path = Path::new(url.path());
				let mut found = false;
				let mut package_path = path.to_owned();
				while package_path.pop() {
					for root_module_file_name in tg::package::ROOT_MODULE_FILE_NAMES {
						if tokio::fs::try_exists(&package_path.join(root_module_file_name))
							.await
							.map_err(|error| {
								tg::error!(source = error, "failed to determine if the path exists")
							})? {
							found = true;
							break;
						}
					}
				}
				if !found {
					let path = path.display();
					return Err(tg::error!(%path, "could not find the package"));
				}

				// Get the module path by stripping the package path.
				let module_path: tg::Path = path
					.strip_prefix(&package_path)
					.unwrap()
					.to_owned()
					.into_os_string()
					.into_string()
					.ok()
					.ok_or_else(|| {
						let path = path.display();
						tg::error!(%path, "the module path was not valid UTF-8")
					})?
					.parse()
					.map_err(|error| {
						let path = path.display();
						tg::error!(source = error, %path, "failed to parse the module path")
					})?;

				// Get or create the document.
				let document = self.get_document(package_path, module_path).await?;

				// Create the module.
				let module = tg::Module::Document(document);

				Ok(module)
			},
			_ => url.clone().try_into(),
		}
	}

	#[allow(clippy::unused_self)]
	#[must_use]
	pub fn url_for_module(&self, module: &tg::Module) -> Url {
		match module {
			tg::Module::Document(document) => {
				let path = document.package_path.join(document.path.to_string());
				let path = path.display();
				format!("file://{path}").parse().unwrap()
			},
			_ => module.clone().into(),
		}
	}
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

async fn handle_message(server: &Server, sender: &Sender, message: jsonrpc::Message) {
	match message {
		// Handle a request.
		jsonrpc::Message::Request(request) => {
			match request.method.as_str() {
				<lsp::request::Completion as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::Completion, _, _>(sender, request, |params| {
						server.handle_completion_request(params)
					})
					.boxed()
				},

				<lsp::request::DocumentSymbolRequest as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::DocumentSymbolRequest, _, _>(
						sender,
						request,
						|params| server.handle_symbols_request(params),
					)
					.boxed()
				},

				<lsp::request::GotoDefinition as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::GotoDefinition, _, _>(
						sender,
						request,
						|params| server.handle_definition_request(params),
					)
					.boxed()
				},

				<lsp::request::Formatting as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::Formatting, _, _>(sender, request, |params| {
						server.handle_format_request(params)
					})
					.boxed()
				},

				<lsp::request::HoverRequest as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::HoverRequest, _, _>(sender, request, |params| {
						server.handle_hover_request(params)
					})
					.boxed()
				},

				<lsp::request::Initialize as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::Initialize, _, _>(
						sender,
						request,
						|params| server.handle_initialize_request(params),
					)
					.boxed()
				},

				<lsp::request::References as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::References, _, _>(sender, request, |params| {
						server.handle_references_request(params)
					})
					.boxed()
				},

				<lsp::request::Rename as lsp::request::Request>::METHOD => {
					handle_request::<lsp::request::Rename, _, _>(sender, request, |params| {
						server.handle_rename_request(params)
					})
					.boxed()
				},

				<lsp::request::Shutdown as lsp::request::Request>::METHOD => handle_request::<lsp::request::Shutdown, _, _>(
					sender,
					request,
					|()| async move { Ok::<_, tg::Error>(()) },
				)
				.boxed(),

				<self::virtual_text_document::VirtualTextDocument as lsp::request::Request>::METHOD => {
					handle_request::<self::virtual_text_document::VirtualTextDocument, _, _>(
						sender,
						request,
						|params| server.handle_virtual_text_document_request(params),
					)
					.boxed()
				},

				// If the request method does not have a handler, then send a method not found response.
				_ => {
					let error = jsonrpc::ResponseError {
						code: jsonrpc::ResponseErrorCode::MethodNotFound,
						message: "method not found".to_owned(),
					};
					send_response::<()>(sender, request.id, None, Some(error));
					future::ready(()).boxed()
				},
			}
			.await;
		},

		// Handle a response.
		jsonrpc::Message::Response(_) => (),

		// Handle a notification.
		jsonrpc::Message::Notification(notification) => {
			match notification.method.as_str() {
				<lsp::notification::DidOpenTextDocument as lsp::notification::Notification>::METHOD => {
					handle_notification::<lsp::notification::DidOpenTextDocument, _, _>(
						sender,
						notification,
						|sender, params| server.handle_did_open_notification(sender, params),
					)
					.boxed()
				},

				<lsp::notification::DidChangeTextDocument as lsp::notification::Notification>::METHOD => {
					handle_notification::<lsp::notification::DidChangeTextDocument, _, _>(
						sender,
						notification,
						|sender, params| server.handle_did_change_notification(sender, params),
					)
					.boxed()
				},

				<lsp::notification::DidCloseTextDocument as lsp::notification::Notification>::METHOD => {
					handle_notification::<lsp::notification::DidCloseTextDocument, _, _>(
						sender,
						notification,
						|sender, params| server.handle_did_close_notification(sender, params),
					)
					.boxed()
				},

				<lsp::notification::DidSaveTextDocument as lsp::notification::Notification>::METHOD => {
					handle_notification::<lsp::notification::DidSaveTextDocument, _, _>(sender, notification, |sender, params| server.handle_did_save_notification(sender, params)).boxed()
				}

				<lsp::notification::DidChangeWorkspaceFolders as lsp::notification::Notification>::METHOD => {
					handle_notification::<lsp::notification::DidChangeWorkspaceFolders, _, _>(sender, notification, |sender, params| server.handle_did_change_workspace_folders(sender, params)).boxed()
				}

				// If the notification method does not have a handler, then do nothing.
				_ => future::ready(()).boxed(),
			}
			.await;
		},
	}
}

async fn handle_request<T, F, Fut>(sender: &Sender, request: jsonrpc::Request, handler: F)
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
		send_response::<()>(sender, request.id, None, Some(error));
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
	send_response(sender, request.id, result, error);
}

async fn handle_notification<T, F, Fut>(sender: &Sender, request: jsonrpc::Notification, handler: F)
where
	T: lsp::notification::Notification,
	F: Fn(Sender, T::Params) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let params = serde_json::from_value(request.params.unwrap_or(serde_json::Value::Null))
		.map_err(|source| tg::error!(!source, "failed to deserialize the request params"))
		.unwrap();
	handler(sender.clone(), params)
		.await
		.inspect_err(|error| {
			tracing::error!("{error}");
		})
		.ok();
}

fn send_response<T>(
	sender: &Sender,
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
	sender.send(message).ok();
}

fn send_notification<T>(sender: &Sender, params: T::Params)
where
	T: lsp::notification::Notification,
{
	let params = serde_json::to_value(params).unwrap();
	let message = jsonrpc::Message::Notification(jsonrpc::Notification {
		jsonrpc: jsonrpc::VERSION.to_owned(),
		method: T::METHOD.to_owned(),
		params: Some(params),
	});
	sender.send(message).ok();
}

/// Run the request handler.
fn run_request_handler(server: Server, mut request_receiver: RequestReceiver) {
	// Create the isolate.
	let params = v8::CreateParams::default().snapshot_blob(SNAPSHOT);
	let mut isolate = v8::Isolate::new(params);

	// Set the prepare stack trace callback.
	isolate.set_prepare_stack_trace_callback(self::error::prepare_stack_trace_callback);

	// Create the context.
	let scope = &mut v8::HandleScope::new(&mut isolate);
	let context = v8::Context::new(scope);
	let scope = &mut v8::ContextScope::new(scope, context);

	// Set the server on the context.
	context.set_slot(scope, server);

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
	let language = v8::String::new_external_onebyte_static(scope, "language".as_bytes()).unwrap();
	let language = global.get(scope, language.into()).unwrap();
	let language = v8::Local::<v8::Object>::try_from(language).unwrap();
	let handle = v8::String::new_external_onebyte_static(scope, "handle".as_bytes()).unwrap();
	let handle = language.get(scope, handle.into()).unwrap();
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
		let response = handle.call(scope, receiver, &[request]).unwrap();
		let response = v8::Local::<v8::Promise>::try_from(response).unwrap();

		let response = match response.state() {
			v8::PromiseState::Pending => unreachable!(),
			v8::PromiseState::Fulfilled => response.result(scope),
			v8::PromiseState::Rejected => {
				let exception = response.result(scope);
				let error = error::from_exception(scope, exception);
				response_sender.send(Err(error)).unwrap();
				continue;
			},
		};

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

impl crate::Server {
	pub async fn format(&self, text: String) -> tg::Result<String> {
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());
		let text = language_server.format(text).await?;
		Ok(text)
	}

	pub async fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		let language_server = crate::language::Server::new(self, tokio::runtime::Handle::current());
		language_server.serve(input, output).await?;
		Ok(())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_format_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the body"))?
			.to_bytes();
		let text = String::from_utf8(bytes.to_vec())
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Format the text.
		let text = self.inner.tg.format(text).await?;

		// Create the body.
		let body = full(text.into_bytes());

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn handle_lsp_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		if !request
			.headers()
			.get(http::header::UPGRADE)
			.is_some_and(|value| value == "lsp")
		{
			return Err(tg::error!("expected an upgrade header"));
		}

		let server = self.clone();
		let mut stop = request
			.extensions()
			.get::<tokio::sync::watch::Receiver<bool>>()
			.cloned()
			.unwrap();
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|source| tg::error!(!source, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = Box::new(input);
				let output = Box::new(output);
				let task = server.inner.tg.lsp(input, output);
				let stop = stop.wait_for(|stop| *stop);
				future::select(pin!(task), pin!(stop))
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
			.header(http::header::UPGRADE, "lsp")
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
