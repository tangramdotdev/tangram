use self::syscall::syscall;
pub use self::{
	diagnostic::Diagnostic, document::Document, import::Import, location::Location, module::Module,
	position::Position, range::Range,
};
use derive_more::Unwrap;
use futures::{future, Future, FutureExt};
use lsp_types as lsp;
use std::{
	collections::{BTreeSet, HashMap},
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use url::Url;

pub mod analyze;
pub mod check;
pub mod completion;
pub mod definition;
pub mod diagnostic;
pub mod diagnostics;
pub mod docs;
pub mod document;
pub mod error;
pub mod format;
pub mod hover;
pub mod import;
pub mod initialize;
pub mod jsonrpc;
pub mod load;
pub mod location;
pub mod module;
pub mod parse;
pub mod position;
pub mod range;
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
	/// The Tangram handle.
	tg: Box<dyn tg::Handle>,

	/// The published diagnostics.
	diagnostics: Arc<tokio::sync::RwLock<Vec<Diagnostic>>>,

	/// The document store.
	document_store: document::Store,

	/// The request sender.
	request_sender: RequestSender,

	/// A handle to the main tokio runtime.
	main_runtime_handle: tokio::runtime::Handle,

	/// The workspaces.
	workspaces: Arc<tokio::sync::RwLock<BTreeSet<PathBuf>>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "request")]
pub enum Request {
	Check(check::Request),
	Completion(completion::Request),
	Definition(definition::Request),
	Diagnostics(diagnostics::Request),
	Docs(docs::Request),
	Format(format::Request),
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
	Docs(docs::Response),
	Format(format::Response),
	Hover(hover::Response),
	References(references::Response),
	Rename(rename::Response),
	Symbols(symbols::Response),
}

pub type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
pub type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
pub type ResponseSender = tokio::sync::oneshot::Sender<Result<Response>>;
pub type _ResponseReceiver = tokio::sync::oneshot::Receiver<Result<Response>>;

impl Server {
	#[must_use]
	pub fn new(tg: &dyn tg::Handle, main_runtime_handle: tokio::runtime::Handle) -> Self {
		// Create the published diagnostics.
		let diagnostics = Arc::new(tokio::sync::RwLock::new(Vec::new()));

		// Create the document store.
		let document_store = document::Store::default();

		// Create the request sender and receiver.
		let (request_sender, request_receiver) =
			tokio::sync::mpsc::unbounded_channel::<(Request, ResponseSender)>();

		// Create the workspaces.
		let workspaces = Arc::new(tokio::sync::RwLock::new(BTreeSet::new()));

		// Create the inner.
		let inner = Arc::new(Inner {
			tg: tg.clone_box(),
			diagnostics,
			document_store,
			request_sender,
			main_runtime_handle,
			workspaces,
		});

		// Create the server
		let server = Self { inner };

		// Spawn a thread to handle requests.
		std::thread::spawn({
			let server = server.clone();
			move || run_request_handler(server, request_receiver)
		});

		server
	}

	pub async fn request(&self, request: Request) -> Result<Response> {
		// Create a oneshot channel for the response.
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

		// Send the request.
		self.inner
			.request_sender
			.send((request, response_sender))
			.wrap_err("Failed to send the request.")?;

		// Receive the response.
		let response = response_receiver
			.await
			.wrap_err("Failed to receive a response for the request.")??;

		Ok(response)
	}

	pub async fn serve(self) -> Result<()> {
		let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());

		// Create a channel to send outgoing messages.
		let (outgoing_message_sender, mut outgoing_message_receiver) =
			tokio::sync::mpsc::unbounded_channel::<jsonrpc::Message>();

		// Create a task to send outgoing messages.
		let outgoing_message_task = tokio::spawn(async move {
			while let Some(outgoing_message) = outgoing_message_receiver.recv().await {
				let body = serde_json::to_string(&outgoing_message)
					.wrap_err("Failed to serialize the message.")?;
				let head = format!("Content-Length: {}\r\n\r\n", body.len());
				stdout
					.write_all(head.as_bytes())
					.await
					.wrap_err("Failed to write the head.")?;
				stdout
					.write_all(body.as_bytes())
					.await
					.wrap_err("Failed to write the body.")?;
				stdout.flush().await.wrap_err("Failed to flush stdout.")?;
			}
			Ok::<_, Error>(())
		});

		// Read incoming messages.
		loop {
			// Read a message.
			let message = read_incoming_message(&mut stdin).await?;

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

	pub async fn module_for_url(&self, url: &Url) -> Result<Module> {
		match url.scheme() {
			"file" => {
				let document =
					Document::for_path(&self.inner.document_store, Path::new(url.path())).await?;
				let module = Module::Document(document);
				Ok(module)
			},
			_ => url.clone().try_into(),
		}
	}

	#[must_use]
	pub fn url_for_module(&self, module: &Module) -> Url {
		match module {
			Module::Document(document) => {
				let path = document.package_path.join(document.path.to_string());
				let path = path.display();
				format!("file://{path}").parse().unwrap()
			},
			_ => module.clone().into(),
		}
	}
}

async fn read_incoming_message<R>(reader: &mut R) -> Result<jsonrpc::Message>
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
			.wrap_err("Failed to read a line.")?;
		if n == 0 {
			break;
		}
		if !line.ends_with("\r\n") {
			return Err(error!("Unexpected line ending."));
		}
		let line = &line[..line.len() - 2];
		if line.is_empty() {
			break;
		}
		let mut components = line.split(": ");
		let key = components.next().wrap_err("Expected a header name.")?;
		let value = components.next().wrap_err("Expected a header value.")?;
		headers.insert(key.to_owned(), value.to_owned());
	}

	// Read and deserialize the message.
	let content_length: usize = headers
		.get("Content-Length")
		.wrap_err("Expected a Content-Length header.")?
		.parse()
		.wrap_err("Failed to parse the Content-Length header value.")?;
	let mut message: Vec<u8> = vec![0; content_length];
	reader
		.read_exact(&mut message)
		.await
		.wrap_err("Failed to read the message.")?;
	let message =
		serde_json::from_slice(&message).wrap_err("Failed to deserialize the message.")?;

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
					|()| async move { Ok(()) },
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
						message: "Method not found.".to_owned(),
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
	Fut: Future<Output = crate::Result<T::Result>>,
{
	// Deserialize the params.
	let Ok(params) = serde_json::from_value(request.params.unwrap_or(serde_json::Value::Null))
	else {
		let error = jsonrpc::ResponseError {
			code: jsonrpc::ResponseErrorCode::InvalidParams,
			message: "Invalid params.".to_owned(),
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
	Fut: Future<Output = crate::Result<()>>,
{
	let params = serde_json::from_value(request.params.unwrap_or(serde_json::Value::Null))
		.wrap_err("Failed to deserialize the request params.")
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
		let request =
			match serde_v8::to_v8(scope, &request).wrap_err("Failed to serialize the request.") {
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
			.wrap_err("Failed to deserialize the response.")
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
