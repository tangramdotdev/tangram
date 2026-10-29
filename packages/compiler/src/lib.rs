use {
	self::document::Document,
	dashmap::DashMap,
	futures::{FutureExt as _, future},
	lsp_types::{self as lsp, notification::Notification as _, request::Request as _},
	std::{
		collections::{BTreeSet, HashMap},
		ops::Deref,
		path::{Path, PathBuf},
		pin::pin,
		sync::{Arc, Mutex, RwLock, atomic::AtomicI32},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tokio::io::{
		AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
	},
	tokio_util::task::TaskTracker,
};

mod backend;
#[cfg(all(feature = "javascript", feature = "python"))]
#[cfg_attr(not(any(feature = "tsrs", feature = "ty")), expect(dead_code))]
mod interfaces;
#[cfg(feature = "tsrs")]
mod javascript;
#[cfg(feature = "javascript")]
mod util;

pub mod analyze;
pub mod call_hierarchy;
pub mod check;
pub mod code_action;
pub mod completion;
pub mod definition;
pub mod diagnostics;
pub mod document;
pub mod document_highlight;
pub mod document_link;
pub mod folding_range;
pub mod format;
pub mod graphs;
pub mod hover;
pub mod implementation;
pub mod initialize;
pub mod inlay_hint;
pub mod interface;
pub mod jsonrpc;
pub mod load;
pub mod metadata;
pub mod prepare_rename;
#[cfg(feature = "python")]
pub mod python;
pub mod references;
pub mod rename;
pub mod resolve;
pub mod selection_range;
pub mod semantic_tokens;
pub mod signature_help;
pub mod symbols;
#[cfg(feature = "javascript")]
pub mod transpile;
pub mod version;
pub mod workspace;
pub mod workspace_symbols;

pub const LIBRARY: include_dir::Dir = include_dir::include_dir!("$OUT_DIR/lib");

#[derive(Clone)]
pub struct Shared(Arc<Owned>);

pub struct Owned {
	compiler: Compiler,
	task: tangram_futures::task::Shared<()>,
}

#[derive(Clone)]
pub struct Compiler(Arc<State>);

pub struct State {
	/// The documents, keyed by source identity.
	documents: DashMap<document::Key, Document, fnv::FnvBuildHasher>,

	/// The Tangram instance.
	instance: tg::instance::dynamic::Instance,

	/// The interfaces of the modules that the checkers load from other languages.
	#[cfg(all(feature = "javascript", feature = "python"))]
	interfaces: interfaces::Interfaces,

	/// The JavaScript service.
	#[cfg(feature = "tsrs")]
	javascript: javascript::Service,

	/// The library path.
	library_path: PathBuf,

	/// A handle to the main tokio runtime.
	#[cfg(any(feature = "tsrs", feature = "ty"))]
	main_runtime_handle: tokio::runtime::Handle,

	/// The position encoding negotiated with the LSP client.
	position_encoding: RwLock<tg::position::Encoding>,

	/// The Python service.
	#[cfg(feature = "ty")]
	python: python::Service,

	/// The outgoing request ID counter.
	request_id: AtomicI32,

	/// The pending outgoing requests.
	requests: DashMap<i32, tokio::sync::oneshot::Sender<jsonrpc::Response>>,

	/// The resolutions of the imports of each module, so that the checkers and preparing interfaces resolve each import once.
	resolutions: DashMap<document::Key, resolve::Resolutions, fnv::FnvBuildHasher>,

	/// The sender.
	sender: RwLock<Option<tokio::sync::mpsc::UnboundedSender<jsonrpc::Message>>>,

	/// The serve task.
	serve_task: Mutex<Option<tangram_futures::task::Shared<()>>>,

	/// The store path.
	store_path: PathBuf,

	/// The version.
	version: String,

	/// The workspaces.
	workspaces: tokio::sync::RwLock<BTreeSet<PathBuf>>,
}

impl Shared {
	pub fn stop(&self) {
		self.task.stop();
	}

	pub async fn wait(&self) -> tg::Result<()> {
		self.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the compiler task panicked"))
	}
}

impl Compiler {
	#[must_use]
	pub fn start(
		instance: tg::instance::dynamic::Instance,
		store_path: PathBuf,
		library_path: PathBuf,
		main_runtime_handle: tokio::runtime::Handle,
		version: String,
	) -> Shared {
		#[cfg(not(any(feature = "tsrs", feature = "ty")))]
		drop(main_runtime_handle);
		let documents = DashMap::default();
		let requests = DashMap::new();
		let resolutions = DashMap::default();
		let request_id = AtomicI32::new(1);
		let sender = std::sync::RwLock::new(None);
		let serve_task = Mutex::new(None);
		let workspaces = tokio::sync::RwLock::new(BTreeSet::new());

		// Create the compiler.
		let compiler = Self(Arc::new(State {
			documents,

			instance,

			#[cfg(all(feature = "javascript", feature = "python"))]
			interfaces: interfaces::Interfaces::default(),

			#[cfg(feature = "tsrs")]
			javascript: javascript::Service::new(),

			library_path,

			#[cfg(any(feature = "tsrs", feature = "ty"))]
			main_runtime_handle,

			position_encoding: RwLock::new(tg::position::Encoding::Utf8),

			#[cfg(feature = "ty")]
			python: python::Service::new(),

			request_id,

			requests,

			resolutions,

			sender,

			serve_task,

			store_path,

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

				#[cfg(feature = "tsrs")]
				{
					compiler.javascript.stop();
					compiler.javascript.join().await;
				}
				#[cfg(feature = "ty")]
				{
					compiler.python.stop();
					compiler.python.join().await;
				}
			}
		};

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn({
			move |stopper| async move {
				stopper.wait().await;
				shutdown.await;
			}
		});

		Shared(Arc::new(Owned { compiler, task }))
	}

	pub async fn serve(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		let task = tangram_futures::task::Shared::spawn(|stopper| {
			let compiler = self.clone();
			async move {
				compiler.serve_inner(input, output, stopper).await;
			}
		});
		self.serve_task.lock().unwrap().replace(task.clone());
		task.wait()
			.await
			.map_err(|error| tg::error!(!error, "the serve task panicked"))?;
		Ok(())
	}

	async fn serve_inner(
		&self,
		mut input: impl AsyncBufRead + Send + Unpin + 'static,
		mut output: impl AsyncWrite + Send + Unpin + 'static,
		stopper: Stopper,
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
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				let head = format!("Content-Length: {}\r\n\r\n", body.len());
				output
					.write_all(head.as_bytes())
					.await
					.map_err(|error| tg::error!(!error, "failed to write the head"))?;
				output
					.write_all(body.as_bytes())
					.await
					.map_err(|error| tg::error!(!error, "failed to write the body"))?;
				output
					.flush()
					.await
					.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
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
			let result = match future::select(pin!(read), pin!(stopper.wait())).await {
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

		// Wait for all tasks to finish.
		tasks.close();
		tasks.wait().await;

		// Drop the outgoing message sender.
		self.sender.write().unwrap().take().unwrap();

		// Wait for the outgoing message task to finish.
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
				.map_err(|error| tg::error!(!error, "failed to read a line"))?;
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
			.map_err(|error| tg::error!(!error, "failed to read the message"))?;
		let message = serde_json::from_slice(&message)
			.map_err(|error| tg::error!(!error, "failed to deserialize the message"))?;

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
			jsonrpc::Message::Response(response) => {
				// Get the ID.
				let id = match response.id {
					jsonrpc::Id::I32(id) => id,
					jsonrpc::Id::String(id) => {
						tracing::error!(%id, "received a response with a string id");
						return;
					},
				};

				// Look up the pending request and send the response.
				let Some((_, sender)) = self.requests.remove(&id) else {
					tracing::warn!(%id, "received response for unknown request");
					return;
				};
				sender.send(response).ok();
			},

			// Handle a notification.
			jsonrpc::Message::Notification(notification) => {
				self.handle_notification(notification).await;
			},
		}
	}

	async fn handle_request(&self, request: jsonrpc::Request) {
		match request.method.as_str() {
			lsp::request::CallHierarchyIncomingCalls::METHOD => self
				.handle_request_with::<lsp::request::CallHierarchyIncomingCalls, _, _>(
					request,
					|params| self.handle_call_hierarchy_incoming_calls_request(params),
				)
				.boxed(),

			lsp::request::CallHierarchyOutgoingCalls::METHOD => self
				.handle_request_with::<lsp::request::CallHierarchyOutgoingCalls, _, _>(
					request,
					|params| self.handle_call_hierarchy_outgoing_calls_request(params),
				)
				.boxed(),

			lsp::request::CallHierarchyPrepare::METHOD => self
				.handle_request_with::<lsp::request::CallHierarchyPrepare, _, _>(
					request,
					|params| self.handle_call_hierarchy_prepare_request(params),
				)
				.boxed(),

			lsp::request::CodeActionRequest::METHOD => self
				.handle_request_with::<lsp::request::CodeActionRequest, _, _>(request, |params| {
					self.handle_code_action_request(params)
				})
				.boxed(),

			lsp::request::CodeActionResolveRequest::METHOD => self
				.handle_request_with::<lsp::request::CodeActionResolveRequest, _, _>(
					request,
					|params| self.handle_code_action_resolve_request(params),
				)
				.boxed(),

			lsp::request::Completion::METHOD => self
				.handle_request_with::<lsp::request::Completion, _, _>(request, |params| {
					self.handle_completion_request(params)
				})
				.boxed(),

			lsp::request::ResolveCompletionItem::METHOD => self
				.handle_request_with::<lsp::request::ResolveCompletionItem, _, _>(
					request,
					|params| self.handle_completion_item_resolve_request(params),
				)
				.boxed(),

			lsp::request::DocumentDiagnosticRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentDiagnosticRequest, _, _>(
					request,
					|params| self.handle_document_diagnostic_request(params),
				)
				.boxed(),

			lsp::request::DocumentHighlightRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentHighlightRequest, _, _>(
					request,
					|params| self.handle_document_highlight_request(params),
				)
				.boxed(),

			lsp::request::DocumentLinkRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentLinkRequest, _, _>(request, |params| {
					self.handle_document_link_request(params)
				})
				.boxed(),

			lsp::request::DocumentLinkResolve::METHOD => self
				.handle_request_with::<lsp::request::DocumentLinkResolve, _, _>(request, |params| {
					self.handle_document_link_resolve_request(params)
				})
				.boxed(),

			lsp::request::DocumentSymbolRequest::METHOD => self
				.handle_request_with::<lsp::request::DocumentSymbolRequest, _, _>(
					request,
					|params| self.handle_document_symbol_request(params),
				)
				.boxed(),

			lsp::request::FoldingRangeRequest::METHOD => self
				.handle_request_with::<lsp::request::FoldingRangeRequest, _, _>(request, |params| {
					self.handle_folding_range_request(params)
				})
				.boxed(),

			lsp::request::GotoDeclaration::METHOD => self
				.handle_request_with::<lsp::request::GotoDeclaration, _, _>(request, |params| {
					self.handle_declaration_request(params)
				})
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

			lsp::request::GotoImplementation::METHOD => self
				.handle_request_with::<lsp::request::GotoImplementation, _, _>(request, |params| {
					self.handle_implementation_request(params)
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

			lsp::request::InlayHintRequest::METHOD => self
				.handle_request_with::<lsp::request::InlayHintRequest, _, _>(request, |params| {
					self.handle_inlay_hint_request(params)
				})
				.boxed(),

			lsp::request::SemanticTokensFullRequest::METHOD => self
				.handle_request_with::<lsp::request::SemanticTokensFullRequest, _, _>(
					request,
					|params| self.handle_semantic_tokens_full_request(params),
				)
				.boxed(),

			lsp::request::SignatureHelpRequest::METHOD => self
				.handle_request_with::<lsp::request::SignatureHelpRequest, _, _>(
					request,
					|params| self.handle_signature_help_request(params),
				)
				.boxed(),

			lsp::request::Initialize::METHOD => self
				.handle_request_with::<lsp::request::Initialize, _, _>(request, |params| {
					self.handle_initialize_request(params)
				})
				.boxed(),

			lsp::request::PrepareRenameRequest::METHOD => self
				.handle_request_with::<lsp::request::PrepareRenameRequest, _, _>(
					request,
					|params| self.handle_prepare_rename_request(params),
				)
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

			lsp::request::SelectionRangeRequest::METHOD => self
				.handle_request_with::<lsp::request::SelectionRangeRequest, _, _>(
					request,
					|params| self.handle_selection_range_request(params),
				)
				.boxed(),

			lsp::request::Shutdown::METHOD => self
				.handle_request_with::<lsp::request::Shutdown, _, _>(request, |()| async move {
					Ok::<_, tg::Error>(())
				})
				.boxed(),

			lsp::request::WorkspaceSymbolRequest::METHOD => self
				.handle_request_with::<lsp::request::WorkspaceSymbolRequest, _, _>(
					request,
					|params| self.handle_workspace_symbol_request(params),
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
			.map_err(|error| tg::error!(!error, "failed to deserialize the request params"))
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

	#[expect(dead_code)]
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

	fn send_request<T>(
		&self,
		params: T::Params,
	) -> impl Future<Output = Result<T::Result, tg::Error>>
	where
		T: lsp::request::Request,
		T::Result: serde::de::DeserializeOwned,
	{
		// Generate a unique request ID.
		let id = self
			.request_id
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

		// Create a oneshot channel for the response.
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

		// Store the response sender.
		self.requests.insert(id, response_sender);

		// Send the request.
		let params = serde_json::to_value(params).unwrap();
		let message = jsonrpc::Message::Request(jsonrpc::Request {
			jsonrpc: jsonrpc::VERSION.to_owned(),
			id: jsonrpc::Id::I32(id),
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

		// Return a future that resolves when the response arrives.
		async move {
			let response = response_receiver
				.await
				.map_err(|_| tg::error!("failed to receive response"))?;

			// Check for error in the response.
			if let Some(error) = response.error {
				return Err(tg::error!("request failed: {}", error.message));
			}

			// Deserialize the result.
			let result = response.result.unwrap_or(serde_json::Value::Null);
			let result = serde_json::from_value(result)
				.map_err(|error| tg::error!(!error, "failed to deserialize response"))?;

			Ok(result)
		}
	}

	/// Get the backend for modules of a kind.
	#[expect(clippy::unused_self)]
	fn backend(&self, kind: tg::module::Kind) -> &'static dyn backend::Backend {
		if kind == tg::module::Kind::Python {
			#[cfg(feature = "ty")]
			return &python::Backend;
			#[cfg(not(feature = "ty"))]
			return &backend::Unavailable { feature: "ty" };
		}
		#[cfg(feature = "tsrs")]
		return &javascript::Backend;
		#[cfg(not(feature = "tsrs"))]
		return &backend::Unavailable { feature: "tsrs" };
	}

	async fn module_kind_for_path(&self, path: &Path) -> tg::Result<tg::module::Kind> {
		if let Ok(kind) = tg::module::module_kind_for_path(path) {
			return Ok(kind);
		}
		let metadata = tokio::fs::symlink_metadata(path)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
		if metadata.is_dir() {
			Ok(tg::module::Kind::Directory)
		} else if metadata.is_file() {
			Ok(tg::module::Kind::File)
		} else if metadata.is_symlink() {
			Ok(tg::module::Kind::Symlink)
		} else {
			Err(tg::error!("expected a directory, file, or symlink"))
		}
	}

	async fn module_for_store_path(
		&self,
		path: &Path,
		absolute_path: &Path,
	) -> tg::Result<tg::module::Data> {
		let component = path
			.components()
			.next()
			.ok_or_else(|| tg::error!("invalid path"))?
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("invalid path"))?;
		let component = tg::store::path::parse_component(component)?;
		match component {
			tg::store::path::Component::Id { id, .. } => {
				self.module_for_store_artifact_path(id, path, absolute_path)
					.await
			},
			tg::store::path::Component::Tag { .. } => self.module_for_store_tag_path(path).await,
		}
	}

	async fn module_for_store_artifact_path(
		&self,
		id: tg::artifact::Id,
		path: &Path,
		absolute_path: &Path,
	) -> tg::Result<tg::module::Data> {
		let kind = self.module_kind_for_path(absolute_path).await?;
		let id: tg::object::Id = id.into();
		let path = path.components().skip(1).collect::<PathBuf>();
		let edge: tg::graph::data::Edge<tg::object::Id> = if path.as_os_str().is_empty() {
			tg::graph::data::Edge::Object(id.clone())
		} else {
			let directory = tg::Object::with_id(id.clone())
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory
				.get_edge_with_instance(&self.instance, &path)
				.await?
				.to_data_artifact()
				.into()
		};
		let source = tg::module::data::Source::Edge(edge);
		let options = if path.as_os_str().is_empty() {
			tg::referent::Options::default()
		} else {
			tg::referent::Options {
				id: Some(id),
				path: Some(path),
				..Default::default()
			}
		};
		let referent = tg::Referent {
			node: source,
			options,
		};
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	async fn module_for_store_tag_path(&self, path: &Path) -> tg::Result<tg::module::Data> {
		// Walk through the path to find the tag symlink.
		let mut current_path = self.store_path.clone();
		let mut relative_path = PathBuf::new();
		let mut symlink_path = None;
		let mut tag_components = Vec::new();
		for component in path.components() {
			if symlink_path.is_some() {
				relative_path.push(component);
				continue;
			}
			current_path.push(component);
			let metadata = tokio::fs::symlink_metadata(&current_path)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
			let component = component
				.as_os_str()
				.to_str()
				.ok_or_else(|| tg::error!("invalid tag component"))?;
			let tg::store::path::Component::Tag { component, suffix } =
				tg::store::path::parse_component(component)?
			else {
				return Err(tg::error!("invalid tag component"));
			};
			if metadata.is_symlink() {
				tag_components.push(component.to_string());
				symlink_path = Some(current_path.clone());
			} else if metadata.is_dir() && suffix.is_none() {
				tag_components.push(component.to_string());
			} else {
				return Err(tg::error!("expected a tag directory or symlink"));
			}
		}
		let symlink_path = symlink_path.ok_or_else(|| tg::error!("invalid tag path"))?;

		// Resolve the symlink to get the artifact ID.
		let symlink_target = tokio::fs::read_link(&symlink_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the symlink"))?;
		let artifact_path = symlink_path.parent().unwrap().join(symlink_target);
		let artifact_path = tokio::fs::canonicalize(artifact_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		let component = artifact_path
			.strip_prefix(&self.store_path)
			.map_err(|_| tg::error!("the artifact path is not in the store directory"))?
			.components()
			.next()
			.ok_or_else(|| tg::error!("invalid artifact path"))?
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("invalid artifact path"))?;
		let tg::store::path::Component::Id { id, .. } =
			tg::store::path::parse_component(component)?
		else {
			return Err(tg::error!("invalid artifact path"));
		};
		let id: tg::object::Id = id.into();

		// Determine the module kind and edge.
		let path = if relative_path.as_os_str().is_empty() {
			artifact_path
		} else {
			artifact_path.join(&relative_path)
		};
		let kind = self.module_kind_for_path(&path).await?;
		let edge: tg::graph::data::Edge<tg::object::Id> = if relative_path.as_os_str().is_empty() {
			tg::graph::data::Edge::Object(id.clone())
		} else {
			let directory = tg::Object::with_id(id.clone())
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			directory
				.get_edge_with_instance(&self.instance, &relative_path)
				.await?
				.to_data_artifact()
				.into()
		};

		// Create the module.
		let tag = tag_components
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the tag"))?;
		let source = tg::module::data::Source::Edge(edge);
		let path = (!relative_path.as_os_str().is_empty()).then_some(relative_path);
		let options = tg::referent::Options {
			id: Some(id),
			path,
			tag: Some(tag),
			..Default::default()
		};
		let referent = tg::Referent {
			node: source,
			options,
		};
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	fn is_bindings(&self, module: &tg::module::Data) -> bool {
		matches!(&module.referent.node, tg::module::data::Source::Path(path) if path.starts_with(self.library_path.join("bindings")))
	}

	async fn module_for_lsp_uri(&self, uri: &lsp::Uri) -> tg::Result<tg::module::Data> {
		// Verify the scheme and get the path.
		if uri.scheme().unwrap().as_str() != "file" {
			return Err(tg::error!("invalid scheme"));
		}
		let path = Path::new(uri.path().as_str());

		// Handle a path in the store directory.
		let absolute_path = path;
		if let Ok(path) = path.strip_prefix(&self.store_path) {
			return self.module_for_store_path(path, absolute_path).await;
		}

		// Handle a path in the library directory.
		if path.starts_with(self.library_path.join("bindings")) {
			let kind = tg::module::module_kind_for_path(path)?;
			let module = tg::module::Data {
				kind,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
			};
			return Ok(module);
		}
		if path.starts_with(self.library_path.join("python")) {
			let module = tg::module::Data {
				kind: tg::module::Kind::Python,
				referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
			};
			return Ok(module);
		}
		if let Ok(path) = path.strip_prefix(&self.library_path) {
			let kind = tg::module::Kind::TypeScriptDeclaration;
			let source = tg::module::data::Source::Path(path.to_owned());
			let referent = tg::Referent::with_node(source);
			let module = tg::module::Data { kind, referent };
			return Ok(module);
		}

		// Create the module.
		let file_name = path
			.file_name()
			.ok_or_else(|| tg::error!(path = %path.display(), "invalid path"))?
			.to_str()
			.ok_or_else(|| tg::error!(path = %path.display(), "invalid path"))?;
		if !tg::module::is_module_path(file_name.as_ref()) {
			return Err(tg::error!(path = %path.display(), "expected a module path"));
		}
		let kind = tg::module::module_kind_for_path(file_name)?;
		let module = tg::module::Data {
			kind,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(path.to_owned())),
		};

		Ok(module)
	}

	/// Convert a location returned by a language service for a module of the given kind, mapping a position in bindings to its source in the original module.
	async fn lsp_location_with_language(
		&self,
		location: &tg::module::data::Location,
		kind: tg::module::Kind,
	) -> tg::Result<lsp::Location> {
		// Map a position in bindings to the source of its export.
		if let Some((language, _)) = bindings_language(kind, location.module.kind) {
			let text = self
				.load_module_with_language(&location.module, Some(language))
				.await?;
			let encoding = *self.position_encoding.read().unwrap();
			if let Some(offset) = location
				.range
				.start
				.try_to_byte_index_in_string(&text, encoding)
				&& let Some(source) = load::source(&text, offset)
			{
				let uri = self.lsp_uri_for_module(&location.module).await?;
				let range = source.range(encoding).into();
				let location = lsp::Location { uri, range };
				return Ok(location);
			}
		}

		// Otherwise, use the representation whose positions the language service returned.
		let uri = self
			.lsp_uri_for_module_with_language(&location.module, kind)
			.await?;
		let range = location.range.into();
		let location = lsp::Location { uri, range };

		Ok(location)
	}

	async fn lsp_uri_for_module_with_language(
		&self,
		module: &tg::module::Data,
		kind: tg::module::Kind,
	) -> tg::Result<lsp::Uri> {
		let Some((language, extension)) = bindings_language(kind, module.kind) else {
			return self.lsp_uri_for_module(module).await;
		};

		// Materialize the representation whose positions the language service returned.
		let text = self
			.load_module_with_language(module, Some(language))
			.await?;
		let id = tg::blob::Id::new(text.as_bytes());
		let directory = self.library_path.join("bindings");
		let path = directory.join(format!("{id}.tg.{extension}"));
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the bindings directory"))?;
		tokio::fs::write(&path, text)
			.await
			.map_err(|error| tg::error!(!error, "failed to materialize the bindings"))?;
		let uri = format!("file://{}", path.display()).parse().unwrap();

		Ok(uri)
	}

	async fn lsp_uri_for_module(&self, module: &tg::module::Data) -> tg::Result<lsp::Uri> {
		match module {
			tg::module::Data {
				kind: tg::module::Kind::TypeScriptDeclaration,
				referent:
					tg::Referent {
						node: tg::module::data::Source::Path(path),
						..
					},
				..
			} => {
				let relative_path = path.strip_prefix("./").unwrap_or(path);
				let absolute_path = self.library_path.join(relative_path);
				let exists = tokio::fs::try_exists(&absolute_path)
					.await
					.map_err(|error| tg::error!(!error, "failed to stat the path"))?;
				if !exists {
					tokio::fs::create_dir_all(&self.library_path)
						.await
						.map_err(|error| {
							tg::error!(!error, "failed to create the library temp directory")
						})?;
					LIBRARY
						.extract(&self.library_path)
						.map_err(|error| tg::error!(!error, "failed to materialize the library"))?;
				}
				let uri = format!("file://{}", absolute_path.display())
					.parse()
					.unwrap();
				Ok(uri)
			},

			tg::module::Data {
				kind,
				referent:
					tg::Referent {
						node: tg::module::data::Source::Edge(edge),
						options,
					},
			} => {
				let artifact = match edge {
					tg::graph::data::Edge::Index(_) => return Err(tg::error!("missing graph")),
					tg::graph::data::Edge::Pointer(pointer) => {
						let pointer = tg::graph::Pointer::try_from_data(pointer.clone())?;
						let artifact = tg::Artifact::with_pointer(pointer);
						artifact.store_with_instance(&self.instance).await?
					},
					tg::graph::data::Edge::Object(object) => object
						.clone()
						.try_into()
						.map_err(|_| tg::error!("expected an artifact"))?,
				};
				let artifact = if let Some(id) = &options.id {
					id.clone()
						.try_into()
						.map_err(|_| tg::error!("expected an artifact"))?
				} else {
					artifact
				};
				let path = if let (Some(tag), Some(_)) = (&options.tag, &options.id) {
					let extension = match kind {
						tg::module::Kind::JavaScript => Some(".tg.js".to_owned()),
						tg::module::Kind::Python => Some(".tg.py".to_owned()),
						tg::module::Kind::TypeScript => Some(".tg.ts".to_owned()),
						_ => None,
					};
					let extension = options.path.is_none().then_some(extension).flatten();
					let reference_options = tg::reference::Options {
						location: options.location.clone().map(Into::into),
						tokens: options.tokens.clone(),
						..Default::default()
					};
					let reference = tg::Reference::with_node_and_options(
						tg::reference::Node::Specifier(tag.clone().into()),
						reference_options,
					);
					let mut node =
						reference
							.get_with_instance(&self.instance)
							.await?
							.try_map(|node| {
								node.try_unwrap_id()
									.map_err(|_| tg::error!("expected a tag ID"))
							})?;
					node.options.artifact = Some(artifact.clone());
					node.options.id.clone_from(&options.id);
					let arg = tg::checkout::Arg {
						dependencies: true,
						extension: extension.clone(),
						force: false,
						lock: None,
						nodes: vec![node],
						path: None,
					};
					let path =
						tg::checkout::checkout_one_with_instance(&self.instance, arg).await?;

					if let Some(path_) = &options.path {
						path.join(path_)
					} else {
						path
					}
				} else if let (Some(id), Some(path)) = (&options.id, &options.path) {
					let artifact: tg::artifact::Id = id.clone().try_into()?;
					let referent_options = tg::referent::Options {
						location: options.location.clone(),
						tokens: options.tokens.clone(),
						..tg::referent::Options::default()
					};
					let artifact = tg::Referent::new(artifact, referent_options);
					let arg = tg::checkout::Arg {
						dependencies: true,
						extension: None,
						force: false,
						lock: None,
						nodes: vec![artifact.map(Into::into)],
						path: None,
					};
					let output =
						tg::checkout::checkout_one_with_instance(&self.instance, arg).await?;
					output.join(path)
				} else {
					let extension = match kind {
						tg::module::Kind::JavaScript => Some(".tg.js".to_owned()),
						tg::module::Kind::Python => Some(".tg.py".to_owned()),
						tg::module::Kind::TypeScript => Some(".tg.ts".to_owned()),
						_ => None,
					};
					let referent_options = tg::referent::Options {
						location: options.location.clone(),
						tokens: options.tokens.clone(),
						..tg::referent::Options::default()
					};
					let artifact = tg::Referent::new(artifact, referent_options);
					let arg = tg::checkout::Arg {
						dependencies: true,
						extension,
						force: false,
						lock: None,
						nodes: vec![artifact.map(Into::into)],
						path: None,
					};
					tg::checkout::checkout_one_with_instance(&self.instance, arg).await?
				};

				let uri = format!("file://{}", path.display()).parse().unwrap();

				Ok(uri)
			},

			tg::module::Data {
				referent:
					tg::Referent {
						node: tg::module::data::Source::Path(path),
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

	/// Load a module in its original language.
	pub async fn load_module(&self, module: &tg::module::Data) -> tg::Result<String> {
		self.load_module_with_language(module, None).await
	}

	/// Load a module for a runtime or checker.
	pub async fn load_module_with_language(
		&self,
		module: &tg::module::Data,
		language: Option<tg::module::load::Language>,
	) -> tg::Result<String> {
		self.load_module_with_language_inner(module, language, true)
			.await
	}

	/// Load a module for a runtime or checker, generating the bindings of a module in another language from the module's cached interface only if `typed` is true.
	pub(crate) async fn load_module_with_language_inner(
		&self,
		module: &tg::module::Data,
		language: Option<tg::module::load::Language>,
		typed: bool,
	) -> tg::Result<String> {
		// Generate the bindings of a module in another language from the module's cached interface, if there is one.
		#[cfg(all(feature = "javascript", feature = "python"))]
		if let Some(language) = load::bindings_language(module, language)? {
			let text = self.load_module_inner(module, None).await?;
			let entry = typed.then(|| self.try_get_interface(module)).flatten();
			let output = if let Some(entry) = entry {
				load::bindings(&entry.interface, &text, language)
			} else {
				load::module(module, &text, Some(language))?
			};
			return Ok(output);
		}

		#[cfg(not(all(feature = "javascript", feature = "python")))]
		let _ = typed;
		self.load_module_inner(module, language).await
	}

	async fn load_module_inner(
		&self,
		module: &tg::module::Data,
		language: Option<tg::module::load::Language>,
	) -> tg::Result<String> {
		if let Some(document) = self.documents.get(&document::Key::new(module))
			&& document.open
		{
			return load::module(module, document.text.as_ref().unwrap(), language);
		}
		let arg = tg::module::load::Arg {
			language,
			module: module.clone(),
		};
		let output = self.instance.load_module(arg).await?;
		Ok(output.text)
	}

	/// Find the lockfile for a given path.
	pub async fn find_lockfile_for_path(&self, path: &Path) -> Option<document::Lockfile> {
		// Find the lockfile of the file, and the nearest lockfile of an ancestor.
		let mut paths = vec![path.with_extension("lock")];
		for ancestor in path.parent()?.ancestors() {
			let candidate = ancestor.join(tg::module::LOCKFILE_FILE_NAME);
			if tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
				paths.push(candidate);
				break;
			}
		}

		// Get the modification times of the lockfiles that exist.
		let mut files = Vec::new();
		for path in paths {
			let Ok(metadata) = tokio::fs::symlink_metadata(&path).await else {
				continue;
			};
			let Ok(mtime) = metadata.modified() else {
				continue;
			};
			files.push((path, mtime));
		}

		// Hash the lock attribute of the file.
		let attr = tg::file::xattrs::read_lock(path)
			.ok()
			.flatten()
			.map(|lock| {
				use std::hash::{Hash as _, Hasher as _};
				let mut hasher = fnv::FnvHasher::default();
				lock.hash(&mut hasher);
				hasher.finish()
			});

		if attr.is_none() && files.is_empty() {
			return None;
		}
		let lockfile = document::Lockfile { attr, files };

		Some(lockfile)
	}
}

impl Deref for Shared {
	type Target = Owned;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Deref for Owned {
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

impl Drop for Owned {
	fn drop(&mut self) {
		#[cfg(feature = "tsrs")]
		self.compiler.javascript.stop();
		#[cfg(feature = "ty")]
		self.compiler.python.stop();
	}
}

/// Get the language and extension of the representation that a module of the given kind uses for a module in another language.
fn bindings_language(
	kind: tg::module::Kind,
	module_kind: tg::module::Kind,
) -> Option<(tg::module::load::Language, &'static str)> {
	match (kind, module_kind) {
		(tg::module::Kind::Python, tg::module::Kind::JavaScript | tg::module::Kind::TypeScript) => {
			Some((tg::module::load::Language::Python, "py"))
		},
		(tg::module::Kind::JavaScript | tg::module::Kind::TypeScript, tg::module::Kind::Python) => {
			Some((tg::module::load::Language::JavaScript, "js"))
		},
		_ => None,
	}
}
