use {
	self::{
		convert::{position, range},
		project::Project,
	},
	crate::{Compiler, Request, Response},
	std::sync::Mutex,
	tangram_client::prelude::*,
};

mod call_hierarchy;
mod check;
mod code_action;
mod completion;
mod convert;
mod definition;
mod diagnostics;
mod document;
mod document_highlight;
mod document_link;
mod folding_range;
mod host;
mod hover;
mod implementation;
mod inlay_hint;
mod module;
mod prepare_rename;
mod project;
mod references;
mod rename;
mod resolver;
mod selection_range;
mod semantic_tokens;
mod signature_help;
mod symbols;
mod workspace_symbol;

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

pub struct Service {
	request_sender: Mutex<Option<RequestSender>>,
	thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Service {
	#[must_use]
	pub fn new() -> Self {
		Self {
			request_sender: Mutex::new(None),
			thread: Mutex::new(None),
		}
	}

	pub async fn request(&self, compiler: &Compiler, request: Request) -> tg::Result<Response> {
		// Start the worker thread.
		{
			let mut thread = self.thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
				let compiler = compiler.clone();
				let worker = std::thread::Builder::new()
					.name("tangram-tsrs".into())
					.stack_size(512 * 1024 * 1024)
					.spawn(move || run(&compiler, receiver))
					.map_err(|error| tg::error!(!error, "failed to start the tsrs service"))?;
				self.request_sender.lock().unwrap().replace(sender);
				thread.replace(worker);
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
			.map_err(|error| tg::error!(!error, "failed to send the tsrs request"))?;

		// Receive the response.
		let response = response_receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the tsrs response"))??;

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
	let mut project = None;
	while let Some((request, response_sender)) = request_receiver.blocking_recv() {
		let response = {
			let region = tsrs_core::arena::Region::new(64 * 1024);
			let _scope = region.enter();
			handle(compiler, &mut project, request)
		};
		response_sender.send(response).ok();
	}
}

fn handle(
	compiler: &Compiler,
	editor: &mut Option<Project>,
	request: Request,
) -> tg::Result<Response> {
	if let Request::Check(request) = request {
		return check::handle(compiler, request).map(Response::Check);
	}
	let mut modules = match &request {
		Request::DocumentDiagnostics(request) => request.modules.clone(),
		request => request.module().cloned().into_iter().collect(),
	};
	modules.extend(
		compiler
			.main_runtime_handle
			.block_on(compiler.list_documents()),
	);
	let project = if let Some(project) = editor {
		project.update(modules)?;
		project
	} else {
		editor.insert(Project::new(compiler, modules)?)
	};
	let response = match request {
		Request::CallHierarchyIncoming(request) => {
			Response::CallHierarchyIncoming(project.call_hierarchy_incoming(&request)?)
		},
		Request::CallHierarchyOutgoing(request) => {
			Response::CallHierarchyOutgoing(project.call_hierarchy_outgoing(&request)?)
		},
		Request::CallHierarchyPrepare(request) => {
			Response::CallHierarchyPrepare(project.call_hierarchy_prepare(&request)?)
		},
		Request::Check(_) => unreachable!("check requests use a separate program"),
		Request::CodeAction(request) => Response::CodeAction(project.code_action(&request)?),
		Request::Completion(request) => Response::Completion(project.completion(&request)?),
		Request::CompletionResolve(request) => {
			Response::CompletionResolve(project.completion_resolve(request)?)
		},
		Request::Declaration(request) => Response::Declaration(project.declaration(&request)?),
		Request::Definition(request) => Response::Definition(project.definition(&request)?),
		Request::Document(request) => Response::Document(project.document(&request)?),
		Request::DocumentDiagnostics(request) => {
			Response::DocumentDiagnostics(project.diagnostics(request)?)
		},
		Request::DocumentHighlight(request) => {
			Response::DocumentHighlight(project.document_highlight(&request)?)
		},
		Request::DocumentLink(request) => Response::DocumentLink(project.document_link(&request)?),
		Request::FoldingRange(request) => Response::FoldingRange(project.folding_range(&request)?),
		Request::Hover(request) => Response::Hover(project.hover(&request)?),
		Request::Implementation(request) => {
			Response::Implementation(project.implementation(&request)?)
		},
		Request::InlayHint(request) => Response::InlayHint(project.inlay_hint(&request)?),
		Request::PrepareRename(request) => {
			Response::PrepareRename(project.prepare_rename(&request)?)
		},
		Request::References(request) => Response::References(project.references(&request)?),
		Request::Rename(request) => Response::Rename(project.rename(request)?),
		Request::SelectionRange(request) => {
			Response::SelectionRange(project.selection_range(request)?)
		},
		Request::SemanticTokens(request) => {
			Response::SemanticTokens(project.semantic_tokens(&request)?)
		},
		Request::SignatureHelp(request) => {
			Response::SignatureHelp(project.signature_help(&request)?)
		},
		Request::Symbols(request) => Response::Symbols(project.symbols(&request)?),
		Request::TypeDefinition(request) => {
			Response::TypeDefinition(project.type_definition(&request)?)
		},
		Request::WorkspaceSymbol(request) => {
			Response::WorkspaceSymbol(project.workspace_symbols(&request)?)
		},
	};

	Ok(response)
}
