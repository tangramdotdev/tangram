#[cfg(feature = "ty")]
use {
	super::{
		Compiler,
		document::{Document, Key},
	},
	futures::{FutureExt as _, future::BoxFuture},
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

#[cfg(feature = "ty")]
mod db;
#[cfg(feature = "ty")]
mod library;
#[cfg(feature = "ty")]
mod system;

pub mod load;
pub mod resolve;

#[cfg(feature = "ty")]
type Documents = BTreeMap<Key, Document>;

#[cfg(feature = "ty")]
type Job = Box<dyn FnOnce(&Compiler, &mut Option<db::Database>) + Send>;

/// The Python backend.
#[cfg(feature = "ty")]
pub(crate) struct Backend;

#[cfg(feature = "ty")]
#[derive(Default)]
pub struct Service {
	sender: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Job>>>,
	thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[cfg(feature = "ty")]
impl Service {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Run a query on a module's file with the database.
	fn query<T, F>(
		&self,
		compiler: &Compiler,
		module: tg::module::Data,
		sync: bool,
		f: F,
	) -> BoxFuture<'static, tg::Result<T>>
	where
		T: Send + 'static,
		F: FnOnce(&db::Database, ruff_db::files::File, tg::position::Encoding) -> tg::Result<T>
			+ Send
			+ 'static,
	{
		let encoding = *compiler.position_encoding.read().unwrap();
		let modules = vec![module.clone()];
		self.run(compiler, modules, move |database| {
			database.query(&module, sync, |database, file| f(database, file, encoding))
		})
	}

	/// Run a function with the database, after updating it with the open documents and preparing the interfaces of the modules in other languages that the modules and the open documents load.
	fn run<T, F>(
		&self,
		compiler: &Compiler,
		modules: Vec<tg::module::Data>,
		f: F,
	) -> BoxFuture<'static, tg::Result<T>>
	where
		T: Send + 'static,
		F: FnOnce(&mut db::Database) -> tg::Result<T> + Send + 'static,
	{
		#[cfg(feature = "javascript")]
		let prepare = {
			let compiler = compiler.clone();
			let mut modules = modules;
			modules.extend(compiler.py_documents().into_values().map(|document| document.module));
			modules.retain(|module| module.kind == tg::module::Kind::Python);
			async move { compiler.prepare_interfaces(modules).await }
		};
		#[cfg(not(feature = "javascript"))]
		let prepare = {
			drop(modules);
			std::future::ready(())
		};

		// Create the job, which sends its output.
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let job: Job = Box::new(move |compiler, database| {
			let output = (|| {
				// Update the database, or create it.
				if database.is_none() {
					database.replace(db::Database::new(compiler.clone(), Documents::new())?);
				}
				let database = database.as_mut().unwrap();
				database.update(compiler.py_documents())?;

				f(database)
			})();
			sender.send(output).ok();
		});

		// Prepare the interfaces, and then send the job to the worker.
		let compiler = compiler.clone();
		async move {
			prepare.await;
			compiler.python.send(&compiler, job)?;
			receiver
				.await
				.map_err(|error| tg::error!(!error, "failed to receive the python output"))?
		}
		.boxed()
	}

	fn send(&self, compiler: &Compiler, job: Job) -> tg::Result<()> {
		// Start the worker lazily, just like the TypeScript service.
		{
			let mut thread = self.thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
				self.sender.lock().unwrap().replace(sender);
				let compiler = compiler.clone();
				thread.replace(std::thread::spawn(move || run(&compiler, receiver)));
			}
		}

		// Send the job, so that updates and analysis are serialized on the worker's persistent database.
		self.sender
			.lock()
			.unwrap()
			.as_ref()
			.ok_or_else(|| tg::error!("the python service is stopped"))?
			.send(job)
			.map_err(|_| tg::error!("failed to send the python job"))?;

		Ok(())
	}

	#[must_use]
	pub fn is_started(&self) -> bool {
		self.thread.lock().unwrap().is_some()
	}

	pub fn stop(&self) {
		self.sender.lock().unwrap().take();
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

#[cfg(feature = "ty")]
impl Compiler {
	fn py_documents(&self) -> Documents {
		self.documents
			.iter()
			.filter(|entry| entry.open)
			.map(|entry| (entry.key().clone(), entry.value().clone()))
			.collect()
	}
}

#[cfg(feature = "ty")]
impl crate::backend::Backend for Backend {
	fn feature(&self) -> &'static str {
		"ty"
	}

	fn call_hierarchy_incoming(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::IncomingArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::IncomingOutput>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, true, move |database, file, encoding| {
				database.call_hierarchy_incoming(file, &arg, encoding)
			})
	}

	fn call_hierarchy_outgoing(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::OutgoingArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::OutgoingOutput>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.call_hierarchy_outgoing(file, &arg, encoding)
			})
	}

	fn call_hierarchy_prepare(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::PrepareArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::PrepareOutput>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.call_hierarchy_prepare(file, &arg, encoding)
			})
	}

	fn check(
		&self,
		compiler: &Compiler,
		arg: crate::check::Arg,
	) -> BoxFuture<'static, tg::Result<crate::check::Output>> {
		let modules = arg.modules.clone();
		compiler.python.run(compiler, modules, move |database| {
			let diagnostics = database
				.check(arg.modules)?
				.iter()
				.map(tg::Diagnostic::to_data)
				.collect();
			Ok(crate::check::Output { diagnostics })
		})
	}

	fn code_action(
		&self,
		compiler: &Compiler,
		arg: crate::code_action::Arg,
	) -> BoxFuture<'static, tg::Result<crate::code_action::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.code_actions(file, &arg, encoding)
			})
	}

	fn completion(
		&self,
		compiler: &Compiler,
		arg: crate::completion::Arg,
	) -> BoxFuture<'static, tg::Result<crate::completion::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.completion(file, arg.position, encoding)
			})
	}

	fn completion_resolve(
		&self,
		_compiler: &Compiler,
		arg: crate::completion::ResolveArg,
	) -> BoxFuture<'static, tg::Result<crate::completion::ResolveOutput>> {
		// The completion's data holds the resolved entry.
		let entry = arg
			.data
			.map(serde_json::from_value)
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid python completion data"));
		let output = entry.map(|entry| crate::completion::ResolveOutput { entry });
		futures::future::ready(output).boxed()
	}

	fn declaration(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.declaration(file, &arg, encoding)
			})
	}

	fn definition(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.definition(file, &arg, encoding)
			})
	}

	fn document(
		&self,
		compiler: &Compiler,
		arg: crate::document::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document::Output>> {
		compiler
			.python
			.query(compiler, arg.module, false, db::Database::document)
	}

	fn document_diagnostics(
		&self,
		compiler: &Compiler,
		arg: crate::diagnostics::DocumentArg,
	) -> BoxFuture<'static, tg::Result<crate::diagnostics::DocumentOutput>> {
		let encoding = *compiler.position_encoding.read().unwrap();
		let modules = arg.modules.clone();
		compiler.python.run(compiler, modules, move |database| {
			database.document_diagnostics(arg.modules, encoding)
		})
	}

	fn document_highlight(
		&self,
		compiler: &Compiler,
		arg: crate::document_highlight::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document_highlight::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.document_highlight(file, &arg, encoding)
			})
	}

	fn document_link(
		&self,
		_compiler: &Compiler,
		_arg: crate::document_link::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document_link::Output>> {
		// Python imports do not have document links.
		let output = crate::document_link::Output { links: None };
		futures::future::ready(Ok(output)).boxed()
	}

	fn folding_range(
		&self,
		compiler: &Compiler,
		arg: crate::folding_range::Arg,
	) -> BoxFuture<'static, tg::Result<crate::folding_range::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.folding_range(file, &arg, encoding)
			})
	}

	fn hover(
		&self,
		compiler: &Compiler,
		arg: crate::hover::Arg,
	) -> BoxFuture<'static, tg::Result<crate::hover::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.hover(file, &arg, encoding)
			})
	}

	fn implementation(
		&self,
		compiler: &Compiler,
		arg: crate::implementation::Arg,
	) -> BoxFuture<'static, tg::Result<crate::implementation::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, true, move |database, file, encoding| {
				database.implementation(file, &arg, encoding)
			})
	}

	fn inlay_hint(
		&self,
		compiler: &Compiler,
		arg: crate::inlay_hint::Arg,
	) -> BoxFuture<'static, tg::Result<crate::inlay_hint::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.inlay_hint(file, &arg, encoding)
			})
	}

	fn prepare_rename(
		&self,
		compiler: &Compiler,
		arg: crate::prepare_rename::Arg,
	) -> BoxFuture<'static, tg::Result<crate::prepare_rename::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, true, move |database, file, encoding| {
				database.prepare_rename(file, &arg, encoding)
			})
	}

	fn references(
		&self,
		compiler: &Compiler,
		arg: crate::references::Arg,
	) -> BoxFuture<'static, tg::Result<crate::references::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, true, move |database, file, encoding| {
				database.references(file, &arg, encoding)
			})
	}

	fn rename(
		&self,
		compiler: &Compiler,
		arg: crate::rename::Arg,
	) -> BoxFuture<'static, tg::Result<crate::rename::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, true, move |database, file, encoding| {
				database.rename(file, &arg, encoding)
			})
	}

	fn selection_range(
		&self,
		compiler: &Compiler,
		arg: crate::selection_range::Arg,
	) -> BoxFuture<'static, tg::Result<crate::selection_range::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.selection_range(file, &arg, encoding)
			})
	}

	fn semantic_tokens(
		&self,
		compiler: &Compiler,
		arg: crate::semantic_tokens::Arg,
	) -> BoxFuture<'static, tg::Result<crate::semantic_tokens::Output>> {
		compiler
			.python
			.query(compiler, arg.module, false, db::Database::semantic_tokens)
	}

	fn signature_help(
		&self,
		compiler: &Compiler,
		arg: crate::signature_help::Arg,
	) -> BoxFuture<'static, tg::Result<crate::signature_help::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.signature_help(file, &arg, encoding)
			})
	}

	fn symbols(
		&self,
		compiler: &Compiler,
		arg: crate::symbols::Arg,
	) -> BoxFuture<'static, tg::Result<crate::symbols::Output>> {
		compiler
			.python
			.query(compiler, arg.module, false, db::Database::symbols)
	}

	fn type_definition(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let module = arg.module.clone();
		compiler
			.python
			.query(compiler, module, false, move |database, file, encoding| {
				database.type_definition(file, &arg, encoding)
			})
	}

	fn workspace_symbol(
		&self,
		compiler: &Compiler,
		arg: crate::workspace_symbols::Arg,
	) -> BoxFuture<'static, tg::Result<crate::workspace_symbols::Output>> {
		let encoding = *compiler.position_encoding.read().unwrap();
		compiler.python.run(compiler, Vec::new(), move |database| {
			database.sync_project(None)?;
			database.workspace_symbols(&arg.query, encoding)
		})
	}
}

#[cfg(feature = "ty")]
fn run(compiler: &Compiler, mut receiver: tokio::sync::mpsc::UnboundedReceiver<Job>) {
	// Run each job, retaining the database across jobs.
	let mut database = None;
	while let Some(job) = receiver.blocking_recv() {
		job(compiler, &mut database);
	}
}
