use {
	self::{
		convert::{position, range},
		project::Project,
	},
	crate::Compiler,
	futures::{FutureExt as _, future::BoxFuture},
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
#[cfg(feature = "python")]
mod interface;
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

type Job = Box<dyn FnOnce(&Compiler, &mut Option<Project>) + Send>;

/// The JavaScript and TypeScript backend.
pub(crate) struct Backend;

pub struct Service {
	sender: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Job>>>,
	thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Service {
	#[must_use]
	pub fn new() -> Self {
		Self {
			sender: Mutex::new(None),
			thread: Mutex::new(None),
		}
	}

	/// Run a function with the editor project, after updating it with the modules and the open documents.
	fn run<T, F>(
		compiler: &Compiler,
		modules: Vec<tg::module::Data>,
		f: F,
	) -> BoxFuture<'static, tg::Result<T>>
	where
		T: Send + 'static,
		F: FnOnce(&mut Project) -> tg::Result<T> + Send + 'static,
	{
		let compiler = compiler.clone();
		async move {
			// Add the open documents to the modules.
			let mut modules = modules;
			modules.extend(compiler.list_documents().await);

			// Prepare the interfaces of the modules in other languages that the project has loaded.
			Self::prepare(&compiler).await;

			// Update the project, and run the function unless the update loaded a module in another language whose interface has not been prepared.
			let output = compiler
				.javascript
				.job(&compiler, {
					let modules = modules.clone();
					move |compiler, project| {
						let project = Self::update(compiler, project, modules)?;
						if Self::unprepared(compiler) {
							return Ok(Err(f));
						}
						f(project).map(Ok)
					}
				})
				.await?;
			let f = match output {
				Ok(output) => return Ok(output),
				Err(f) => f,
			};

			// Prepare the interfaces of the newly loaded modules, and then update the project again and run the function.
			Self::prepare(&compiler).await;
			compiler
				.javascript
				.job(&compiler, move |compiler, project| {
					let project = Self::update(compiler, project, modules)?;
					f(project)
				})
				.await
		}
		.boxed()
	}

	/// Update the project with the modules, or create it.
	fn update<'a>(
		compiler: &Compiler,
		project: &'a mut Option<Project>,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<&'a mut Project> {
		let project = if let Some(project) = project {
			project.update(modules)?;
			project
		} else {
			project.insert(Project::new(compiler, modules)?)
		};
		Ok(project)
	}

	/// Prepare the interfaces of the modules in other languages that the project has loaded.
	async fn prepare(compiler: &Compiler) {
		#[cfg(feature = "python")]
		compiler
			.prepare_interfaces(tg::module::load::Language::JavaScript)
			.await;
		#[cfg(not(feature = "python"))]
		let _ = compiler;
	}

	/// Determine whether the project has loaded a module in another language whose interface has not been prepared.
	fn unprepared(compiler: &Compiler) -> bool {
		#[cfg(feature = "python")]
		let unprepared = compiler.take_unprepared(tg::module::load::Language::JavaScript);
		#[cfg(not(feature = "python"))]
		let unprepared = {
			let _ = compiler;
			false
		};
		unprepared
	}

	/// Run a function on the worker thread and return its output.
	fn job<T, F>(&self, compiler: &Compiler, f: F) -> BoxFuture<'static, tg::Result<T>>
	where
		T: Send + 'static,
		F: FnOnce(&Compiler, &mut Option<Project>) -> tg::Result<T> + Send + 'static,
	{
		// Create the job, which sends its output.
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let job: Job = Box::new(move |compiler, project| {
			sender.send(f(compiler, project)).ok();
		});

		// Send the job to the worker.
		let result = self.send(compiler, job);

		async move {
			result?;
			receiver
				.await
				.map_err(|error| tg::error!(!error, "failed to receive the javascript output"))?
		}
		.boxed()
	}

	fn send(&self, compiler: &Compiler, job: Job) -> tg::Result<()> {
		// Start the worker thread.
		{
			let mut thread = self.thread.lock().unwrap();
			if thread.is_none() {
				let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
				let compiler = compiler.clone();
				let worker = std::thread::Builder::new()
					.name("tangram-javascript".into())
					.stack_size(512 * 1024 * 1024)
					.spawn(move || run(&compiler, receiver))
					.map_err(|error| {
						tg::error!(!error, "failed to start the javascript service")
					})?;
				self.sender.lock().unwrap().replace(sender);
				thread.replace(worker);
			}
		}

		// Send the job.
		self.sender
			.lock()
			.unwrap()
			.as_ref()
			.ok_or_else(|| tg::error!("the javascript service is stopped"))?
			.send(job)
			.map_err(|_| tg::error!("failed to send the javascript job"))?;

		Ok(())
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

impl crate::backend::Backend for Backend {
	fn feature(&self) -> &'static str {
		"tsrs"
	}

	fn call_hierarchy_incoming(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::IncomingArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::IncomingOutput>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.call_hierarchy_incoming(&arg)
		})
	}

	fn call_hierarchy_outgoing(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::OutgoingArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::OutgoingOutput>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.call_hierarchy_outgoing(&arg)
		})
	}

	fn call_hierarchy_prepare(
		&self,
		compiler: &Compiler,
		arg: crate::call_hierarchy::PrepareArg,
	) -> BoxFuture<'static, tg::Result<crate::call_hierarchy::PrepareOutput>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.call_hierarchy_prepare(&arg)
		})
	}

	fn check(
		&self,
		compiler: &Compiler,
		arg: crate::check::Arg,
	) -> BoxFuture<'static, tg::Result<crate::check::Output>> {
		// Check with a separate program rather than the editor project, creating the program again if it loaded a module in another language whose interface had not been prepared.
		let compiler = compiler.clone();
		async move {
			Service::prepare(&compiler).await;
			let output = compiler
				.javascript
				.job(&compiler, {
					let modules = arg.modules.clone();
					move |compiler, _| {
						let project = Project::new(compiler, modules)?;
						if Service::unprepared(compiler) {
							return Ok(None);
						}
						check::handle(&project).map(Some)
					}
				})
				.await?;
			if let Some(output) = output {
				return Ok(output);
			}
			Service::prepare(&compiler).await;
			compiler
				.javascript
				.job(&compiler, move |compiler, _| {
					let project = Project::new(compiler, arg.modules)?;
					check::handle(&project)
				})
				.await
		}
		.boxed()
	}

	fn code_action(
		&self,
		compiler: &Compiler,
		arg: crate::code_action::Arg,
	) -> BoxFuture<'static, tg::Result<crate::code_action::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.code_action(&arg))
	}

	fn completion(
		&self,
		compiler: &Compiler,
		arg: crate::completion::Arg,
	) -> BoxFuture<'static, tg::Result<crate::completion::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.completion(&arg))
	}

	fn completion_resolve(
		&self,
		compiler: &Compiler,
		arg: crate::completion::ResolveArg,
	) -> BoxFuture<'static, tg::Result<crate::completion::ResolveOutput>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.completion_resolve(arg)
		})
	}

	fn declaration(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.declaration(&arg))
	}

	fn definition(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.definition(&arg))
	}

	fn document(
		&self,
		compiler: &Compiler,
		arg: crate::document::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.document(&arg))
	}

	fn document_diagnostics(
		&self,
		compiler: &Compiler,
		arg: crate::diagnostics::DocumentArg,
	) -> BoxFuture<'static, tg::Result<crate::diagnostics::DocumentOutput>> {
		let modules = arg.modules.clone();
		Service::run(compiler, modules, move |project| project.diagnostics(arg))
	}

	fn document_highlight(
		&self,
		compiler: &Compiler,
		arg: crate::document_highlight::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document_highlight::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.document_highlight(&arg)
		})
	}

	fn document_link(
		&self,
		compiler: &Compiler,
		arg: crate::document_link::Arg,
	) -> BoxFuture<'static, tg::Result<crate::document_link::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.document_link(&arg)
		})
	}

	fn folding_range(
		&self,
		compiler: &Compiler,
		arg: crate::folding_range::Arg,
	) -> BoxFuture<'static, tg::Result<crate::folding_range::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.folding_range(&arg)
		})
	}

	fn hover(
		&self,
		compiler: &Compiler,
		arg: crate::hover::Arg,
	) -> BoxFuture<'static, tg::Result<crate::hover::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.hover(&arg))
	}

	fn implementation(
		&self,
		compiler: &Compiler,
		arg: crate::implementation::Arg,
	) -> BoxFuture<'static, tg::Result<crate::implementation::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.implementation(&arg)
		})
	}

	fn inlay_hint(
		&self,
		compiler: &Compiler,
		arg: crate::inlay_hint::Arg,
	) -> BoxFuture<'static, tg::Result<crate::inlay_hint::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.inlay_hint(&arg))
	}

	#[cfg(feature = "python")]
	fn interface(
		&self,
		compiler: &Compiler,
		arg: crate::interfaces::Arg,
	) -> BoxFuture<'static, tg::Result<tg::Interface>> {
		// Get the interface with a separate program rather than the editor project.
		compiler.javascript.job(compiler, move |compiler, _| {
			let text = compiler
				.main_runtime_handle
				.block_on(compiler.load_module(&arg.module))?;
			let modules = vec![arg.module.clone()];
			let project = Project::with_untyped(compiler, modules, arg.untyped)?;
			project.interface(&arg.module, &text)
		})
	}

	fn prepare_rename(
		&self,
		compiler: &Compiler,
		arg: crate::prepare_rename::Arg,
	) -> BoxFuture<'static, tg::Result<crate::prepare_rename::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.prepare_rename(&arg)
		})
	}

	fn references(
		&self,
		compiler: &Compiler,
		arg: crate::references::Arg,
	) -> BoxFuture<'static, tg::Result<crate::references::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.references(&arg))
	}

	fn rename(
		&self,
		compiler: &Compiler,
		arg: crate::rename::Arg,
	) -> BoxFuture<'static, tg::Result<crate::rename::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.rename(arg))
	}

	fn selection_range(
		&self,
		compiler: &Compiler,
		arg: crate::selection_range::Arg,
	) -> BoxFuture<'static, tg::Result<crate::selection_range::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.selection_range(arg)
		})
	}

	fn semantic_tokens(
		&self,
		compiler: &Compiler,
		arg: crate::semantic_tokens::Arg,
	) -> BoxFuture<'static, tg::Result<crate::semantic_tokens::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.semantic_tokens(&arg)
		})
	}

	fn signature_help(
		&self,
		compiler: &Compiler,
		arg: crate::signature_help::Arg,
	) -> BoxFuture<'static, tg::Result<crate::signature_help::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.signature_help(&arg)
		})
	}

	fn symbols(
		&self,
		compiler: &Compiler,
		arg: crate::symbols::Arg,
	) -> BoxFuture<'static, tg::Result<crate::symbols::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| project.symbols(&arg))
	}

	fn type_definition(
		&self,
		compiler: &Compiler,
		arg: crate::definition::Arg,
	) -> BoxFuture<'static, tg::Result<crate::definition::Output>> {
		let modules = vec![arg.module.clone()];
		Service::run(compiler, modules, move |project| {
			project.type_definition(&arg)
		})
	}

	fn workspace_symbol(
		&self,
		compiler: &Compiler,
		arg: crate::workspace_symbols::Arg,
	) -> BoxFuture<'static, tg::Result<crate::workspace_symbols::Output>> {
		Service::run(compiler, Vec::new(), move |project| {
			project.workspace_symbols(&arg)
		})
	}
}

fn run(compiler: &Compiler, mut receiver: tokio::sync::mpsc::UnboundedReceiver<Job>) {
	// Run each job within an allocation region, retaining the editor project across jobs.
	let mut project = None;
	while let Some(job) = receiver.blocking_recv() {
		let region = tsrs_core::arena::Region::new(64 * 1024);
		let _scope = region.enter();
		job(compiler, &mut project);
	}
}
