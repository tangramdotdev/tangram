use {
	super::Project,
	crate::{Compiler, check},
	tangram_client::prelude::*,
};

pub(super) fn handle(compiler: &Compiler, request: check::Request) -> tg::Result<check::Response> {
	// Create a separate compiler program.
	let project = Project::new(compiler, request.modules)?;

	// Collect and convert the diagnostics while the snapshot owns their source files.
	let context = Project::context();
	let snapshot = project.snapshot.borrow();
	let native_project = snapshot
		.0
		.project_collection
		.get_project(&project.id.as_id())
		.ok_or_else(|| tg::error!("the tsrs snapshot did not contain the project"))?;
	let program = project.program()?;
	let mut diagnostics = program.get_semantic_diagnostics(&context, None);
	diagnostics.extend(native_project.get_project_diagnostics(&context));
	diagnostics.extend(program.get_declaration_diagnostics(&context, None));
	diagnostics.extend(program.get_syntactic_diagnostics(&context, None));
	let diagnostics = tsrs_compiler::sort_and_deduplicate_diagnostics(&diagnostics)
		.iter()
		.map(|diagnostic| project.diagnostic(*diagnostic))
		.collect::<tg::Result<_>>()?;
	let response = check::Response { diagnostics };

	Ok(response)
}
