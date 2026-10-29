use {crate::Compiler, tangram_client::prelude::*};

impl Compiler {
	/// Build an export of a module as a cached process, checking in the module first if it is a path.
	pub(crate) async fn build_export(
		&self,
		module: &tg::module::Data,
		export: &str,
	) -> tg::Result<tg::Value> {
		// Get a module that refers to an object, checking in a path.
		let module = match &module.referent.node {
			tg::module::data::Source::Edge(_) => module.clone(),
			tg::module::data::Source::Path(path) => {
				let arg = tg::checkin::Arg {
					options: tg::checkin::Options::default(),
					path: path.clone(),
					updates: Vec::new(),
				};
				let output = tg::checkin::checkin_with_instance(&self.instance, arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to check in the module"))?;
				let edge = tg::graph::data::Edge::Object(output.artifact.node.clone().into());
				let mut referent = output
					.artifact
					.map(|_| tg::module::data::Source::Edge(edge));
				referent.options.path = path.file_name().map(Into::into);
				tg::module::Data {
					kind: module.kind,
					referent,
				}
			},
		};
		let module = tg::Module::try_from_data(module)?;

		// Build the export.
		let command =
			tg::command::Builder::with_module(module, Some(export.to_owned()), []).build()?;
		let command = tg::Referent::with_node(tg::Either::Right(command));
		let arg = tg::process::Arg {
			command: Some(command),
			stderr: tg::process::Stdio::Log,
			stdin: tg::process::Stdio::Null,
			stdout: tg::process::Stdio::Log,
			..Default::default()
		};

		// Build on a blocking thread, because the build future is not sendable.
		let instance = self.instance.clone();
		let handle = tokio::runtime::Handle::current();
		let output = tokio::task::spawn_blocking(move || {
			handle.block_on(tg::process::build::build_with_instance(&instance, arg))
		})
		.await
		.map_err(|error| tg::error!(!error, "the build task panicked"))?
		.map_err(|error| tg::error!(!error, %export, "failed to build the export"))?;

		Ok(output)
	}
}
