use {
	crate::Compiler,
	futures::{FutureExt as _, TryStreamExt as _},
	tangram_client::prelude::*,
};

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

		// Build on a blocking thread, because the build future is not sendable, and get the end of the output of the process that failed if the build fails.
		let instance = self.instance.clone();
		let handle = tokio::runtime::Handle::current();
		let result = tokio::task::spawn_blocking(move || {
			handle.block_on(async move {
				let build = tg::process::build::build_with_instance(&instance, arg).boxed_local();
				match build.await {
					Ok(output) => Ok(output),
					Err(error) => {
						let output = try_get_failed_process_output(&instance, &error).await;
						Err((error, output))
					},
				}
			})
		})
		.await
		.map_err(|error| tg::error!(!error, "the build task panicked"))?;
		let output = match result {
			Ok(output) => output,
			Err((error, None)) => {
				return Err(tg::error!(!error, %export, "failed to build the export"));
			},
			Err((error, Some(output))) => {
				return Err(tg::error!(
					!error,
					%export,
					"failed to build the export, whose failed process printed:\n{output}"
				));
			},
		};

		Ok(output)
	}
}

/// Get the end of the output of the innermost process in an error's chain, which is the process whose failure caused the error.
async fn try_get_failed_process_output(
	instance: &tg::instance::dynamic::Instance,
	error: &tg::Error,
) -> Option<String> {
	// Find the innermost process in the error's chain.
	let mut process = None;
	let mut current = Some((error.clone(), tg::referent::Options::default()));
	while let Some((error, options)) = current.take() {
		let object = error.object_with_instance(instance).await.ok()?;
		if let Some(id) = object
			.values
			.get("id")
			.and_then(|id| id.parse::<tg::process::Id>().ok())
		{
			let referent = tg::Referent {
				node: id,
				options: options.clone(),
			};
			process = Some(referent);
		}
		current = object.source.as_ref().map(|source| {
			let error = match &source.node {
				tg::Either::Left(object) => tg::Error::with_object((**object).clone()),
				tg::Either::Right(error) => (**error).clone(),
			};
			(error, source.options.clone())
		});
	}
	let process = tg::Process::<tg::Value>::with_referent(process?);

	// Read the end of the process's output, and keep its last lines.
	let options = tg::process::stdio::read::Options {
		length: None,
		location: None,
		position: Some(std::io::SeekFrom::End(-4096)),
		size: None,
		streams: vec![
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		],
		timeout: Some(std::time::Duration::ZERO),
	};
	let stdio = process
		.try_read_stdio_with_instance(instance, options)
		.await
		.ok()??;
	let bytes = stdio
		.try_fold(Vec::new(), |mut bytes, chunk| async move {
			bytes.extend_from_slice(&chunk.bytes);
			Ok(bytes)
		})
		.await
		.ok()?;
	let text = String::from_utf8_lossy(&bytes);
	let lines = text.lines().collect::<Vec<_>>();
	let lines = &lines[lines.len().saturating_sub(20)..];
	let output = lines
		.iter()
		.map(|line| format!("  {line}"))
		.collect::<Vec<_>>()
		.join("\n");
	if output.trim().is_empty() {
		return None;
	}

	Some(output)
}
