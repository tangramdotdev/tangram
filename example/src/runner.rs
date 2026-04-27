use {
	crate::inspector::{self, InspectorRuntime},
	crossbeam_channel::unbounded,
	std::{
		fs,
		path::{Path, PathBuf},
	},
};

pub fn run(script: PathBuf, inspector_options: Option<inspector::Options>) -> Result<(), String> {
	let source = fs::read_to_string(&script)
		.map_err(|error| format!("failed to read the script: {error}"))?;
	let script_url = file_url(&script);
	let (command_tx, command_rx) = unbounded();
	let server = if let Some(options) = inspector_options {
		let server = inspector::Server::start(options.address, script_url.clone(), command_tx)?;
		eprintln!("Debugger listening on {}", server.url());
		eprintln!("Visit chrome://inspect to connect to the debugger.");
		if options.mode != inspector::Mode::Inspect {
			eprintln!("Waiting for the debugger to connect.");
		}
		Some((server, options.mode))
	} else {
		None
	};

	let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
	isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
	let inspector = server
		.as_ref()
		.map(|(_, _)| InspectorRuntime::new(isolate, command_rx));
	v8::scope!(let scope, isolate);
	let context = v8::Context::new(scope, Default::default());
	let scope = &mut v8::ContextScope::new(scope, context);
	install_console(scope, context);

	if let Some(inspector) = inspector.as_ref() {
		inspector.register_context(context);
	}
	if let Some((_, mode)) = server.as_ref()
		&& let Some(inspector) = inspector.as_ref()
	{
		inspector.prepare_to_run(*mode);
	}

	let result = execute_script(scope, &source, &script_url);
	scope.perform_microtask_checkpoint();
	if let Some(inspector) = inspector.as_ref() {
		inspector.drain_commands();
		inspector.context_destroyed(context);
	}

	result
}

fn execute_script(
	scope: &mut v8::PinScope<'_, '_>,
	source: &str,
	script_url: &str,
) -> Result<(), String> {
	v8::tc_scope!(let scope, scope);
	let source = v8::String::new(scope, source).unwrap();
	let resource_name = v8::String::new(scope, script_url).unwrap();
	let origin = v8::ScriptOrigin::new(
		scope,
		resource_name.into(),
		0,
		0,
		false,
		0,
		None,
		false,
		false,
		false,
		None,
	);
	let Some(script) = v8::Script::compile(scope, source, Some(&origin)) else {
		return Err(format_exception(scope));
	};
	if script.run(scope).is_none() {
		return Err(format_exception(scope));
	}
	Ok(())
}

fn format_exception(scope: &mut v8::PinnedRef<'_, v8::TryCatch<v8::HandleScope>>) -> String {
	let Some(exception) = scope.exception() else {
		return "javascript execution failed".to_owned();
	};
	let exception_string = exception
		.to_string(scope)
		.map(|string| string.to_rust_string_lossy(scope))
		.unwrap_or_else(|| "javascript exception".to_owned());
	let Some(message) = scope.message() else {
		return exception_string;
	};
	let filename = message.get_script_resource_name(scope).map_or_else(
		|| "(unknown)".to_owned(),
		|value| {
			value
				.to_string(scope)
				.map(|string| string.to_rust_string_lossy(scope))
				.unwrap_or_else(|| "(unknown)".to_owned())
		},
	);
	let line_number = message.get_line_number(scope).unwrap_or_default();
	let mut output = format!("{filename}:{line_number}: {exception_string}");
	if let Some(source_line) = message.get_source_line(scope) {
		let source_line = source_line
			.to_string(scope)
			.map(|string| string.to_rust_string_lossy(scope))
			.unwrap_or_default();
		output.push('\n');
		output.push_str(&source_line);
		output.push('\n');
		output.push_str(&" ".repeat(message.get_start_column()));
		output.push_str(
			&"^".repeat(
				message
					.get_end_column()
					.saturating_sub(message.get_start_column()),
			),
		);
	}
	if let Some(stack_trace) = scope.stack_trace() {
		let stack_trace = unsafe { v8::Local::<v8::String>::cast_unchecked(stack_trace) };
		if let Some(stack_trace) = stack_trace.to_string(scope) {
			output.push('\n');
			output.push_str(&stack_trace.to_rust_string_lossy(scope));
		}
	}
	output
}

fn install_console(scope: &mut v8::PinScope<'_, '_>, context: v8::Local<v8::Context>) {
	let console = v8::Object::new(scope);
	let log = v8::Function::new(scope, console_log).unwrap();
	let error = v8::Function::new(scope, console_error).unwrap();
	let log_key = v8::String::new_external_onebyte_static(scope, b"log").unwrap();
	let error_key = v8::String::new_external_onebyte_static(scope, b"error").unwrap();
	console.set(scope, log_key.into(), log.into());
	console.set(scope, error_key.into(), error.into());
	let console_key = v8::String::new_external_onebyte_static(scope, b"console").unwrap();
	context
		.global(scope)
		.set(scope, console_key.into(), console.into());
}

fn console_log(
	scope: &mut v8::PinScope<'_, '_>,
	args: v8::FunctionCallbackArguments,
	_return_value: v8::ReturnValue,
) {
	println!("{}", format_console_args(scope, args));
}

fn console_error(
	scope: &mut v8::PinScope<'_, '_>,
	args: v8::FunctionCallbackArguments,
	_return_value: v8::ReturnValue,
) {
	eprintln!("{}", format_console_args(scope, args));
}

fn format_console_args(
	scope: &mut v8::PinScope<'_, '_>,
	args: v8::FunctionCallbackArguments,
) -> String {
	(0..args.length())
		.map(|index| {
			args.get(index)
				.to_string(scope)
				.map(|value| value.to_rust_string_lossy(scope))
				.unwrap_or_else(|| "<unprintable>".to_owned())
		})
		.collect::<Vec<_>>()
		.join(" ")
}

fn file_url(path: &Path) -> String {
	let path = fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
	format!("file://{}", path.display())
}
