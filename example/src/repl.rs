use {
	crate::{
		Engine,
		protocol::{
			BaseMessage, ExecutionContextCreatedNotification, InspectorRequest, InspectorResponse,
			RemoteObject, RemoteValue, RuntimeEvaluateParams,
		},
	},
	rquickjs as qjs,
	std::{
		cell::RefCell,
		collections::VecDeque,
		io::{self, Write as _},
		rc::Rc,
	},
	v8::inspector::{
		Channel, ChannelImpl, StringBuffer, StringView, V8Inspector, V8InspectorClient,
		V8InspectorClientImpl, V8InspectorClientTrustLevel, V8InspectorSession,
	},
};

const CONTEXT_GROUP_ID: i32 = 1;

pub fn run(engine: Engine) {
	match engine {
		Engine::V8 => run_v8_repl(),
		Engine::QuickJs => run_quickjs_repl(),
	}
}

fn run_v8_repl() {
	let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
	isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);

	let inspector_client = V8InspectorClient::new(Box::new(InspectorClient));
	let inspector = V8Inspector::create(isolate, inspector_client);

	v8::scope!(let scope, isolate);
	let context = v8::Context::new(scope, Default::default());
	let scope = &mut v8::ContextScope::new(scope, context);

	inspector.context_created(
		context,
		CONTEXT_GROUP_ID,
		StringView::from(&b"repl"[..]),
		StringView::from(&br#"{"isDefault":true,"type":"default"}"#[..]),
	);

	let channel = InspectorChannel::new();
	let session = inspector.connect(
		CONTEXT_GROUP_ID,
		Channel::new(Box::new(channel.clone())),
		StringView::from(&b"{}"[..]),
		V8InspectorClientTrustLevel::FullyTrusted,
	);

	let mut repl = V8Repl {
		channel,
		context_id: None,
		next_id: 1,
		session,
	};

	let _ = repl.post(scope, "Runtime.enable", None::<()>);
	repl.context_id = repl.take_context_id();

	let banner = format!(
		"V8 {} inspector REPL. Type .exit to quit.",
		v8::V8::get_version()
	);
	run_input_loop(&banner, |input| repl.evaluate(scope, input));

	inspector.context_destroyed(context);
}

fn run_quickjs_repl() {
	let repl = match QuickJsRepl::new() {
		Ok(repl) => repl,
		Err(error) => {
			eprintln!("{error}");
			return;
		},
	};

	run_input_loop("QuickJS rquickjs REPL. Type .exit to quit.", |input| {
		repl.evaluate(input)
	});
}

fn run_input_loop(banner: &str, mut evaluate: impl FnMut(&str) -> Result<String, String>) {
	println!("{banner}");
	let stdin = io::stdin();
	loop {
		print!("> ");
		io::stdout().flush().unwrap();

		let mut input = String::new();
		let bytes = stdin.read_line(&mut input).unwrap();
		if bytes == 0 {
			println!();
			break;
		}

		let input = input.trim_end();
		if input == ".exit" {
			break;
		}
		if input.trim().is_empty() {
			continue;
		}

		match evaluate(input) {
			Ok(output) => println!("{output}"),
			Err(error) => eprintln!("{error}"),
		}
	}
}

struct InspectorClient;

impl V8InspectorClientImpl for InspectorClient {}

#[derive(Clone)]
struct InspectorChannel {
	messages: Rc<RefCell<VecDeque<String>>>,
}

impl InspectorChannel {
	fn new() -> Self {
		Self {
			messages: Rc::new(RefCell::new(VecDeque::new())),
		}
	}

	fn push_message(&self, message: v8::UniquePtr<StringBuffer>) {
		self.messages
			.borrow_mut()
			.push_back(message.unwrap().string().to_string());
	}

	fn take_response(&self, id: i32) -> Option<InspectorResponse> {
		let mut messages = self.messages.borrow_mut();
		let index = messages.iter().position(|message| {
			serde_json::from_str::<BaseMessage>(message)
				.ok()
				.and_then(|message| message.id)
				== Some(id)
		})?;
		let message = messages.remove(index)?;
		serde_json::from_str(&message).ok()
	}

	fn take_context_id(&self) -> Option<u64> {
		let messages = self.messages.borrow();
		messages.iter().find_map(|message| {
			let base = serde_json::from_str::<BaseMessage>(message).ok()?;
			if base.method.as_deref() != Some("Runtime.executionContextCreated") {
				return None;
			}
			let notification =
				serde_json::from_str::<ExecutionContextCreatedNotification>(message).ok()?;
			Some(notification.params.context.id)
		})
	}
}

impl ChannelImpl for InspectorChannel {
	fn send_response(&self, _call_id: i32, message: v8::UniquePtr<StringBuffer>) {
		self.push_message(message);
	}

	fn send_notification(&self, message: v8::UniquePtr<StringBuffer>) {
		self.push_message(message);
	}

	fn flush_protocol_notifications(&self) {}
}

struct V8Repl {
	channel: InspectorChannel,
	context_id: Option<u64>,
	next_id: i32,
	session: V8InspectorSession,
}

impl V8Repl {
	fn evaluate(&mut self, scope: &mut v8::PinScope, expression: &str) -> Result<String, String> {
		let params = RuntimeEvaluateParams {
			expression,
			await_promise: true,
			repl_mode: true,
			context_id: self.context_id,
		};
		let response = self
			.post(scope, "Runtime.evaluate", Some(params))
			.ok_or_else(|| "inspector did not return an evaluation response".to_owned())?;
		format_evaluate_response(&response)
	}

	fn post<P: serde::Serialize>(
		&mut self,
		scope: &mut v8::PinScope,
		method: &'static str,
		params: Option<P>,
	) -> Option<InspectorResponse> {
		let id = self.next_id;
		self.next_id += 1;

		let message = InspectorRequest { id, method, params };
		let message = serde_json::to_string(&message).unwrap();
		self.session
			.dispatch_protocol_message(StringView::from(message.as_bytes()));

		// This is the important bit for REPL mode with explicit microtasks. V8's
		// inspector REPL evaluation resolves through a microtask even for simple
		// top-level await cases.
		scope.perform_microtask_checkpoint();

		self.channel.take_response(id)
	}

	fn take_context_id(&self) -> Option<u64> {
		self.channel.take_context_id()
	}
}

fn format_evaluate_response(response: &InspectorResponse) -> Result<String, String> {
	if let Some(error) = &response.error {
		return Err(format!("{}: {}", error.code, error.message));
	}

	let result = response
		.result
		.as_ref()
		.ok_or_else(|| "malformed evaluation response".to_owned())?;

	if let Some(details) = &result.exception_details {
		let text = details.text.as_deref().unwrap_or("exception");
		let description = details
			.exception
			.as_ref()
			.map(format_remote_value)
			.unwrap_or_else(|| "unknown exception".to_owned());
		return Err(format!("{text}: {description}"));
	}

	let remote = result
		.result
		.as_ref()
		.ok_or_else(|| "malformed evaluation result".to_owned())?;
	Ok(format_remote_value(remote))
}

fn format_remote_value(remote: &RemoteObject) -> String {
	if let Some(value) = &remote.unserializable_value {
		return value.clone();
	}

	if remote.subtype.as_deref() == Some("null") {
		return "null".to_owned();
	}

	if let Some(value) = &remote.value {
		return match value {
			RemoteValue::Bool(value) => value.to_string(),
			RemoteValue::Number(value) => {
				if value.fract() == 0.0 {
					(*value as i64).to_string()
				} else {
					value.to_string()
				}
			},
			RemoteValue::String(value) => value.clone(),
		};
	}

	if let Some(description) = &remote.description {
		return description.clone();
	}

	remote.kind.as_deref().unwrap_or("<unknown>").to_owned()
}

struct QuickJsRepl {
	_runtime: qjs::Runtime,
	context: qjs::Context,
}

impl QuickJsRepl {
	fn new() -> Result<Self, String> {
		let runtime = qjs::Runtime::new().map_err(|error| error.to_string())?;
		let context = qjs::Context::full(&runtime).map_err(|error| error.to_string())?;
		let repl = Self {
			_runtime: runtime,
			context,
		};
		repl.install_format_helper()?;
		Ok(repl)
	}

	fn install_format_helper(&self) -> Result<(), String> {
		self.context.with(|ctx| {
			ctx.eval::<(), _>(
				r#"
				Object.defineProperty(globalThis, "__replFormat", {
					configurable: true,
					value(value) {
						if (value === undefined) {
							return "undefined";
						}
						if (value === null) {
							return "null";
						}
						if (typeof value === "string") {
							return value;
						}
						if (typeof value === "bigint") {
							return `${value}n`;
						}
						if (typeof value === "symbol" || typeof value === "function") {
							return String(value);
						}
						if (typeof value === "object") {
							if (value instanceof Error) {
								let text = String(value);
								return value.stack ? `${text}\n${value.stack}` : text;
							}
							try {
								let json = JSON.stringify(value);
								if (json !== undefined) {
									return json;
								}
							} catch {
							}
						}
						return String(value);
					},
				});
				"#,
			)
			.map_err(|error| format_quickjs_error(&ctx, error))
		})
	}

	fn evaluate(&self, source: &str) -> Result<String, String> {
		self.context
			.with(|ctx| match evaluate_quickjs(&ctx, source) {
				Ok(output) => Ok(output),
				Err(error) => Err(format_quickjs_error(&ctx, error)),
			})
	}
}

fn evaluate_quickjs(ctx: &qjs::Ctx<'_>, source: &str) -> qjs::Result<String> {
	let mut options = qjs::context::EvalOptions::default();
	options.strict = false;
	options.backtrace_barrier = true;
	options.promise = true;
	options.filename = Some("<evalScript>".to_owned());

	let promise = ctx.eval_with_options::<qjs::Promise, _>(source, options)?;
	let value = promise.finish::<qjs::Value>()?;
	let value = unwrap_quickjs_completion(value)?;
	ctx.globals().set("_", value.clone())?;
	format_quickjs_value(ctx, value)
}

fn unwrap_quickjs_completion<'js>(value: qjs::Value<'js>) -> qjs::Result<qjs::Value<'js>> {
	if let Some(object) = value.as_object()
		&& object.contains_key("value")?
	{
		return object.get("value");
	}
	Ok(value)
}

fn format_quickjs_value<'js>(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<String> {
	let globals = ctx.globals();
	let format = globals.get::<_, qjs::Function>("__replFormat")?;
	format.call((value,))
}

fn format_quickjs_error(ctx: &qjs::Ctx<'_>, error: qjs::Error) -> String {
	if matches!(error, qjs::Error::Exception) {
		let exception = ctx.catch();
		format_quickjs_value(ctx, exception).unwrap_or_else(|_| "javascript exception".to_owned())
	} else {
		error.to_string()
	}
}
