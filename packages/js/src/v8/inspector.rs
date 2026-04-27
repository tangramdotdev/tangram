use {
	serde::{Deserialize, Serialize},
	std::{cell::RefCell, collections::VecDeque, rc::Rc},
	tangram_client::prelude::*,
	tokio::sync::oneshot,
};

const CONTEXT_GROUP_ID: i32 = 1;

pub struct Inspector {
	channel: InspectorChannel,
	repl: Option<crate::repl::Receiver>,
	repl_closed: bool,
	context_id: Option<u64>,
	inner: v8::inspector::V8Inspector,
	next_id: i32,
	pending: Option<PendingCommand>,
	session: Option<v8::inspector::V8InspectorSession>,
}

pub struct PollState {
	pub clear_rejection: bool,
}

struct InspectorClient;

#[derive(Clone)]
struct InspectorChannel {
	messages: Rc<RefCell<VecDeque<String>>>,
}

struct PendingCommand {
	id: i32,
	kind: PendingCommandKind,
}

enum PendingCommandKind {
	Evaluate {
		response: oneshot::Sender<tg::Result<()>>,
	},
	Log {
		response: oneshot::Sender<tg::Result<()>>,
	},
}

#[derive(Debug, Deserialize)]
struct BaseMessage {
	id: Option<i32>,
	method: Option<String>,
}

#[derive(Debug, Serialize)]
struct InspectorRequest<P> {
	id: i32,
	method: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	params: Option<P>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeEvaluateParams<'a> {
	expression: &'a str,
	await_promise: bool,
	repl_mode: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	context_id: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeCallFunctionOnParams<'a> {
	function_declaration: &'a str,
	arguments: Vec<CallArgument<'a>>,
	await_promise: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	execution_context_id: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CallArgument<'a> {
	#[serde(skip_serializing_if = "Option::is_none")]
	object_id: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	unserializable_value: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	value: Option<RemoteValue>,
}

#[derive(Debug, Deserialize)]
struct InspectorResponse {
	result: Option<RuntimeEvaluateResponse>,
	error: Option<ProtocolError>,
}

#[derive(Debug, Deserialize)]
struct ProtocolError {
	code: i64,
	message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeEvaluateResponse {
	result: Option<RemoteObject>,
	exception_details: Option<ExceptionDetails>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExceptionDetails {
	text: Option<String>,
	exception: Option<RemoteObject>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteObject {
	#[serde(rename = "type")]
	kind: Option<String>,
	subtype: Option<String>,
	value: Option<RemoteValue>,
	unserializable_value: Option<String>,
	description: Option<String>,
	object_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum RemoteValue {
	Null,
	Bool(bool),
	Number(f64),
	String(String),
}

#[derive(Debug, Deserialize)]
struct ExecutionContextCreatedNotification {
	params: ExecutionContextCreatedParams,
}

#[derive(Debug, Deserialize)]
struct ExecutionContextCreatedParams {
	context: ExecutionContextDescription,
}

#[derive(Debug, Deserialize)]
struct ExecutionContextDescription {
	id: u64,
}

impl Inspector {
	pub fn new(
		isolate: &mut v8::Isolate,
		inspect: Option<crate::inspect::Options>,
		repl: Option<crate::repl::Receiver>,
	) -> Self {
		let inspector_client = v8::inspector::V8InspectorClient::new(Box::new(InspectorClient));
		let inspector = v8::inspector::V8Inspector::create(isolate, inspector_client);
		let repl_closed = repl.is_none();
		Self {
			channel: InspectorChannel::new(),
			repl,
			repl_closed,
			context_id: None,
			inner: inspector,
			next_id: 1,
			pending: None,
			session: None,
		}
	}

	pub fn register_context(
		&mut self,
		scope: &mut v8::PinScope,
		context: v8::Local<v8::Context>,
	) -> tg::Result<()> {
		self.inner.context_created(
			context,
			CONTEXT_GROUP_ID,
			v8::inspector::StringView::from(&b"tangram"[..]),
			v8::inspector::StringView::from(&br#"{"isDefault":true,"type":"default"}"#[..]),
		);
		let session = self.inner.connect(
			CONTEXT_GROUP_ID,
			v8::inspector::Channel::new(Box::new(self.channel.clone())),
			v8::inspector::StringView::from(&b"{}"[..]),
			v8::inspector::V8InspectorClientTrustLevel::FullyTrusted,
		);
		self.session = Some(session);
		self.post(scope, "Runtime.enable", None::<()>)?;
		self.context_id = self.channel.take_context_id();
		if self.context_id.is_none() {
			return Err(tg::error!(
				"failed to get the V8 inspector execution context"
			));
		}
		Ok(())
	}

	pub fn poll(
		&mut self,
		cx: &mut std::task::Context<'_>,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
	) -> std::task::Poll<tg::Result<Option<PollState>>> {
		if let Some(clear_rejection) = self.complete_pending_command(isolate, context) {
			return std::task::Poll::Ready(Ok(Some(PollState { clear_rejection })));
		}

		if self.pending.is_some() {
			return std::task::Poll::Pending;
		}

		let command = match self.poll_command(cx) {
			std::task::Poll::Ready(Some(command)) => command,
			std::task::Poll::Ready(None) => {
				return std::task::Poll::Ready(Ok(None));
			},
			std::task::Poll::Pending => {
				return std::task::Poll::Pending;
			},
		};

		self.start_command(isolate, context, command);
		if let Some(clear_rejection) = self.complete_pending_command(isolate, context) {
			std::task::Poll::Ready(Ok(Some(PollState { clear_rejection })))
		} else {
			std::task::Poll::Ready(Ok(Some(PollState {
				clear_rejection: false,
			})))
		}
	}

	pub fn context_destroyed(&mut self, context: v8::Local<v8::Context>) {
		self.inner.context_destroyed(context);
	}

	#[must_use]
	pub fn is_handling_command(&self) -> bool {
		self.pending.is_some()
	}

	fn poll_command(
		&mut self,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<crate::repl::Command>> {
		let Some(repl) = self.repl.as_mut() else {
			return std::task::Poll::Ready(None);
		};
		if self.repl_closed {
			return std::task::Poll::Ready(None);
		}
		match repl.poll_recv(cx) {
			std::task::Poll::Pending => std::task::Poll::Pending,
			std::task::Poll::Ready(Some(command)) => std::task::Poll::Ready(Some(command)),
			std::task::Poll::Ready(None) => {
				self.repl_closed = true;
				std::task::Poll::Ready(None)
			},
		}
	}

	fn start_command(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
		command: crate::repl::Command,
	) {
		self.start_evaluation(isolate, context, command);
	}

	fn start_evaluation(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
		command: crate::repl::Command,
	) {
		let crate::repl::Command { source, response } = command;
		unsafe { isolate.enter() };
		let result = {
			v8::scope!(scope, isolate);
			let context = v8::Local::new(scope, context.clone());
			let scope = &mut v8::ContextScope::new(scope, context);
			self.evaluate(scope, &source)
		};
		unsafe { isolate.exit() };
		match result {
			Ok(id) => {
				self.pending = Some(PendingCommand {
					id,
					kind: PendingCommandKind::Evaluate { response },
				});
			},
			Err(error) => {
				let _ = response.send(Err(error));
			},
		}
	}

	fn complete_pending_command(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
	) -> Option<bool> {
		let pending = self.pending.as_ref()?;
		let response = self.channel.take_response(pending.id)?;
		let pending = self.pending.take().unwrap();
		match pending.kind {
			PendingCommandKind::Evaluate { response: sender } => {
				let result = response_remote_object(&response);
				match result {
					Ok(remote) => {
						self.start_log(isolate, context, remote, sender);
					},
					Err(message) => {
						let _ = sender.send(Err(tg::error!("{message}")));
					},
				}
				Some(true)
			},
			PendingCommandKind::Log { response: sender } => {
				let result = response_remote_object(&response)
					.map(|_| ())
					.map_err(|message| tg::error!("{message}"));
				let _ = sender.send(result);
				Some(true)
			},
		}
	}

	fn evaluate(&mut self, scope: &mut v8::PinScope, expression: &str) -> tg::Result<i32> {
		let params = RuntimeEvaluateParams {
			expression,
			await_promise: true,
			repl_mode: true,
			context_id: self.context_id,
		};
		self.post(scope, "Runtime.evaluate", Some(params))
	}

	fn start_log(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
		remote: &RemoteObject,
		response: oneshot::Sender<tg::Result<()>>,
	) {
		unsafe { isolate.enter() };
		let result = {
			v8::scope!(scope, isolate);
			let context = v8::Local::new(scope, context.clone());
			let scope = &mut v8::ContextScope::new(scope, context);
			self.log(scope, remote)
		};
		unsafe { isolate.exit() };
		match result {
			Ok(id) => {
				self.pending = Some(PendingCommand {
					id,
					kind: PendingCommandKind::Log { response },
				});
			},
			Err(error) => {
				let _ = response.send(Err(error));
			},
		}
	}

	fn log(&mut self, scope: &mut v8::PinScope, remote: &RemoteObject) -> tg::Result<i32> {
		let params = RuntimeCallFunctionOnParams {
			function_declaration: "function(value) { globalThis._ = value; console.log(value); }",
			arguments: vec![CallArgument::new(remote)],
			await_promise: true,
			execution_context_id: self.context_id,
		};
		self.post(scope, "Runtime.callFunctionOn", Some(params))
	}

	fn post<P: Serialize>(
		&mut self,
		scope: &mut v8::PinScope,
		method: &'static str,
		params: Option<P>,
	) -> tg::Result<i32> {
		let id = self.next_id;
		self.next_id += 1;
		let message = InspectorRequest { id, method, params };
		let message = serde_json::to_string(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the inspector request"))?;
		let Some(session) = self.session.as_mut() else {
			return Err(tg::error!("the V8 inspector session is not available"));
		};
		session.dispatch_protocol_message(v8::inspector::StringView::from(message.as_bytes()));
		scope.perform_microtask_checkpoint();
		Ok(id)
	}
}

impl<'a> CallArgument<'a> {
	fn new(remote: &'a RemoteObject) -> Self {
		if let Some(object_id) = remote.object_id.as_deref() {
			Self {
				object_id: Some(object_id),
				unserializable_value: None,
				value: None,
			}
		} else if let Some(unserializable_value) = remote.unserializable_value.as_deref() {
			Self {
				object_id: None,
				unserializable_value: Some(unserializable_value),
				value: None,
			}
		} else {
			Self {
				object_id: None,
				unserializable_value: None,
				value: if remote.subtype.as_deref() == Some("null") {
					Some(RemoteValue::Null)
				} else {
					remote.value.clone()
				},
			}
		}
	}
}

impl v8::inspector::V8InspectorClientImpl for InspectorClient {}

impl InspectorChannel {
	fn new() -> Self {
		Self {
			messages: Rc::new(RefCell::new(VecDeque::new())),
		}
	}

	fn push_message(&self, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
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

impl v8::inspector::ChannelImpl for InspectorChannel {
	fn send_response(&self, _call_id: i32, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
		self.push_message(message);
	}

	fn send_notification(&self, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
		self.push_message(message);
	}

	fn flush_protocol_notifications(&self) {}
}

fn response_remote_object(response: &InspectorResponse) -> Result<&RemoteObject, String> {
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
			.map_or_else(|| "unknown exception".to_owned(), format_remote_value);
		return Err(format!("{text}: {description}"));
	}

	result
		.result
		.as_ref()
		.ok_or_else(|| "malformed evaluation result".to_owned())
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
			RemoteValue::Null => "null".to_owned(),
			RemoteValue::Bool(value) => value.to_string(),
			RemoteValue::Number(value) => value.to_string(),
			RemoteValue::String(value) => value.clone(),
		};
	}

	if let Some(description) = &remote.description {
		return description.clone();
	}

	remote.kind.as_deref().unwrap_or("<unknown>").to_owned()
}
