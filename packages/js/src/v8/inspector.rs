use {
	axum::response::IntoResponse as _,
	futures::{SinkExt as _, StreamExt as _, task::AtomicWaker},
	serde::{Deserialize, Serialize},
	std::{
		cell::{Cell, RefCell},
		collections::VecDeque,
		rc::Rc,
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
			mpsc as std_mpsc,
		},
		thread,
	},
	tangram_client::prelude::*,
	tokio::sync::{mpsc as tokio_mpsc, oneshot},
};

const CONTEXT_GROUP_ID: i32 = 1;

const DEFAULT_INSPECTOR_ADDRESS: std::net::SocketAddr =
	std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9229);

pub struct Inspector {
	channel: InspectorChannel,
	context_id: Option<u64>,
	debugger: Option<Debugger>,
	inner: v8::inspector::V8Inspector,
	next_id: i32,
	pending: Option<PendingCommand>,
	repl: Option<crate::repl::Receiver>,
	repl_closed: bool,
	session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	startup_error: Option<tg::Error>,
}

pub struct PollState {
	pub clear_rejection: bool,
}

struct Debugger {
	_server: Server,
	mode: crate::inspect::Mode,
	state: Rc<DebuggerState>,
}

struct DebuggerState {
	active: RefCell<Option<ActiveConnection>>,
	break_on_next_user_script: Cell<bool>,
	commands: DebuggerCommandReceiver,
	paused: Cell<bool>,
	run_if_waiting_for_debugger: Cell<bool>,
	session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	waiting_for_debugger: Cell<bool>,
}

struct ActiveConnection {
	id: u64,
	outgoing: tokio_mpsc::UnboundedSender<String>,
}

#[derive(Clone)]
struct DebuggerCommandSender {
	sender: std_mpsc::Sender<DebuggerCommand>,
	waker: Arc<AtomicWaker>,
}

struct DebuggerCommandReceiver {
	receiver: std_mpsc::Receiver<DebuggerCommand>,
	waker: Arc<AtomicWaker>,
}

enum DebuggerCommand {
	Connected {
		id: u64,
		outgoing: tokio_mpsc::UnboundedSender<String>,
	},
	Message {
		id: u64,
		message: String,
	},
	Disconnected {
		id: u64,
	},
}

struct InspectorClient {
	debugger: Option<Rc<DebuggerState>>,
}

#[derive(Clone)]
struct InspectorChannel {
	debugger: Option<Rc<DebuggerState>>,
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

struct Server {
	shutdown: Option<oneshot::Sender<()>>,
	thread: Option<thread::JoinHandle<()>>,
	url: String,
}

#[derive(Clone)]
struct ServerState {
	command: DebuggerCommandSender,
	connection_counter: Arc<AtomicU64>,
	host: std::net::SocketAddr,
	target: Arc<Target>,
}

struct Target {
	id: String,
	title: String,
	url: String,
}

#[derive(Debug, Deserialize)]
struct BaseMessage {
	id: Option<i32>,
	method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CdpCommandEnvelope {
	id: Option<i64>,
	method: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScriptParsedNotification {
	params: ScriptParsedParams,
}

#[derive(Debug, Deserialize)]
struct ScriptParsedParams {
	url: String,
}

#[derive(Debug, Serialize)]
struct CdpNotification {
	method: &'static str,
}

#[derive(Debug, Serialize)]
struct CdpResponse<T> {
	id: i64,
	result: T,
}

#[derive(Debug, Serialize)]
struct EmptyResult {}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetMetadata {
	description: String,
	devtools_frontend_url: String,
	id: String,
	title: String,
	#[serde(rename = "type")]
	target_type: String,
	url: String,
	web_socket_debugger_url: String,
}

#[derive(Clone, Debug, Serialize)]
struct VersionMetadata {
	#[serde(rename = "Browser")]
	browser: String,
	#[serde(rename = "Protocol-Version")]
	protocol_version: String,
	#[serde(rename = "V8-Version")]
	v8_version: String,
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
	#[allow(clippy::needless_pass_by_value)]
	pub fn new(
		isolate: &mut v8::Isolate,
		inspect: Option<crate::inspect::Options>,
		repl: Option<crate::repl::Receiver>,
	) -> Self {
		let session = Rc::new(RefCell::new(None));
		let (debugger, startup_error) =
			if let Some(crate::inspect::Options { addr, mode }) = inspect {
				match Debugger::new(addr, mode, session.clone()) {
					Ok(debugger) => (Some(debugger), None),
					Err(error) => (None, Some(error)),
				}
			} else {
				(None, None)
			};
		let debugger_state = debugger.as_ref().map(|debugger| debugger.state.clone());
		let inspector_client = v8::inspector::V8InspectorClient::new(Box::new(InspectorClient {
			debugger: debugger_state.clone(),
		}));
		let inspector = v8::inspector::V8Inspector::create(isolate, inspector_client);
		let repl_closed = repl.is_none();
		Self {
			channel: InspectorChannel::new(debugger_state),
			context_id: None,
			debugger,
			inner: inspector,
			next_id: 1,
			pending: None,
			repl,
			repl_closed,
			session,
			startup_error,
		}
	}

	pub fn register_context(
		&mut self,
		scope: &mut v8::PinScope,
		context: v8::Local<v8::Context>,
	) -> tg::Result<()> {
		if let Some(error) = self.startup_error.take() {
			return Err(error);
		}
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
		*self.session.borrow_mut() = Some(session);
		self.post(scope, "Runtime.enable", None::<()>)?;
		self.context_id = self.channel.take_context_id();
		if self.context_id.is_none() {
			return Err(tg::error!(
				"failed to get the V8 inspector execution context"
			));
		}
		if let Some(debugger) = &self.debugger {
			debugger.prepare_to_run();
		}
		Ok(())
	}

	pub fn poll(
		&mut self,
		cx: &mut std::task::Context<'_>,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
	) -> std::task::Poll<tg::Result<Option<PollState>>> {
		if let Some(debugger) = &self.debugger {
			unsafe { isolate.enter() };
			let handled = debugger.poll(cx);
			unsafe { isolate.exit() };
			if handled {
				return std::task::Poll::Ready(Ok(Some(PollState {
					clear_rejection: false,
				})));
			}
		}

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
		let mut session = self.session.borrow_mut();
		let Some(session) = session.as_mut() else {
			return Err(tg::error!("the V8 inspector session is not available"));
		};
		session.dispatch_protocol_message(v8::inspector::StringView::from(message.as_bytes()));
		scope.perform_microtask_checkpoint();
		Ok(id)
	}
}

impl Debugger {
	fn new(
		addr: Option<std::net::SocketAddr>,
		mode: crate::inspect::Mode,
		session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	) -> tg::Result<Self> {
		let (command, commands) = debugger_command_channel();
		let server = Server::start(addr.unwrap_or(DEFAULT_INSPECTOR_ADDRESS), command)?;
		eprintln!("debugger listening on {}", server.url());
		if mode != crate::inspect::Mode::Normal {
			eprintln!("waiting for the debugger to connect");
		}
		let state = Rc::new(DebuggerState {
			active: RefCell::new(None),
			break_on_next_user_script: Cell::new(false),
			commands,
			paused: Cell::new(false),
			run_if_waiting_for_debugger: Cell::new(false),
			session,
			waiting_for_debugger: Cell::new(false),
		});
		Ok(Self {
			_server: server,
			mode,
			state,
		})
	}

	fn poll(&self, cx: &mut std::task::Context<'_>) -> bool {
		self.state.poll(cx)
	}

	fn prepare_to_run(&self) {
		match self.mode {
			crate::inspect::Mode::Normal => {
				self.state.drain_commands();
			},
			crate::inspect::Mode::Wait => {
				self.state.wait_for_session();
				self.state.waiting_for_debugger.set(true);
				while !self.state.run_if_waiting_for_debugger.get() {
					if let Some(command) = self.state.recv_command() {
						self.state.handle_command(command);
					} else {
						break;
					}
				}
				self.state.waiting_for_debugger.set(false);
			},
			crate::inspect::Mode::Break => {
				self.state.wait_for_session();
				self.state.waiting_for_debugger.set(true);
				while !self.state.run_if_waiting_for_debugger.get() {
					if let Some(command) = self.state.recv_command() {
						self.state.handle_command(command);
					} else {
						break;
					}
				}
				self.state.waiting_for_debugger.set(false);
				self.state.break_on_next_user_script.set(true);
			},
		}
	}
}

impl DebuggerState {
	fn poll(&self, cx: &mut std::task::Context<'_>) -> bool {
		self.commands.waker.register(cx.waker());
		self.drain_commands()
	}

	fn drain_commands(&self) -> bool {
		let mut handled = false;
		while let Ok(command) = self.commands.receiver.try_recv() {
			self.handle_command(command);
			handled = true;
		}
		handled
	}

	fn recv_command(&self) -> Option<DebuggerCommand> {
		self.commands.receiver.recv().ok()
	}

	fn wait_for_session(&self) {
		while self.active.borrow().is_none() {
			if let Some(command) = self.recv_command() {
				self.handle_command(command);
			} else {
				break;
			}
		}
	}

	fn handle_command(&self, command: DebuggerCommand) {
		match command {
			DebuggerCommand::Connected { id, outgoing } => {
				*self.active.borrow_mut() = Some(ActiveConnection { id, outgoing });
			},
			DebuggerCommand::Message { id, message } => {
				if self.active.borrow().as_ref().map(|active| active.id) != Some(id) {
					return;
				}
				if self.handle_local_command(&message) {
					return;
				}
				if let Some(session) = self.session.borrow_mut().as_mut() {
					session.dispatch_protocol_message(v8::inspector::StringView::from(
						message.as_bytes(),
					));
				}
			},
			DebuggerCommand::Disconnected { id } => {
				let is_active = self.active.borrow().as_ref().map(|active| active.id) == Some(id);
				if is_active {
					*self.active.borrow_mut() = None;
					self.paused.set(false);
				}
			},
		}
	}

	fn handle_local_command(&self, message: &str) -> bool {
		let Ok(envelope) = serde_json::from_str::<CdpCommandEnvelope>(message) else {
			return false;
		};
		let Some(method) = envelope.method.as_deref() else {
			return false;
		};
		match method {
			"NodeRuntime.enable" | "NodeRuntime.disable" => {
				if let Some(id) = envelope.id {
					let response = CdpResponse {
						id,
						result: EmptyResult {},
					};
					self.send_protocol_message(&response);
				}
				if method == "NodeRuntime.enable" && self.waiting_for_debugger.get() {
					let notification = CdpNotification {
						method: "NodeRuntime.waitingForDebugger",
					};
					self.send_protocol_message(&notification);
				}
				true
			},
			"Runtime.runIfWaitingForDebugger" => {
				self.run_if_waiting_for_debugger.set(true);
				false
			},
			_ => false,
		}
	}

	fn handle_inspector_message(&self, message: &str) {
		if !self.break_on_next_user_script.get() {
			return;
		}
		let Ok(base) = serde_json::from_str::<BaseMessage>(message) else {
			return;
		};
		if base.method.as_deref() != Some("Debugger.scriptParsed") {
			return;
		}
		let Ok(notification) = serde_json::from_str::<ScriptParsedNotification>(message) else {
			return;
		};
		if notification.params.url.is_empty() || notification.params.url == "main" {
			return;
		}
		self.break_on_next_user_script.set(false);
		if let Some(session) = self.session.borrow_mut().as_mut() {
			let reason = v8::inspector::StringView::from(&b"debugCommand"[..]);
			let detail = v8::inspector::StringView::empty();
			session.schedule_pause_on_next_statement(reason, detail);
		}
	}

	fn send_protocol_message<T: Serialize>(&self, message: &T) {
		if let Some(active) = self.active.borrow().as_ref()
			&& let Ok(message) = serde_json::to_string(message)
		{
			let _ = active.outgoing.send(message);
		}
	}

	fn send_inspector_message(&self, message: String) {
		if let Some(active) = self.active.borrow().as_ref() {
			let _ = active.outgoing.send(message);
		}
	}
}

impl DebuggerCommandSender {
	fn send(&self, command: DebuggerCommand) -> Result<(), std_mpsc::SendError<DebuggerCommand>> {
		let result = self.sender.send(command);
		self.waker.wake();
		result
	}
}

impl InspectorClient {
	fn debugger(&self) -> Option<&DebuggerState> {
		self.debugger.as_deref()
	}
}

impl v8::inspector::V8InspectorClientImpl for InspectorClient {
	fn run_message_loop_on_pause(&self, _context_group_id: i32) {
		let Some(debugger) = self.debugger() else {
			return;
		};
		debugger.paused.set(true);
		while debugger.paused.get() {
			if let Some(command) = debugger.recv_command() {
				debugger.handle_command(command);
			} else {
				break;
			}
		}
	}

	fn quit_message_loop_on_pause(&self) {
		if let Some(debugger) = self.debugger() {
			debugger.paused.set(false);
		}
	}

	fn run_if_waiting_for_debugger(&self, _context_group_id: i32) {
		if let Some(debugger) = self.debugger() {
			debugger.run_if_waiting_for_debugger.set(true);
		}
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

impl InspectorChannel {
	fn new(debugger: Option<Rc<DebuggerState>>) -> Self {
		Self {
			debugger,
			messages: Rc::new(RefCell::new(VecDeque::new())),
		}
	}

	fn push_message(&self, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
		let mut message = message.unwrap().string().to_string();
		if Self::is_snapshot_script_parsed(&message) {
			return;
		}
		if let Some(debugger) = &self.debugger {
			debugger.handle_inspector_message(&message);
		}
		Self::hide_generated_script_url(&mut message);
		self.messages.borrow_mut().push_back(message.clone());
		if let Some(debugger) = &self.debugger {
			debugger.send_inspector_message(message);
		}
	}

	fn is_snapshot_script_parsed(message: &str) -> bool {
		let Ok(base) = serde_json::from_str::<BaseMessage>(message) else {
			return false;
		};
		if base.method.as_deref() != Some("Debugger.scriptParsed") {
			return false;
		}
		let Ok(notification) = serde_json::from_str::<ScriptParsedNotification>(message) else {
			return false;
		};
		notification.params.url == "main"
	}

	fn hide_generated_script_url(message: &mut String) {
		let Ok(base) = serde_json::from_str::<BaseMessage>(message) else {
			return;
		};
		if base.method.as_deref() != Some("Debugger.scriptParsed") {
			return;
		}
		let Ok(mut value) = serde_json::from_str::<serde_json::Value>(message) else {
			return;
		};
		let Some(params) = value
			.get_mut("params")
			.and_then(serde_json::Value::as_object_mut)
		else {
			return;
		};
		if params
			.get("sourceMapURL")
			.and_then(serde_json::Value::as_str)
			.is_none_or(str::is_empty)
		{
			return;
		}
		params.insert("url".to_owned(), serde_json::Value::String(String::new()));
		params.insert(
			"embedderName".to_owned(),
			serde_json::Value::String(String::new()),
		);
		if let Ok(value) = serde_json::to_string(&value) {
			*message = value;
		}
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

impl Server {
	fn start(address: std::net::SocketAddr, command: DebuggerCommandSender) -> tg::Result<Self> {
		let listener = std::net::TcpListener::bind(address)
			.map_err(|source| tg::error!(!source, "failed to bind the inspector server"))?;
		listener
			.set_nonblocking(true)
			.map_err(|source| tg::error!(!source, "failed to configure the inspector server"))?;
		let host = listener
			.local_addr()
			.map_err(|source| tg::error!(!source, "failed to get the inspector server address"))?;
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let target = Arc::new(Target {
			id,
			title: format!("tangram js [pid: {}]", std::process::id()),
			url: "tangram:js".to_owned(),
		});
		let state = ServerState {
			command,
			connection_counter: Arc::new(AtomicU64::new(1)),
			host,
			target,
		};
		let url = state.websocket_debugger_url(&host.to_string());
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let thread = thread::spawn(move || {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("failed to build the inspector runtime");
			runtime.block_on(async move {
				let listener = tokio::net::TcpListener::from_std(listener)
					.expect("failed to create the inspector listener");
				let app = axum::Router::new()
					.route("/json", axum::routing::get(json_list))
					.route("/json/list", axum::routing::get(json_list))
					.route("/json/version", axum::routing::get(json_version))
					.route("/ws/{id}", axum::routing::get(ws))
					.with_state(state);
				let shutdown = async {
					let _ = shutdown_rx.await;
				};
				let _ = axum::serve(listener, app)
					.with_graceful_shutdown(shutdown)
					.await;
			});
		});
		Ok(Self {
			shutdown: Some(shutdown_tx),
			thread: Some(thread),
			url,
		})
	}

	fn url(&self) -> &str {
		&self.url
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		if let Some(shutdown) = self.shutdown.take() {
			let _ = shutdown.send(());
		}
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}

impl ServerState {
	fn target_metadata(&self, host: &str) -> TargetMetadata {
		TargetMetadata {
			description: "tangram js".to_owned(),
			devtools_frontend_url: self.frontend_url(host),
			id: self.target.id.clone(),
			title: self.target.title.clone(),
			target_type: "node".to_owned(),
			url: self.target.url.clone(),
			web_socket_debugger_url: self.websocket_debugger_url(host),
		}
	}

	fn frontend_url(&self, host: &str) -> String {
		format!(
			"devtools://devtools/bundled/js_app.html?ws={host}/ws/{}&experiments=true&v8only=true",
			self.target.id
		)
	}

	fn websocket_debugger_url(&self, host: &str) -> String {
		format!("ws://{host}/ws/{}", self.target.id)
	}
}

async fn json_list(
	axum::extract::State(state): axum::extract::State<ServerState>,
	headers: axum::http::HeaderMap,
) -> axum::Json<Vec<TargetMetadata>> {
	let host = request_host(&headers).unwrap_or_else(|| state.host.to_string());
	axum::Json(vec![state.target_metadata(&host)])
}

async fn json_version() -> axum::Json<VersionMetadata> {
	axum::Json(VersionMetadata {
		browser: "tangram".to_owned(),
		protocol_version: "1.3".to_owned(),
		v8_version: v8::V8::get_version().to_owned(),
	})
}

async fn ws(
	axum::extract::Path(id): axum::extract::Path<String>,
	axum::extract::State(state): axum::extract::State<ServerState>,
	upgrade: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
	if id != state.target.id {
		return axum::http::StatusCode::NOT_FOUND.into_response();
	}
	upgrade
		.on_upgrade(move |socket| websocket(socket, state))
		.into_response()
}

async fn websocket(socket: axum::extract::ws::WebSocket, state: ServerState) {
	let id = state.connection_counter.fetch_add(1, Ordering::SeqCst);
	let (outgoing, mut outgoing_rx) = tokio_mpsc::unbounded_channel::<String>();
	let _ = state
		.command
		.send(DebuggerCommand::Connected { id, outgoing });
	let (mut sender, mut receiver) = socket.split();
	loop {
		tokio::select! {
			Some(message) = outgoing_rx.recv() => {
				if sender.send(axum::extract::ws::Message::Text(message.into())).await.is_err() {
					break;
				}
			},
			message = receiver.next() => {
				match message {
					Some(Ok(axum::extract::ws::Message::Text(message))) => {
						let _ = state.command.send(DebuggerCommand::Message {
							id,
							message: message.to_string(),
						});
					},
					Some(Ok(axum::extract::ws::Message::Close(_)) | Err(_)) | None => break,
					Some(Ok(_)) => {},
				}
			},
		}
	}
	let _ = state.command.send(DebuggerCommand::Disconnected { id });
}

fn request_host(headers: &axum::http::HeaderMap) -> Option<String> {
	headers
		.get(axum::http::header::HOST)
		.and_then(|host| host.to_str().ok())
		.map(ToOwned::to_owned)
}

fn debugger_command_channel() -> (DebuggerCommandSender, DebuggerCommandReceiver) {
	let (sender, receiver) = std_mpsc::channel();
	let waker = Arc::new(AtomicWaker::new());
	(
		DebuggerCommandSender {
			sender,
			waker: waker.clone(),
		},
		DebuggerCommandReceiver { receiver, waker },
	)
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
