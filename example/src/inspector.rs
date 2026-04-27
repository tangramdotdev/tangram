use {
	crate::protocol::{
		CdpCommandEnvelope, CdpNotification, CdpResponse, EmptyResult, TargetMetadata,
		VersionMetadata,
	},
	axum::{
		Router,
		extract::{
			Path, State, WebSocketUpgrade,
			ws::{Message, WebSocket},
		},
		http::{HeaderMap, StatusCode, header::HOST},
		response::IntoResponse,
		routing::get,
	},
	crossbeam_channel::{Receiver, Sender},
	futures_util::{SinkExt as _, StreamExt as _},
	std::{
		cell::{Cell, RefCell},
		net::SocketAddr,
		rc::Rc,
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		thread,
	},
	tokio::sync::{mpsc, oneshot},
	uuid::Uuid,
	v8::inspector::{
		Channel, ChannelImpl, StringBuffer, StringView, V8Inspector, V8InspectorClient,
		V8InspectorClientImpl, V8InspectorClientTrustLevel, V8InspectorSession,
	},
};

const CONTEXT_GROUP_ID: i32 = 1;

#[derive(Clone, Copy)]
pub struct Options {
	pub address: SocketAddr,
	pub mode: Mode,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Mode {
	Inspect,
	Wait,
	Break,
}

pub enum Command {
	Connected {
		id: u64,
		outgoing: mpsc::UnboundedSender<String>,
	},
	Message {
		id: u64,
		message: String,
	},
	Disconnected {
		id: u64,
	},
}

pub struct InspectorRuntime {
	_inspector: V8Inspector,
	state: Rc<RuntimeState>,
	session: Rc<RefCell<Option<V8InspectorSession>>>,
}

pub struct Server {
	shutdown: Option<oneshot::Sender<()>>,
	thread: Option<thread::JoinHandle<()>>,
	url: String,
}

#[derive(Clone)]
struct ServerState {
	command_tx: Sender<Command>,
	connection_counter: Arc<AtomicU64>,
	host: SocketAddr,
	target: Arc<Target>,
}

struct Target {
	id: Uuid,
	title: String,
	url: String,
}

struct RuntimeState {
	active: RefCell<Option<ActiveConnection>>,
	commands: Receiver<Command>,
	paused: Cell<bool>,
	run_if_waiting_for_debugger: Cell<bool>,
	session: Rc<RefCell<Option<V8InspectorSession>>>,
	waiting_for_debugger: Cell<bool>,
}

struct ActiveConnection {
	id: u64,
	outgoing: mpsc::UnboundedSender<String>,
}

struct InspectorClient {
	state: Rc<RuntimeState>,
}

#[derive(Clone)]
struct InspectorChannel {
	state: Rc<RuntimeState>,
}

impl InspectorRuntime {
	pub fn new(isolate: &mut v8::Isolate, commands: Receiver<Command>) -> Self {
		let session = Rc::new(RefCell::new(None));
		let state = Rc::new(RuntimeState {
			active: RefCell::new(None),
			commands,
			paused: Cell::new(false),
			run_if_waiting_for_debugger: Cell::new(false),
			session: session.clone(),
			waiting_for_debugger: Cell::new(false),
		});
		let inspector_client = V8InspectorClient::new(Box::new(InspectorClient {
			state: state.clone(),
		}));
		let inspector = V8Inspector::create(isolate, inspector_client);
		Self {
			_inspector: inspector,
			state,
			session,
		}
	}

	pub fn register_context(&self, context: v8::Local<v8::Context>) {
		self._inspector.context_created(
			context,
			CONTEXT_GROUP_ID,
			StringView::from(&b"script"[..]),
			StringView::from(&br#"{"isDefault":true,"type":"default"}"#[..]),
		);
		let v8_session = self._inspector.connect(
			CONTEXT_GROUP_ID,
			Channel::new(Box::new(InspectorChannel {
				state: self.state.clone(),
			})),
			StringView::from(&b"{}"[..]),
			V8InspectorClientTrustLevel::FullyTrusted,
		);
		*self.session.borrow_mut() = Some(v8_session);
	}

	pub fn context_destroyed(&self, context: v8::Local<v8::Context>) {
		self._inspector.context_destroyed(context);
	}

	pub fn drain_commands(&self) {
		self.state.drain_commands();
	}

	pub fn prepare_to_run(&self, mode: Mode) {
		match mode {
			Mode::Inspect => self.drain_commands(),
			Mode::Wait => {
				self.wait_for_session();
				self.drain_commands();
			},
			Mode::Break => {
				self.wait_for_session();
				self.state.waiting_for_debugger.set(true);
				while !self.state.run_if_waiting_for_debugger.get() {
					if let Ok(command) = self.state.commands.recv() {
						self.state.handle_command(command);
					} else {
						break;
					}
				}
				self.state.waiting_for_debugger.set(false);
				if let Some(session) = self.session.borrow().as_ref() {
					let reason = StringView::from(&b"debugCommand"[..]);
					let detail = StringView::empty();
					session.schedule_pause_on_next_statement(reason, detail);
				}
			},
		}
	}

	fn wait_for_session(&self) {
		while self.state.active.borrow().is_none() {
			if let Ok(command) = self.state.commands.recv() {
				self.state.handle_command(command);
			} else {
				break;
			}
		}
	}
}

impl Server {
	pub fn start(
		address: SocketAddr,
		script_url: String,
		command_tx: Sender<Command>,
	) -> Result<Self, String> {
		let listener = std::net::TcpListener::bind(address)
			.map_err(|error| format!("failed to bind the inspector server: {error}"))?;
		listener
			.set_nonblocking(true)
			.map_err(|error| format!("failed to configure the inspector server: {error}"))?;
		let host = listener
			.local_addr()
			.map_err(|error| format!("failed to get the inspector server address: {error}"))?;
		let target = Arc::new(Target {
			id: Uuid::new_v4(),
			title: format!("rusty-v8-real-repl [pid: {}]", std::process::id()),
			url: script_url,
		});
		let state = ServerState {
			command_tx,
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
				let app = Router::new()
					.route("/json", get(json_list))
					.route("/json/list", get(json_list))
					.route("/json/version", get(json_version))
					.route("/ws/{id}", get(ws))
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

	pub fn url(&self) -> &str {
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

impl RuntimeState {
	fn drain_commands(&self) {
		while let Ok(command) = self.commands.try_recv() {
			self.handle_command(command);
		}
	}

	fn handle_command(&self, command: Command) {
		match command {
			Command::Connected { id, outgoing } => {
				*self.active.borrow_mut() = Some(ActiveConnection { id, outgoing });
			},
			Command::Message { id, message } => {
				if self.active.borrow().as_ref().map(|active| active.id) != Some(id) {
					return;
				}
				if self.handle_local_command(&message) {
					return;
				}
				if let Some(session) = self.session.borrow().as_ref() {
					session.dispatch_protocol_message(StringView::from(message.as_bytes()));
				}
			},
			Command::Disconnected { id } => {
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

	fn send_protocol_message<T: serde::Serialize>(&self, message: &T) {
		if let Some(active) = self.active.borrow().as_ref()
			&& let Ok(message) = serde_json::to_string(message)
		{
			let _ = active.outgoing.send(message);
		}
	}
}

impl V8InspectorClientImpl for InspectorClient {
	fn run_message_loop_on_pause(&self, _context_group_id: i32) {
		self.state.paused.set(true);
		while self.state.paused.get() {
			if let Ok(command) = self.state.commands.recv() {
				self.state.handle_command(command);
			} else {
				break;
			}
		}
	}

	fn quit_message_loop_on_pause(&self) {
		self.state.paused.set(false);
	}

	fn run_if_waiting_for_debugger(&self, _context_group_id: i32) {
		self.state.run_if_waiting_for_debugger.set(true);
	}
}

impl ChannelImpl for InspectorChannel {
	fn send_response(&self, _call_id: i32, message: v8::UniquePtr<StringBuffer>) {
		self.send_message(message);
	}

	fn send_notification(&self, message: v8::UniquePtr<StringBuffer>) {
		self.send_message(message);
	}

	fn flush_protocol_notifications(&self) {}
}

impl InspectorChannel {
	fn send_message(&self, message: v8::UniquePtr<StringBuffer>) {
		if let Some(active) = self.state.active.borrow().as_ref() {
			let _ = active.outgoing.send(message.unwrap().string().to_string());
		}
	}
}

impl ServerState {
	fn target_metadata(&self, host: &str) -> TargetMetadata {
		TargetMetadata {
			description: "rusty-v8-real-repl".to_owned(),
			devtools_frontend_url: self.frontend_url(host),
			id: self.target.id,
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
	State(state): State<ServerState>,
	headers: HeaderMap,
) -> axum::Json<Vec<TargetMetadata>> {
	let host = request_host(&headers).unwrap_or_else(|| state.host.to_string());
	axum::Json(vec![state.target_metadata(&host)])
}

async fn json_version() -> axum::Json<VersionMetadata> {
	axum::Json(VersionMetadata {
		browser: "rusty-v8-real-repl".to_owned(),
		protocol_version: "1.3".to_owned(),
		v8_version: v8::V8::get_version().to_owned(),
	})
}

async fn ws(
	Path(id): Path<Uuid>,
	State(state): State<ServerState>,
	upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
	if id != state.target.id {
		return StatusCode::NOT_FOUND.into_response();
	}
	upgrade
		.on_upgrade(move |socket| websocket(socket, state))
		.into_response()
}

async fn websocket(socket: WebSocket, state: ServerState) {
	let id = state.connection_counter.fetch_add(1, Ordering::SeqCst);
	let (outgoing, mut outgoing_rx) = mpsc::unbounded_channel::<String>();
	let _ = state.command_tx.send(Command::Connected { id, outgoing });
	let (mut sender, mut receiver) = socket.split();
	loop {
		tokio::select! {
			Some(message) = outgoing_rx.recv() => {
				if sender.send(Message::Text(message.into())).await.is_err() {
					break;
				}
			},
			message = receiver.next() => {
				match message {
					Some(Ok(Message::Text(message))) => {
						let _ = state.command_tx.send(Command::Message {
							id,
							message: message.to_string(),
						});
					},
					Some(Ok(Message::Close(_))) | None => break,
					Some(Ok(_)) => {},
					Some(Err(_)) => break,
				}
			},
		}
	}
	let _ = state.command_tx.send(Command::Disconnected { id });
}

fn request_host(headers: &HeaderMap) -> Option<String> {
	headers
		.get(HOST)
		.and_then(|host| host.to_str().ok())
		.map(ToOwned::to_owned)
}
