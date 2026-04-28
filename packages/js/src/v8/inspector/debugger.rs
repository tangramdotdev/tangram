use {
	super::{http, protocol},
	futures::task::AtomicWaker,
	serde::Serialize,
	std::{
		cell::{Cell, RefCell},
		rc::Rc,
		sync::{
			Arc,
			mpsc::{self as std_mpsc},
		},
	},
	tangram_client::prelude::*,
	tokio::sync::mpsc as tokio_mpsc,
};

pub(super) struct Debugger {
	pub(super) state: Rc<State>,
	_server: http::Server,
	mode: crate::inspect::Mode,
}

pub(super) struct State {
	active: RefCell<Option<ActiveConnection>>,
	break_on_next_user_script: Cell<bool>,
	commands: CommandReceiver,
	paused: Cell<bool>,
	run_if_waiting_for_debugger: Cell<bool>,
	session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	waiting_for_debugger: Cell<bool>,
}

pub(super) struct Client {
	debugger: Option<Rc<State>>,
}

struct ActiveConnection {
	id: u64,
	outgoing: tokio_mpsc::UnboundedSender<String>,
}

#[derive(Clone)]
pub(super) struct CommandSender {
	sender: std_mpsc::Sender<Command>,
	waker: Arc<AtomicWaker>,
}

struct CommandReceiver {
	receiver: std_mpsc::Receiver<Command>,
	waker: Arc<AtomicWaker>,
}

pub(super) enum Command {
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

impl Debugger {
	pub(super) fn new(
		addr: Option<std::net::SocketAddr>,
		mode: crate::inspect::Mode,
		session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	) -> tg::Result<Self> {
		let (command, commands) = command_channel();
		let server = http::Server::start(addr.unwrap_or(http::DEFAULT_ADDRESS), command)?;
		eprintln!("debugger listening on {}", server.url());
		if mode != crate::inspect::Mode::Normal {
			eprintln!("waiting for the debugger to connect");
		}
		let state = Rc::new(State {
			active: RefCell::new(None),
			break_on_next_user_script: Cell::new(false),
			commands,
			paused: Cell::new(false),
			run_if_waiting_for_debugger: Cell::new(false),
			session,
			waiting_for_debugger: Cell::new(false),
		});
		Ok(Self {
			state,
			_server: server,
			mode,
		})
	}

	pub(super) fn poll(&self, cx: &mut std::task::Context<'_>) -> bool {
		self.state.poll(cx)
	}

	pub(super) fn prepare_to_run(&self) {
		match self.mode {
			crate::inspect::Mode::Normal => {
				self.state.drain_commands();
			},
			crate::inspect::Mode::Wait => {
				self.state.wait_for_debugger();
			},
			crate::inspect::Mode::Break => {
				self.state.wait_for_debugger();
				self.state.break_on_next_user_script.set(true);
			},
		}
	}
}

impl State {
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

	fn recv_command(&self) -> Option<Command> {
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

	fn wait_for_debugger(&self) {
		self.wait_for_session();
		self.waiting_for_debugger.set(true);
		while !self.run_if_waiting_for_debugger.get() {
			if let Some(command) = self.recv_command() {
				self.handle_command(command);
			} else {
				break;
			}
		}
		self.waiting_for_debugger.set(false);
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
				if let Some(session) = self.session.borrow_mut().as_mut() {
					session.dispatch_protocol_message(v8::inspector::StringView::from(
						message.as_bytes(),
					));
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
		let Ok(envelope) = serde_json::from_str::<protocol::CdpCommandEnvelope>(message) else {
			return false;
		};
		let Some(method) = envelope.method.as_deref() else {
			return false;
		};
		match method {
			"NodeRuntime.enable" | "NodeRuntime.disable" => {
				if let Some(id) = envelope.id {
					let response = protocol::CdpResponse {
						id,
						result: protocol::EmptyResult {},
					};
					self.send_protocol_message(&response);
				}
				if method == "NodeRuntime.enable" && self.waiting_for_debugger.get() {
					let notification = protocol::CdpNotification {
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

	pub(super) fn handle_inspector_message(&self, message: &str) {
		if !self.break_on_next_user_script.get() {
			return;
		}
		let Ok(base) = serde_json::from_str::<protocol::BaseMessage>(message) else {
			return;
		};
		if base.method.as_deref() != Some("Debugger.scriptParsed") {
			return;
		}
		let Ok(notification) = serde_json::from_str::<protocol::ScriptParsedNotification>(message)
		else {
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

	pub(super) fn send_inspector_message(&self, message: String) {
		if let Some(active) = self.active.borrow().as_ref() {
			let _ = active.outgoing.send(message);
		}
	}
}

impl Client {
	pub(super) fn new(debugger: Option<Rc<State>>) -> Self {
		Self { debugger }
	}

	fn debugger(&self) -> Option<&State> {
		self.debugger.as_deref()
	}
}

impl CommandSender {
	pub(super) fn send(&self, command: Command) -> Result<(), std_mpsc::SendError<Command>> {
		let result = self.sender.send(command);
		self.waker.wake();
		result
	}
}

impl v8::inspector::V8InspectorClientImpl for Client {
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

fn command_channel() -> (CommandSender, CommandReceiver) {
	let (sender, receiver) = std_mpsc::channel();
	let waker = Arc::new(AtomicWaker::new());
	(
		CommandSender {
			sender,
			waker: waker.clone(),
		},
		CommandReceiver { receiver, waker },
	)
}
