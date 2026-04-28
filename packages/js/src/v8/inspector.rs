use {
	serde::Serialize,
	std::{cell::RefCell, rc::Rc},
	tangram_client::prelude::*,
};

mod channel;
mod debugger;
mod error;
mod http;
mod protocol;
mod repl;

const CONTEXT_GROUP_ID: i32 = 1;

pub struct Inspector {
	channel: channel::Channel,
	context_id: Option<u64>,
	debugger: Option<debugger::Debugger>,
	inner: v8::inspector::V8Inspector,
	next_id: i32,
	pending: Option<repl::PendingCommand>,
	repl: Option<crate::repl::Receiver>,
	repl_closed: bool,
	session: Rc<RefCell<Option<v8::inspector::V8InspectorSession>>>,
	startup_error: Option<tg::Error>,
}

pub struct PollState {
	pub clear_rejection: bool,
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
				match debugger::Debugger::new(addr, mode, session.clone()) {
					Ok(debugger) => (Some(debugger), None),
					Err(error) => (None, Some(error)),
				}
			} else {
				(None, None)
			};
		let debugger_state = debugger.as_ref().map(|debugger| debugger.state.clone());
		let client = debugger::Client::new(debugger_state.clone());
		let inspector_client = v8::inspector::V8InspectorClient::new(Box::new(client));
		let inspector = v8::inspector::V8Inspector::create(isolate, inspector_client);
		let repl_closed = repl.is_none();
		Self {
			channel: channel::Channel::new(debugger_state),
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

		self.start_evaluation(isolate, context, command);
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

	pub(super) fn post<P: Serialize>(
		&mut self,
		scope: &mut v8::PinScope,
		method: &'static str,
		params: Option<P>,
	) -> tg::Result<i32> {
		let id = self.next_id;
		self.next_id += 1;
		let message = protocol::Request { id, method, params };
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
