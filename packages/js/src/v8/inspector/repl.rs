use {
	super::{Inspector, error, protocol},
	tangram_client::prelude::*,
	tokio::sync::oneshot,
};

pub(super) struct PendingCommand {
	pub(super) id: i32,
	kind: PendingCommandKind,
}

enum PendingCommandKind {
	Evaluate {
		response: oneshot::Sender<tg::Result<()>>,
	},
	Exception {
		fallback: String,
		response: oneshot::Sender<tg::Result<()>>,
	},
	Log {
		response: oneshot::Sender<tg::Result<()>>,
	},
}

impl Inspector {
	pub(super) fn poll_command(
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

	pub(super) fn start_evaluation(
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

	pub(super) fn complete_pending_command(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
	) -> Option<bool> {
		let pending = self.pending.as_ref()?;
		let response = self.channel.take_response(pending.id)?;
		let pending = self.pending.take().unwrap();
		match pending.kind {
			PendingCommandKind::Evaluate { response: sender } => {
				match protocol::response_result(&response) {
					Ok(protocol::ResponseResult::Value(remote)) => {
						self.start_log(isolate, context, remote, sender);
					},
					Ok(protocol::ResponseResult::Exception(details)) => {
						self.start_exception(isolate, context, details, sender);
					},
					Err(message) => {
						let _ = sender.send(Err(tg::error!("{message}")));
					},
				}
				Some(true)
			},
			PendingCommandKind::Exception {
				fallback,
				response: sender,
			} => {
				let error = error::from_response(&response, &fallback);
				let _ = sender.send(Err(error));
				Some(true)
			},
			PendingCommandKind::Log { response: sender } => {
				match protocol::response_result(&response) {
					Ok(protocol::ResponseResult::Value(_)) => {
						let _ = sender.send(Ok(()));
					},
					Ok(protocol::ResponseResult::Exception(details)) => {
						self.start_exception(isolate, context, details, sender);
					},
					Err(message) => {
						let _ = sender.send(Err(tg::error!("{message}")));
					},
				}
				Some(true)
			},
		}
	}

	fn evaluate(&mut self, scope: &mut v8::PinScope, expression: &str) -> tg::Result<i32> {
		let params = protocol::EvaluateParams {
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
		remote: &protocol::RemoteObject,
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

	fn start_exception(
		&mut self,
		isolate: &mut v8::OwnedIsolate,
		context: &v8::Global<v8::Context>,
		details: &protocol::ExceptionDetails,
		response: oneshot::Sender<tg::Result<()>>,
	) {
		let fallback = error::format_exception_details(details);
		let Some(exception) = details.exception.as_ref() else {
			let _ = response.send(Err(tg::error!("{fallback}")));
			return;
		};
		unsafe { isolate.enter() };
		let result = {
			v8::scope!(scope, isolate);
			let context = v8::Local::new(scope, context.clone());
			let scope = &mut v8::ContextScope::new(scope, context);
			self.exception(scope, exception)
		};
		unsafe { isolate.exit() };
		match result {
			Ok(id) => {
				self.pending = Some(PendingCommand {
					id,
					kind: PendingCommandKind::Exception { fallback, response },
				});
			},
			Err(error) => {
				let _ = response.send(Err(error));
			},
		}
	}

	fn exception(
		&mut self,
		scope: &mut v8::PinScope,
		remote: &protocol::RemoteObject,
	) -> tg::Result<i32> {
		let params = protocol::CallFunctionOnParams {
			function_declaration: error::EXCEPTION_TO_ERROR_DATA_FUNCTION,
			arguments: vec![protocol::CallArgument::new(remote)],
			await_promise: true,
			return_by_value: true,
			execution_context_id: self.context_id,
		};
		self.post(scope, "Runtime.callFunctionOn", Some(params))
	}

	fn log(
		&mut self,
		scope: &mut v8::PinScope,
		remote: &protocol::RemoteObject,
	) -> tg::Result<i32> {
		let params = protocol::CallFunctionOnParams {
			function_declaration: "function(value) { globalThis._ = value; console.log(value); }",
			arguments: vec![protocol::CallArgument::new(remote)],
			await_promise: true,
			return_by_value: false,
			execution_context_id: self.context_id,
		};
		self.post(scope, "Runtime.callFunctionOn", Some(params))
	}
}
