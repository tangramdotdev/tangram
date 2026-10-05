use {
	super::{debugger, protocol},
	std::{cell::RefCell, collections::VecDeque, rc::Rc},
};

#[derive(Clone)]
pub(super) struct Channel {
	debugger: Option<Rc<debugger::State>>,
	messages: Rc<RefCell<VecDeque<String>>>,
}

impl Channel {
	pub(super) fn new(debugger: Option<Rc<debugger::State>>) -> Self {
		Self {
			debugger,
			messages: Rc::new(RefCell::new(VecDeque::new())),
		}
	}

	pub(super) fn take_response(&self, id: i32) -> Option<protocol::Response> {
		let mut messages = self.messages.borrow_mut();
		let index = messages.iter().position(|message| {
			serde_json::from_str::<protocol::BaseMessage>(message)
				.ok()
				.and_then(|message| message.id)
				== Some(id)
		})?;
		let message = messages.remove(index)?;
		serde_json::from_str(&message).ok()
	}

	pub(super) fn take_context_id(&self) -> Option<u64> {
		let messages = self.messages.borrow();
		messages.iter().find_map(|message| {
			let base = serde_json::from_str::<protocol::BaseMessage>(message).ok()?;
			if base.method.as_deref() != Some("Runtime.executionContextCreated") {
				return None;
			}
			let notification =
				serde_json::from_str::<protocol::ExecutionContextCreatedNotification>(message)
					.ok()?;
			Some(notification.params.context.id)
		})
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
		let Ok(base) = serde_json::from_str::<protocol::BaseMessage>(message) else {
			return false;
		};
		if base.method.as_deref() != Some("Debugger.scriptParsed") {
			return false;
		}
		let Ok(notification) = serde_json::from_str::<protocol::ScriptParsedNotification>(message)
		else {
			return false;
		};
		notification.params.url == "main"
	}

	fn hide_generated_script_url(message: &mut String) {
		let Ok(base) = serde_json::from_str::<protocol::BaseMessage>(message) else {
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
}

impl v8::inspector::ChannelImpl for Channel {
	fn send_response(&self, _call_id: i32, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
		self.push_message(message);
	}

	fn send_notification(&self, message: v8::UniquePtr<v8::inspector::StringBuffer>) {
		self.push_message(message);
	}

	fn flush_protocol_notifications(&self) {}
}
