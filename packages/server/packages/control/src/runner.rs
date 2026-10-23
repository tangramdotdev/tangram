use tangram_client::prelude::*;

impl crate::Output for tg::runner::control::ClientMessage {
	fn is_request(&self) -> bool {
		matches!(self, Self::Request(_))
	}

	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::Input<tg::runner::control::ServerMessage> for tg::runner::control::ClientMessage {
	fn kind(&self) -> crate::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Notification(_) => crate::InputKind::Message { id: None },
			Self::Response(response) => crate::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::runner::control::ServerMessage {
		tg::runner::control::ServerMessage::Ack(tg::runner::control::ServerAck { id })
	}
}

impl crate::Output for tg::runner::control::ServerMessage {
	fn is_request(&self) -> bool {
		matches!(self, Self::Request(_))
	}

	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::Input<tg::runner::control::ClientMessage> for tg::runner::control::ServerMessage {
	fn kind(&self) -> crate::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Response(response) => crate::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::runner::control::ClientMessage {
		tg::runner::control::ClientMessage::Ack(tg::runner::control::ClientAck { id })
	}
}
