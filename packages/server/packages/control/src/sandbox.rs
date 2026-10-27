use tangram_client::prelude::*;

impl crate::Output for tg::sandbox::control::ClientMessage {
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

impl crate::Input<tg::sandbox::control::ServerMessage> for tg::sandbox::control::ClientMessage {
	fn kind(&self) -> crate::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::InputKind::Ack { id: &ack.id },
			Self::Notification(_) => crate::InputKind::Message { id: None },
			Self::Request(request) => crate::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Response(response) => crate::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::sandbox::control::ServerMessage {
		tg::sandbox::control::ServerMessage::Ack(tg::sandbox::control::ServerAck { id })
	}
}

impl crate::Output for tg::sandbox::control::ServerMessage {
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

impl crate::Input<tg::sandbox::control::ClientMessage> for tg::sandbox::control::ServerMessage {
	fn kind(&self) -> crate::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::InputKind::Ack { id: &ack.id },
			Self::Notification(_) => crate::InputKind::Message { id: None },
			Self::Request(request) => crate::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Response(response) => crate::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::sandbox::control::ClientMessage {
		tg::sandbox::control::ClientMessage::Ack(tg::sandbox::control::ClientAck { id })
	}
}
