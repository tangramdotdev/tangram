use {
	super::{ClientMessage, Response, client_subject, server_subject},
	tangram_client::prelude::*,
	tangram_messenger::Payload as _,
};

struct Kind;

impl super::Kind for Kind {
	type Arg = u64;

	type Id = tg::indexer::Id;

	type Output = u64;

	const NAME: &'static str = "indexer";

	async fn available(_server: &crate::Server) -> tg::Result<Vec<Self::Id>> {
		Ok(Vec::new())
	}
}

#[test]
fn subjects_address_a_named_server() {
	let id = tg::indexer::Id::new();
	assert_eq!(server_subject::<Kind>(&id), format!("indexers.{id}.server"));
	assert_eq!(client_subject::<Kind>("1"), "indexers.client.1");
}

#[test]
fn unavailable_messages_round_trip() {
	let message = ClientMessage::<u64>::Unavailable(super::Ack { id: "1".to_owned() });
	let bytes = message.serialize().unwrap();
	let message = ClientMessage::<u64>::deserialize(bytes).unwrap();
	assert!(matches!(message, ClientMessage::Unavailable(ack) if ack.id == "1"));
}

#[test]
fn responses_carry_outputs_and_errors() {
	let response = Response::new("1".to_owned(), Ok(2u64));
	assert_eq!(response.output, Some(2));
	assert!(response.error.is_none());
	let response = Response::<u64>::new("1".to_owned(), Err(tg::error!("failed")));
	assert!(response.output.is_none());
	assert!(response.error.is_some());
}
