use {
	super::*,
	crate::{Messenger as _, test::TestMessenger},
	futures::StreamExt as _,
	std::sync::Mutex,
};

struct Test;

impl Protocol for Test {
	type Request = Bytes;
	type Response = Bytes;
}

/// An observer that records the names of the events.
#[derive(Default)]
struct Record {
	events: Mutex<Vec<&'static str>>,
}

impl Observer<Test> for Record {
	fn observe<'a>(&'a self, event: Event<'a, Test>) -> BoxFuture<'a, ()> {
		let name = match event {
			Event::Acknowledged { .. } => "acknowledged",
			Event::Attempt { .. } => "attempt",
			Event::Cancelled { .. } => "cancelled",
			Event::HeartbeatResponded { .. } => "heartbeat_responded",
			Event::HeartbeatSent { .. } => "heartbeat_sent",
			Event::RequestSent { .. } => "request_sent",
			Event::Responded { .. } => "responded",
			Event::ResponseAcknowledged { .. } => "response_acknowledged",
			Event::Started { .. } => "started",
		};
		self.events.lock().unwrap().push(name);
		Box::pin(async {})
	}
}

impl Record {
	fn count(&self, name: &str) -> usize {
		self.events
			.lock()
			.unwrap()
			.iter()
			.filter(|event| **event == name)
			.count()
	}
}

fn options() -> Options {
	Options {
		attempt_ttl: Duration::from_millis(200),
		heartbeat_interval: Duration::from_millis(20),
		idle_timeout: Duration::from_millis(100),
		recovery_timeout: Duration::from_millis(500),
		request_timeout: Duration::from_millis(500),
		retry_interval: Duration::from_millis(10),
	}
}

async fn server(messenger: &TestMessenger, options: Options) -> Server<Test> {
	serve::<Test, _>(messenger, "test", options, Arc::new(Ignore))
		.await
		.unwrap()
}

fn clients(messenger: &TestMessenger, options: Options) -> Arc<Clients<Test, TestMessenger>> {
	Arc::new(Clients::new(messenger.clone(), options, Arc::new(Ignore)).unwrap())
}

fn spawn_request(
	clients: &Arc<Clients<Test, TestMessenger>>,
	payload: &'static str,
) -> tokio::task::JoinHandle<Result<Bytes, Error>> {
	let mut request = clients.request("test", Bytes::from(payload)).unwrap();
	tokio::spawn(async move { request.wait().await })
}

#[tokio::test]
async fn a_request_is_answered() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "hello");
	let incoming = server.next().await.unwrap();
	assert_eq!(incoming.request, Bytes::from("hello"));
	incoming.reply.send(&Bytes::from("world")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("world"));
}

#[tokio::test]
async fn a_request_sent_before_the_server_starts_is_recovered_when_the_server_announces() {
	// Use a retry interval far longer than the test, so that only the announcement can deliver the request in time.
	let options = Options {
		attempt_ttl: Duration::from_secs(60),
		heartbeat_interval: Duration::from_secs(30),
		idle_timeout: Duration::from_secs(60),
		recovery_timeout: Duration::from_secs(60),
		request_timeout: Duration::from_secs(60),
		retry_interval: Duration::from_secs(30),
	};
	let messenger = TestMessenger::default();
	let clients = clients(&messenger, options);
	let request = spawn_request(&clients, "early");
	tokio::time::sleep(Duration::from_millis(50)).await;
	let mut server = server(&messenger, options).await;
	let incoming = tokio::time::timeout(Duration::from_secs(5), server.next())
		.await
		.expect("the announcement must recover the request")
		.unwrap();
	incoming.reply.send(&Bytes::from("late")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("late"));
}

#[tokio::test]
async fn heartbeats_keep_a_request_alive_while_the_server_takes_longer_than_the_attempt_ttl() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "slow");
	let incoming = server.next().await.unwrap();
	incoming.reply.acknowledge().unwrap();
	tokio::time::sleep(options().attempt_ttl * 3).await;
	assert!(!incoming.reply.is_cancelled());
	incoming.reply.send(&Bytes::from("done")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("done"));
}

#[tokio::test]
async fn each_request_is_delivered_once_and_answered_under_loss_duplication_and_reordering() {
	let messenger = TestMessenger::with_faults(3, 4, 5);
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let payloads = (0..16).map(|index| index.to_string()).collect::<Vec<_>>();
	let requests = payloads
		.iter()
		.map(|payload| {
			let mut request = clients
				.request("test", Bytes::from(payload.clone()))
				.unwrap();
			tokio::spawn(async move { request.wait().await })
		})
		.collect::<Vec<_>>();
	let answer = tokio::spawn(async move {
		let mut delivered = Vec::new();
		while delivered.len() < 16 {
			let incoming = server.next().await.unwrap();
			delivered.push(incoming.request.clone());
			incoming.reply.send(&incoming.request).unwrap();
		}
		(server, delivered)
	});
	for (payload, request) in payloads.iter().zip(requests) {
		assert_eq!(
			request.await.unwrap().unwrap(),
			Bytes::from(payload.clone())
		);
	}
	let (_server, mut delivered) = answer.await.unwrap();
	delivered.sort();
	delivered.dedup();
	assert_eq!(
		delivered.len(),
		16,
		"each request must be delivered exactly once"
	);
}

#[tokio::test]
async fn a_request_is_resent_until_the_server_acknowledges_it() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let record = Arc::new(Record::default());
	let clients = Arc::new(Clients::new(messenger.clone(), options(), record.clone()).unwrap());
	let request = spawn_request(&clients, "retained");
	let incoming = server.next().await.unwrap();

	// The server holds the acknowledgment, so the client resends.
	tokio::time::sleep(options().retry_interval * 5).await;
	assert!(record.count("request_sent") > 1);
	assert_eq!(record.count("acknowledged"), 0);

	// Once acknowledged, the client stops resending.
	incoming.reply.acknowledge().unwrap();
	tokio::time::timeout(Duration::from_secs(5), async {
		while record.count("acknowledged") == 0 {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await
	.unwrap();
	let sent = record.count("request_sent");
	tokio::time::sleep(options().retry_interval * 5).await;
	assert_eq!(record.count("request_sent"), sent);
	incoming.reply.send(&Bytes::from("done")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("done"));
}

#[tokio::test]
async fn dropping_a_request_cancels_it_at_the_server() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "drop");
	let incoming = server.next().await.unwrap();
	request.abort();
	tokio::time::timeout(Duration::from_secs(5), incoming.reply.cancelled())
		.await
		.expect("the request must be cancelled");
}

#[tokio::test]
async fn the_server_cancels_the_requests_of_an_attempt_whose_heartbeats_stop() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let _request = spawn_request(&clients, "abandoned");
	let incoming = server.next().await.unwrap();
	clients.stop();
	tokio::time::timeout(options().attempt_ttl * 5, incoming.reply.cancelled())
		.await
		.expect("the attempt must expire");
}

#[tokio::test]
async fn a_request_times_out_without_an_server() {
	let messenger = TestMessenger::default();
	let clients = clients(&messenger, options());
	let result = spawn_request(&clients, "nobody").await.unwrap();
	assert!(matches!(result, Err(Error::Timeout)));
}

#[tokio::test]
async fn a_request_is_recovered_by_a_new_server_after_the_first_stops() {
	let messenger = TestMessenger::default();
	let mut first = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "moved");
	first.next().await.unwrap();
	drop(first);
	let mut second = server(&messenger, options()).await;
	let incoming = second.next().await.unwrap();
	incoming.reply.send(&Bytes::from("second")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("second"));
}

#[tokio::test]
async fn stopping_the_clients_fails_their_pending_requests() {
	let messenger = TestMessenger::default();
	let _server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "stop");
	tokio::time::sleep(Duration::from_millis(50)).await;
	clients.stop();
	let result = request.await.unwrap();
	assert!(matches!(result, Err(Error::Stopped)));
}

#[tokio::test]
async fn requests_to_a_subject_share_one_client_until_it_is_idle() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let record = Arc::new(Record::default());
	let clients = Arc::new(Clients::new(messenger.clone(), options(), record.clone()).unwrap());
	let answer = tokio::spawn(async move {
		while let Some(incoming) = server.next().await {
			incoming.reply.send(&incoming.request).unwrap();
		}
	});

	// Concurrent requests share one client.
	let first = spawn_request(&clients, "first");
	let second = spawn_request(&clients, "second");
	first.await.unwrap().unwrap();
	second.await.unwrap().unwrap();
	assert_eq!(record.count("started"), 1);

	// A request after the client has been idle starts a new one.
	tokio::time::sleep(options().idle_timeout * 3).await;
	spawn_request(&clients, "third").await.unwrap().unwrap();
	assert_eq!(record.count("started"), 2);
	answer.abort();
}

#[tokio::test]
async fn the_server_status_counts_unacknowledged_responses_until_they_are_acknowledged() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "status");
	let incoming = server.next().await.unwrap();
	assert_eq!(server.status().attempts, 1);
	incoming.reply.send(&Bytes::from("ok")).unwrap();
	request.await.unwrap().unwrap();
	tokio::time::timeout(Duration::from_secs(5), async {
		while server.status().unacknowledged != 0 {
			server.changed().await.unwrap();
		}
	})
	.await
	.expect("the response must be acknowledged");
}

#[tokio::test]
async fn a_stray_cancellation_does_not_affect_a_request() {
	let messenger = TestMessenger::default();
	let mut server = server(&messenger, options()).await;
	let clients = clients(&messenger, options());
	let request = spawn_request(&clients, "mine");
	let incoming = server.next().await.unwrap();
	let message = Message::Cancel {
		attempt: "unknown".to_owned(),
		id: "unknown".to_owned(),
	};
	messenger
		.publish(server_subject("test"), message)
		.await
		.unwrap();
	incoming.reply.send(&Bytes::from("ok")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("ok"));
}

#[tokio::test]
async fn invalid_options_are_rejected() {
	let messenger = TestMessenger::default();
	let options = Options {
		retry_interval: Duration::ZERO,
		..options()
	};
	let result = serve::<Test, _>(&messenger, "test", options, Arc::new(Ignore)).await;
	assert!(matches!(result, Err(Error::Options(_))));
}

#[test]
fn messages_roundtrip() {
	let string = || "value".to_owned();
	for message in [
		Message::Ack {
			attempt: string(),
			id: string(),
		},
		Message::Announce,
		Message::Cancel {
			attempt: string(),
			id: string(),
		},
		Message::Heartbeat {
			client: string(),
			id: string(),
		},
		Message::HeartbeatResponse {
			attempt: string(),
			id: string(),
			ttl: Duration::from_millis(1234),
		},
		Message::Request {
			attempt: string(),
			client: string(),
			id: String::new(),
			request: Bytes::from("request"),
		},
		Message::RequestAck {
			attempt: string(),
			id: string(),
		},
		Message::Response {
			attempt: string(),
			id: string(),
			response: Bytes::new(),
		},
	] {
		let bytes = message.serialize().unwrap();
		assert_eq!(Message::deserialize(bytes).unwrap(), message);
	}
}
