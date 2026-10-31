use {
	super::*,
	crate::{Messenger as _, test::TestMessenger},
	std::sync::Mutex,
};

/// An observer that records the names of the events.
#[derive(Default)]
struct Record {
	events: Mutex<Vec<&'static str>>,
}

impl Observer<Bytes> for Record {
	fn observe<'a>(&'a self, event: Event<'a, Bytes>) -> BoxFuture<'a, ()> {
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

async fn server(messenger: &TestMessenger, options: Options) -> Server<Bytes, Bytes> {
	Server::start(messenger, "test", options, Arc::new(Ignore))
		.await
		.unwrap()
}

fn client(messenger: &TestMessenger, options: Options) -> Arc<Client<Bytes, Bytes, TestMessenger>> {
	Arc::new(Client::new(messenger.clone(), "test.service", options, Arc::new(Ignore)).unwrap())
}

fn spawn_request(
	client: &Arc<Client<Bytes, Bytes, TestMessenger>>,
	payload: &'static str,
) -> tokio::task::JoinHandle<Result<Bytes, Error>> {
	let client = client.clone();
	tokio::spawn(async move { client.request("test", Bytes::from(payload)).await })
}

async fn cancelled(reply: &Reply<Bytes>) {
	while !reply.is_cancelled() {
		tokio::time::sleep(Duration::from_millis(1)).await;
	}
}

#[tokio::test]
async fn a_request_is_answered() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "hello");
	let (payload, reply) = server.next().await.unwrap();
	assert_eq!(payload, Bytes::from("hello"));
	reply.send(&Bytes::from("world")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("world"));
}

#[tokio::test]
async fn a_request_is_answered_without_waiting_for_a_heartbeat() {
	// Use a heartbeat interval far longer than the test, so that the request must establish its attempt itself.
	let options = Options {
		heartbeat_interval: Duration::from_secs(60),
		..options()
	};
	let messenger = TestMessenger::default();
	let server = server(&messenger, options).await;
	let record = Arc::new(Record::default());
	let client =
		Arc::new(Client::new(messenger.clone(), "test.service", options, record.clone()).unwrap());
	let request = spawn_request(&client, "hello");
	let (_, reply) = server.next().await.unwrap();
	reply.acknowledge();
	reply.send(&Bytes::from("world")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("world"));
	assert_eq!(record.count("heartbeat_sent"), 0);
}

#[tokio::test]
async fn the_connections_of_a_client_share_one_response_subscription() {
	let messenger = TestMessenger::default();
	let mut answers = Vec::new();
	for subject in ["first", "second"] {
		let server =
			Server::<Bytes, Bytes>::start(&messenger, subject, options(), Arc::new(Ignore))
				.await
				.unwrap();
		answers.push(tokio::spawn(async move {
			while let Some((request, reply)) = server.next().await {
				reply.send(&request).unwrap();
			}
		}));
	}
	let client = client(&messenger, options());
	for subject in ["first", "second"] {
		let response = client.request(subject, Bytes::from(subject)).await.unwrap();
		assert_eq!(response, Bytes::from(subject));
	}
	assert_eq!(messenger.subscriptions("test.service."), 1);
	for answer in answers {
		answer.abort();
	}
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
	let client = client(&messenger, options);
	let request = spawn_request(&client, "early");
	tokio::time::sleep(Duration::from_millis(50)).await;
	let server = server(&messenger, options).await;
	let (_, reply) = tokio::time::timeout(Duration::from_secs(5), server.next())
		.await
		.expect("the announcement must recover the request")
		.unwrap();
	reply.send(&Bytes::from("late")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("late"));
}

#[tokio::test]
async fn heartbeats_keep_a_request_alive_while_the_server_takes_longer_than_the_attempt_ttl() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "slow");
	let (_, reply) = server.next().await.unwrap();
	reply.acknowledge();
	tokio::time::sleep(options().attempt_ttl * 3).await;
	assert!(!reply.is_cancelled());
	reply.send(&Bytes::from("done")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("done"));
}

#[tokio::test]
async fn each_request_is_delivered_once_and_answered_under_loss_duplication_and_reordering() {
	let messenger = TestMessenger::with_faults(3, 4, 5);
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let payloads = (0..16).map(|index| index.to_string()).collect::<Vec<_>>();
	let requests = payloads
		.iter()
		.map(|payload| {
			let client = client.clone();
			let payload = Bytes::from(payload.clone());
			tokio::spawn(async move { client.request("test", payload).await })
		})
		.collect::<Vec<_>>();
	let answer = tokio::spawn(async move {
		let mut delivered = Vec::new();
		while delivered.len() < 16 {
			let (payload, reply) = server.next().await.unwrap();
			delivered.push(payload.clone());
			reply.send(&payload).unwrap();
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
	let server = server(&messenger, options()).await;
	let record = Arc::new(Record::default());
	let client = Arc::new(
		Client::new(messenger.clone(), "test.service", options(), record.clone()).unwrap(),
	);
	let request = spawn_request(&client, "retained");
	let (_, reply) = server.next().await.unwrap();

	// The server holds the acknowledgment, so the client resends.
	tokio::time::sleep(options().retry_interval * 5).await;
	assert!(record.count("request_sent") > 1);
	assert_eq!(record.count("acknowledged"), 0);

	// Once acknowledged, the client stops resending.
	reply.acknowledge();
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
	reply.send(&Bytes::from("done")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("done"));
}

#[tokio::test]
async fn dropping_a_request_cancels_it_at_the_server() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "drop");
	let (_, reply) = server.next().await.unwrap();
	request.abort();
	tokio::time::timeout(Duration::from_secs(5), cancelled(&reply))
		.await
		.expect("the request must be cancelled");
}

#[tokio::test]
async fn the_server_cancels_the_requests_of_an_attempt_whose_heartbeats_stop() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let _request = spawn_request(&client, "abandoned");
	let (_, reply) = server.next().await.unwrap();
	client.stop();
	tokio::time::timeout(options().attempt_ttl * 5, cancelled(&reply))
		.await
		.expect("the attempt must expire");
}

#[tokio::test]
async fn a_request_times_out_without_an_server() {
	let messenger = TestMessenger::default();
	let client = client(&messenger, options());
	let result = spawn_request(&client, "nobody").await.unwrap();
	assert!(matches!(result, Err(Error::Timeout)));
}

#[tokio::test]
async fn a_request_is_recovered_by_a_new_server_after_the_first_stops() {
	let messenger = TestMessenger::default();
	let first = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "moved");
	first.next().await.unwrap();
	drop(first);
	let second = server(&messenger, options()).await;
	let (_, reply) = second.next().await.unwrap();
	reply.send(&Bytes::from("second")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("second"));
}

#[tokio::test]
async fn stopping_the_client_fails_its_pending_requests() {
	let messenger = TestMessenger::default();
	let _server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "stop");
	tokio::time::sleep(Duration::from_millis(50)).await;
	client.stop();
	let result = request.await.unwrap();
	assert!(matches!(result, Err(Error::Stopped)));
}

#[tokio::test]
async fn requests_to_a_subject_share_one_connection_until_it_is_idle() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let record = Arc::new(Record::default());
	let client = Arc::new(
		Client::new(messenger.clone(), "test.service", options(), record.clone()).unwrap(),
	);
	let answer = tokio::spawn(async move {
		while let Some((request, reply)) = server.next().await {
			reply.send(&request).unwrap();
		}
	});

	// Concurrent requests share one connection.
	let first = spawn_request(&client, "first");
	let second = spawn_request(&client, "second");
	first.await.unwrap().unwrap();
	second.await.unwrap().unwrap();
	assert_eq!(record.count("started"), 1);

	// A request after the connection has been idle starts a new one.
	tokio::time::sleep(options().idle_timeout * 3).await;
	spawn_request(&client, "third").await.unwrap().unwrap();
	assert_eq!(record.count("started"), 2);
	answer.abort();
}

#[tokio::test]
async fn the_server_status_counts_unacknowledged_responses_until_they_are_acknowledged() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "status");
	let (_, reply) = server.next().await.unwrap();
	assert_eq!(server.status().attempts, 1);
	reply.send(&Bytes::from("ok")).unwrap();
	request.await.unwrap().unwrap();
	tokio::time::timeout(Duration::from_secs(5), async {
		while server.status().unacknowledged != 0 {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await
	.expect("the response must be acknowledged");
}

#[tokio::test]
async fn a_stopped_server_finishes_once_its_responses_are_acknowledged() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "drain");
	let (_, reply) = server.next().await.unwrap();
	reply.send(&Bytes::from("ok")).unwrap();
	server.stop();
	tokio::time::timeout(options().attempt_ttl * 5, server.wait())
		.await
		.expect("the server must finish");
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("ok"));
	assert!(server.next().await.is_none());
}

#[tokio::test]
async fn a_stopped_server_does_not_deliver_new_requests() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	server.stop();
	server.wait().await;
	let client = client(&messenger, options());
	let result = spawn_request(&client, "late").await.unwrap();
	assert!(matches!(result, Err(Error::Timeout)));
	assert!(server.next().await.is_none());
}

#[tokio::test]
async fn a_stray_cancellation_does_not_affect_a_request() {
	let messenger = TestMessenger::default();
	let server = server(&messenger, options()).await;
	let client = client(&messenger, options());
	let request = spawn_request(&client, "mine");
	let (_, reply) = server.next().await.unwrap();
	let message = Message::Cancel {
		attempt: Some("unknown".to_owned()),
		client: "unknown".to_owned(),
		id: "unknown".to_owned(),
	};
	messenger
		.publish(server_subject("test"), message)
		.await
		.unwrap();
	reply.send(&Bytes::from("ok")).unwrap();
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("ok"));
}

#[tokio::test]
async fn invalid_options_are_rejected() {
	let messenger = TestMessenger::default();
	let options = Options {
		retry_interval: Duration::ZERO,
		..options()
	};
	let result = Server::<Bytes, Bytes>::start(&messenger, "test", options, Arc::new(Ignore)).await;
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
			attempt: Some(string()),
			client: string(),
			id: string(),
		},
		Message::Cancel {
			attempt: None,
			client: string(),
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
			attempt: Some(string()),
			client: string(),
			id: String::new(),
			request: Bytes::from("request"),
		},
		Message::Request {
			attempt: None,
			client: string(),
			id: string(),
			request: Bytes::new(),
		},
		Message::RequestAck {
			attempt: string(),
			id: string(),
			ttl: Duration::from_millis(1234),
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
