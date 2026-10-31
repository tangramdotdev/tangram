use {
	super::*,
	bytes::Bytes,
	std::{sync::Mutex, time::Duration},
	tangram_messenger::service::Ignore,
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
		async {}.boxed()
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

type Messenger = tangram_messenger_memory::Messenger;

fn messenger() -> Messenger {
	let config = tangram_messenger_memory::Config {
		subscription_capacity: 1024,
	};
	Messenger::new(&config)
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

fn client(
	messenger: &Messenger,
	record: Arc<Record>,
) -> Arc<Client<String, Bytes, Bytes, Messenger>> {
	let client = Client::new(messenger.clone(), "test", options(), record).unwrap();
	Arc::new(client)
}

fn server(messenger: &Messenger, key: &str) -> Server<Bytes, Bytes> {
	Server::start(messenger, "test", &key, options(), Arc::new(Ignore))
}

/// Answer every request with the request.
fn spawn_echo(server: &Server<Bytes, Bytes>) -> tokio::task::JoinHandle<()> {
	let server = server.clone();
	tokio::spawn(async move {
		while let Some((request, reply)) = server.next().await {
			reply.send(&request);
		}
	})
}

#[tokio::test]
async fn a_request_to_a_local_server_skips_the_messenger() {
	let messenger = messenger();
	let record = Arc::new(Record::default());
	let client = client(&messenger, record.clone());
	let server = server(&messenger, "key");
	let echo = spawn_echo(&server);
	let response = client
		.request(&"key".to_owned(), Some(&server), Bytes::from("hello"))
		.await
		.unwrap();
	assert_eq!(response, Bytes::from("hello"));
	assert_eq!(record.count("request_sent"), 1);
	assert_eq!(record.count("responded"), 1);
	assert_eq!(record.count("started"), 0);
	echo.abort();
}

#[tokio::test]
async fn a_request_without_a_local_server_is_answered_over_the_messenger() {
	let messenger = messenger();
	let record = Arc::new(Record::default());
	let client = client(&messenger, record.clone());
	let server = server(&messenger, "key");
	let echo = spawn_echo(&server);
	let response = client
		.request(&"key".to_owned(), None, Bytes::from("hello"))
		.await
		.unwrap();
	assert_eq!(response, Bytes::from("hello"));
	assert_eq!(record.count("started"), 1);
	echo.abort();
}

#[tokio::test]
async fn an_acknowledged_local_request_waits_for_its_response() {
	let messenger = messenger();
	let record = Arc::new(Record::default());
	let client = client(&messenger, record.clone());
	let server = server(&messenger, "key");
	let request = tokio::spawn({
		let client = client.clone();
		let server = server.clone();
		async move {
			client
				.request(&"key".to_owned(), Some(&server), Bytes::from("hello"))
				.await
		}
	});
	let (_, reply) = server.next().await.unwrap();
	reply.acknowledge();
	tokio::time::timeout(Duration::from_secs(5), async {
		while record.count("acknowledged") == 0 {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await
	.unwrap();
	assert!(!reply.is_cancelled());
	reply.send(&Bytes::from("world"));
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("world"));
}

#[tokio::test]
async fn a_request_to_a_stopped_local_server_falls_back_to_the_messenger() {
	let messenger = messenger();
	let record = Arc::new(Record::default());
	let client = client(&messenger, record.clone());
	let stopped = server(&messenger, "key");
	stopped.stop();
	let server = server(&messenger, "key");
	let echo = spawn_echo(&server);
	let response = client
		.request(&"key".to_owned(), Some(&stopped), Bytes::from("hello"))
		.await
		.unwrap();
	assert_eq!(response, Bytes::from("hello"));
	assert_eq!(record.count("started"), 1);
	echo.abort();
}

#[tokio::test]
async fn a_local_request_falls_back_to_the_messenger_when_the_local_server_stops_before_answering()
{
	let messenger = messenger();
	let client = client(&messenger, Arc::new(Record::default()));
	let local = server(&messenger, "key");
	let request = tokio::spawn({
		let client = client.clone();
		let local = local.clone();
		async move {
			client
				.request(&"key".to_owned(), Some(&local), Bytes::from("hello"))
				.await
		}
	});
	let (_, reply) = local.next().await.unwrap();
	local.stop();
	drop(reply);
	let server = server(&messenger, "key");
	let echo = spawn_echo(&server);
	assert_eq!(request.await.unwrap().unwrap(), Bytes::from("hello"));
	echo.abort();
}

#[tokio::test]
async fn a_stopped_server_yields_no_more_requests_and_finishes() {
	let messenger = messenger();
	let server = server(&messenger, "key");
	server.stop();
	assert!(server.is_stopped());
	assert!(server.next().await.is_none());
	tokio::time::timeout(Duration::from_secs(5), server.wait())
		.await
		.expect("the server must finish");
}
