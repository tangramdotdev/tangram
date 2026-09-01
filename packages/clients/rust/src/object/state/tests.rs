use {
	super::*,
	futures::future,
	hyper::service::service_fn,
	hyper_util::rt::{TokioExecutor, TokioIo},
	std::{
		collections::BTreeMap,
		convert::Infallible,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	},
	tangram_http::body::Boxed,
};

#[tokio::test]
async fn concurrent_loads_share_an_in_flight_get() {
	// Create an object response.
	let directory = tg::Directory::with_entries(BTreeMap::new());
	let id = directory.id();
	let bytes = directory
		.state()
		.object()
		.unwrap()
		.to_data()
		.serialize()
		.unwrap();

	// Serve object GETs over an in-memory connection.
	let requests = Arc::new(AtomicUsize::new(0));
	let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
	let server = tokio::spawn({
		let id = id.clone();
		let requests = requests.clone();
		async move {
			let service = service_fn(move |request| {
				let bytes = bytes.clone();
				let id = id.clone();
				let requests = requests.clone();
				async move {
					assert_eq!(request.method(), http::Method::GET);
					assert_eq!(request.uri().path(), format!("/objects/{id}"));
					requests.fetch_add(1, Ordering::SeqCst);
					let response = http::Response::builder()
						.body(Boxed::with_bytes(bytes))
						.unwrap();

					Ok::<_, Infallible>(response)
				}
			});
			let executor = TokioExecutor::new();
			let stream = TokioIo::new(server_stream);
			hyper::server::conn::http2::Builder::new(executor)
				.serve_connection(stream, service)
				.await
				.unwrap();
		}
	});

	// Load the object concurrently through one state.
	let client = tg::Client::with_stream(tg::Arg::default(), client_stream)
		.await
		.unwrap();
	let state = State::with_id(id);
	let loads = (0..8).map(|_| state.load_with_handle(&client));
	let objects = future::try_join_all(loads).await.unwrap();
	assert_eq!(objects.len(), 8);

	// Stop the server and check the request count.
	let requests = requests.load(Ordering::SeqCst);
	drop(client);
	server.await.unwrap();

	assert_eq!(requests, 1);
}
