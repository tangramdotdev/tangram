use {
	crate::prelude::*,
	std::sync::{Arc, Mutex},
	tangram_http::{body::Ext as _, response::builder::Ext as _},
	tower::util::BoxCloneSyncService,
};

const ID: &str = "blb_010000000000000000000000000000000000000000000000000000";

#[tokio::test]
async fn buffered_writes_replay_after_a_connection_failure() -> tg::Result<()> {
	let bodies = Arc::new(Mutex::new(Vec::new()));
	let client = client(bodies.clone(), false)?;
	let output = client
		.write_bytes(tg::write::Arg::default(), "hello")
		.await?;
	assert_eq!(output.blob.node.to_string(), ID);
	assert_eq!(*bodies.lock().unwrap(), vec![b"hello".to_vec(); 2]);

	Ok(())
}

#[tokio::test]
async fn streaming_writes_do_not_replay() -> tg::Result<()> {
	let bodies = Arc::new(Mutex::new(Vec::new()));
	let client = client(bodies.clone(), false)?;
	client
		.write(tg::write::Arg::default(), &b"hello"[..])
		.await
		.unwrap_err();
	assert_eq!(*bodies.lock().unwrap(), vec![b"hello".to_vec()]);

	Ok(())
}

#[tokio::test]
async fn buffered_writes_do_not_retry_timeouts() -> tg::Result<()> {
	let bodies = Arc::new(Mutex::new(Vec::new()));
	let client = client(bodies.clone(), true)?;
	client
		.write_bytes(tg::write::Arg::default(), "hello")
		.await
		.unwrap_err();
	assert_eq!(*bodies.lock().unwrap(), vec![b"hello".to_vec()]);

	Ok(())
}

fn client(bodies: Arc<Mutex<Vec<Vec<u8>>>>, timeout: bool) -> tg::Result<tg::Client> {
	let arg = tg::Arg {
		url: Some("http://localhost".parse().unwrap()),
		..Default::default()
	};
	let mut client = tg::Client::new(arg)?;
	let service = tower::service_fn(move |request: http::Request<tangram_http::body::Boxed>| {
		let bodies = bodies.clone();
		async move {
			let body = request.into_body().collect().await.unwrap().to_bytes();
			let count = {
				let mut bodies = bodies.lock().unwrap();
				bodies.push(body.to_vec());
				bodies.len()
			};
			if timeout {
				return Err(crate::http::Error::Timeout(tg::error!(
					"the request timed out"
				)));
			}
			if count == 1 {
				return Err(crate::http::Error::Connection(tg::error!(
					"the connection closed"
				)));
			}
			let output = serde_json::json!({ "blob": ID });
			let response = http::Response::builder()
				.json(output)
				.unwrap()
				.unwrap()
				.map(tangram_http::body::Boxed::new);

			Ok(response)
		}
	});
	Arc::get_mut(&mut client.0).unwrap().service = BoxCloneSyncService::new(service);

	Ok(client)
}
