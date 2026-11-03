use super::is_retryable;

#[tokio::test]
async fn remote_goaway() {
	for (reason, expected) in [
		(h2::Reason::NO_ERROR, true),
		(h2::Reason::INTERNAL_ERROR, false),
		(h2::Reason::PROTOCOL_ERROR, false),
	] {
		let error = receive_goaway(reason).await;
		assert_eq!(is_retryable(&error), expected);
	}
}

#[test]
fn bare_no_error() {
	let error = h2::Error::from(h2::Reason::NO_ERROR);
	assert!(!is_retryable(&error));
}

async fn receive_goaway(reason: h2::Reason) -> h2::Error {
	let (client, server) = tokio::io::duplex(1024);
	let client = async {
		let (sender, connection) = h2::client::handshake(client).await.unwrap();
		connection.await.ok();
		sender.ready().await.unwrap_err()
	};
	let server = async {
		let mut connection = h2::server::handshake(server).await.unwrap();
		connection.abrupt_shutdown(reason);
		let _ = connection.accept().await;
	};
	let (error, ()) = tokio::join!(client, server);

	error
}
