use {
	tangram_client::prelude::*,
	tangram_tests_util::{Config, server::Server},
};

#[tokio::test]
async fn a_server_serves_requests_after_a_restart() -> tg::Result<()> {
	let mut server = Server::start(Config::new()).await?;
	server.client()?.health(tg::health::Arg::default()).await?;
	server.restart().await?;
	server.client()?.health(tg::health::Arg::default()).await?;
	server.stop().await?;

	Ok(())
}
