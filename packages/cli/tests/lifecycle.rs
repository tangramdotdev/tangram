use {
	tangram_client::prelude::*,
	tangram_tests_util::{Config, cli::Server},
};

#[tokio::test]
async fn a_server_serves_requests_after_a_crash() -> tg::Result<()> {
	let mut server = Server::start(env!("CARGO_BIN_EXE_tangram"), Config::new()).await?;
	server.client()?.health(tg::health::Arg::default()).await?;
	server.kill().await?;
	server.restart().await?;
	server.client()?.health(tg::health::Arg::default()).await?;
	server.stop().await?;

	Ok(())
}
