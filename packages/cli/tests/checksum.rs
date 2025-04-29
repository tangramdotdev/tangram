use insta::assert_snapshot;
use tangram_cli::test::{assert_failure, assert_success, test};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn download_checksum_none() {
	test(TG, async move |context| {
		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Download with checksum "none".
		let output = server
			.tg()
			.arg("download")
			.arg("https://example.com")
			.arg("--checksum")
			.arg("none")
			.output()
			.await
			.unwrap();
		assert_failure!(output);

		// Download with the correct checksum.
		let output = server
			.tg()
			.arg("download")
			.arg("https://example.com")
			.arg("--checksum")
			.arg("sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9")
			.output()
			.await
			.unwrap();
		assert_success!(output);
		let stdout = std::str::from_utf8(&output.stdout).unwrap();
		assert_snapshot!(stdout, @"blb_01fvba7brv0fdzcx0khtms8fm61wbeaf1a75f3td9c2mj6hrp9qr30");
	})
	.await;
}
