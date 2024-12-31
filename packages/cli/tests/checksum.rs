use insta::assert_snapshot;
use tangram_cli::{assert_output_failure, assert_output_success, config::Config, test::test};
use tokio::io::AsyncWriteExt as _;

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test building a module without a package.
#[tokio::test]
async fn download_checksum_none() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Download with checksum None.
		let output = server
			.tg()
			.arg("blob")
			.arg("download")
			.arg("https://example.com")
			.arg("--checksum")
			.arg("none")
			.arg("--quiet")
			.output()
			.await
			.unwrap();
		assert_output_failure!(output);

		// Download with checksum set.
		let output = server
			.tg()
			.arg("blob")
			.arg("download")
			.arg("https://example.com")
			.arg("--checksum")
			.arg("sha256:ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9")
			.arg("--quiet")
			.output()
			.await
			.unwrap();
		assert_output_success!(output);
		assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"lef_01n8ycmc4q78cr0q5rfpqwxnggwe978p3hm2hwecv52g6wtbqmxwbg");
	})
	.await;
}
