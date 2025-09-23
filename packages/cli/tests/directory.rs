use {
	indoc::indoc,
	insta::assert_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn get_symlink() {
	// Start the server.
	let server = Server::new(TG).await.unwrap();

	// Create a directory with a module.
	let temp = Temp::new();
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let directory = await tg.directory({
					"hello.txt": "Hello, World!",
					"link": tg.symlink("hello.txt"),
				});
				return directory.get("link");
			};
		"#),
	};
	directory.to_path(temp.as_ref()).await.unwrap();

	// Build the module.
	let output = server
		.tg()
		.arg("build")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Assert the output.
	assert_snapshot!(std::str::from_utf8(&output.stdout).unwrap(), @"fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00");
}

#[tokio::test]
async fn get_file_through_symlink() {
	// Start the server.
	let server = Server::new(TG).await.unwrap();

	// Create a directory with a module.
	let temp = Temp::new();
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default async () => {
				let directory = await tg.directory({
					"subdirectory": {
						"hello.txt": "Hello, World!",
					},
					"link": tg.symlink("subdirectory"),
				});
				return directory.get("link/hello.txt");
			};
		"#),
	};
	directory.to_path(temp.as_ref()).await.unwrap();

	// Build the module.
	let output = server
		.tg()
		.arg("build")
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);
	let stdout = std::str::from_utf8(&output.stdout).unwrap();
	assert_snapshot!(stdout, @"fil_01w439ptb1t3g6srv9h369xjwyqj7m17cfqqvnt7e2pdg8yhjy7h00");
}
