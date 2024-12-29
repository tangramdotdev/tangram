use insta::assert_json_snapshot;
use tangram_cli::{assert_output_success, test::test};
use tangram_temp::{self as temp, Temp};
use tokio::io::AsyncWriteExt as _;

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test formatting a package.
#[tokio::test]
async fn format() {
	test(TG, |context| async move {
		let mut context = context.lock().await;

		// Start the server.
		let server = context.spawn_server().await.unwrap();

		// Create a package.
		let temp = Temp::new();
		let package = temp::directory! {
			".tangramignore" => "foo",
			"foo.ts" => r#"export default   "not formatted""#,
			"bar.tg.ts" => r#"export default   "formatted""#,
			"foo" => temp::directory! {
				"tangram.ts" => r#"export default   "not formatted""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => r#"export default   "formatted""#,
			},
			"tangram.ts" => r#"export default   "formatted""#,
		};
		package.to_path(temp.as_ref()).await.unwrap();

		// Format the package.
		let output = server
			.tg()
			.arg("format")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_output_success!(output);

		// Snapshot the package.
		let package = temp::Artifact::with_path(temp.path()).await.unwrap();
		assert_json_snapshot!(package, @r#"
  {
    "kind": "directory",
    "entries": {
      ".tangramignore": {
        "kind": "file",
        "contents": "foo",
        "executable": false
      },
      "bar": {
        "kind": "directory",
        "entries": {
          "tangram.ts": {
            "kind": "file",
            "contents": "export default \"formatted\";\n",
            "executable": false
          }
        }
      },
      "bar.tg.ts": {
        "kind": "file",
        "contents": "export default \"formatted\";\n",
        "executable": false
      },
      "foo": {
        "kind": "directory",
        "entries": {
          "tangram.ts": {
            "kind": "file",
            "contents": "export default   \"not formatted\"",
            "executable": false
          }
        }
      },
      "foo.ts": {
        "kind": "file",
        "contents": "export default   \"not formatted\"",
        "executable": false
      },
      "tangram.ts": {
        "kind": "file",
        "contents": "export default \"formatted\";\n",
        "executable": false
      }
    }
  }
  "#);
	})
	.await;
}
