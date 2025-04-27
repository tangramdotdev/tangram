use insta::assert_json_snapshot;
use tangram_cli::{assert_success, test::test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

/// Test formatting a package.
#[tokio::test]
async fn format() {
	test(TG, async move |context| {
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
		assert_success!(output);

		// Snapshot the package.
		let package = temp::Artifact::with_path(temp.path()).await.unwrap();
		assert_json_snapshot!(package, @r#"
		{
		  "kind": "directory",
		  "entries": {
		    ".tangramignore": {
		      "kind": "file",
		      "contents": "foo"
		    },
		    "bar": {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": {
		          "kind": "file",
		          "contents": "export default \"formatted\";\n"
		        }
		      }
		    },
		    "bar.tg.ts": {
		      "kind": "file",
		      "contents": "export default \"formatted\";\n"
		    },
		    "foo": {
		      "kind": "directory",
		      "entries": {
		        "tangram.ts": {
		          "kind": "file",
		          "contents": "export default   \"not formatted\""
		        }
		      }
		    },
		    "foo.ts": {
		      "kind": "file",
		      "contents": "export default   \"not formatted\""
		    },
		    "tangram.ts": {
		      "kind": "file",
		      "contents": "export default \"formatted\";\n"
		    }
		  }
		}
		"#);
	})
	.await;
}
