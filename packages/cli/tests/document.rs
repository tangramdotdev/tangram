use indoc::indoc;
use insta::assert_json_snapshot;
use tangram_cli::test::{assert_success, test};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	test(TG, async move |context| {
		let server = context.spawn_server().await.unwrap();

		let temp = Temp::new();
		let package = temp::directory! {
			"tangram.ts" => indoc!(r#"
				export default () => "Hello, World!";
			"#),
		};
		package.to_path(temp.as_ref()).await.unwrap();

		let output = server
			.tg()
			.arg("document")
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);

		let output = serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();

		assert_json_snapshot!(output, @r#"
		{
		  "exports": {
		    "default": {
		      "variable": [
		        {
		          "comment": {
		            "tags": [],
		            "text": ""
		          },
		          "location": {
		            "module": {
		              "kind": "ts",
		              "referent": {
		                "item": "dir_01stbe827c8yzmshnymra1aqmehbqtfayk32s5xktd7f80snb2jy2g",
		                "subpath": "tangram.ts"
		              }
		            },
		            "range": {
		              "end": {
		                "character": 37,
		                "line": 0
		              },
		              "start": {
		                "character": 0,
		                "line": 0
		              }
		            }
		          },
		          "type": {
		            "kind": "function",
		            "value": {
		              "signatures": [
		                {
		                  "location": {
		                    "module": {
		                      "kind": "ts",
		                      "referent": {
		                        "item": "dir_01stbe827c8yzmshnymra1aqmehbqtfayk32s5xktd7f80snb2jy2g",
		                        "subpath": "tangram.ts"
		                      }
		                    },
		                    "range": {
		                      "end": {
		                        "character": 36,
		                        "line": 0
		                      },
		                      "start": {
		                        "character": 15,
		                        "line": 0
		                      }
		                    }
		                  },
		                  "parameters": {},
		                  "return": {
		                    "kind": "keyword",
		                    "value": "string"
		                  },
		                  "typeParameters": {}
		                }
		              ]
		            }
		          }
		        }
		      ]
		    }
		  }
		}
		"#);
	})
	.await;
}
