use {
	indoc::indoc,
	insta::assert_json_snapshot,
	tangram_cli_test::{Server, assert_success},
	tangram_temp::{self as temp, Temp},
};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn hello_world() {
	let server = Server::new(TG).await.unwrap();

	let temp = Temp::new();
	let directory = temp::directory! {
		"tangram.ts" => indoc!(r#"
			export default () => "Hello, World!";
		"#),
	};
	directory.to_path(temp.as_ref()).await.unwrap();

	let output = server
		.tg()
		.arg("document")
		.current_dir(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	let stdout = serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
	assert_json_snapshot!(stdout, @r#"
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
	                "item": "fil_01g8rfpv95s4g7pmen8n11t9f6st5xpdnm38v286tva106f5qeyfvg",
	                "options": {
	                  "path": "tangram.ts"
	                }
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
	                        "item": "fil_01g8rfpv95s4g7pmen8n11t9f6st5xpdnm38v286tva106f5qeyfvg",
	                        "options": {
	                          "path": "tangram.ts"
	                        }
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
}
