use crate::{Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tangram_client as tg;
use tangram_temp::Temp;

#[tokio::test]
async fn test_simple() -> tg::Result<()> {
	test(
		tg::directory! {
			"tangram.ts" => tg::file!("export default 5;"),
		}
		.into(),
		None,
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "exports": {
       "default": {
         "variable": [
           {
             "location": {
               "module": {
                 "kind": "ts",
                 "referent": {
                   "item": "dir_01nxsbe5s2rwwszgxrq0x3jp3r94fzggmkk4t2y82zhrd4dbf2403g",
                   "subpath": "tangram.ts"
                 }
               },
               "range": {
                 "start": {
                   "line": 0,
                   "character": 0
                 },
                 "end": {
                   "line": 0,
                   "character": 17
                 }
               }
             },
             "type": {
               "kind": "literal",
               "value": {
                 "kind": "number",
                 "value": 5
               }
             },
             "comment": {
               "text": "",
               "tags": []
             }
           }
         ]
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn test_simple_with_subpath() -> tg::Result<()> {
	test(
		tg::directory! {
			"tangram.ts" => tg::file!("export default 5;"),
		}
		.into(),
		Some(PathBuf::from("tangram.ts")),
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "exports": {
       "default": {
         "variable": [
           {
             "location": {
               "module": {
                 "kind": "ts",
                 "referent": {
                   "item": "dir_01nxsbe5s2rwwszgxrq0x3jp3r94fzggmkk4t2y82zhrd4dbf2403g",
                   "subpath": "tangram.ts"
                 }
               },
               "range": {
                 "start": {
                   "line": 0,
                   "character": 0
                 },
                 "end": {
                   "line": 0,
                   "character": 17
                 }
               }
             },
             "type": {
               "kind": "literal",
               "value": {
                 "kind": "number",
                 "value": 5
               }
             },
             "comment": {
               "text": "",
               "tags": []
             }
           }
         ]
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn test_module() -> tg::Result<()> {
	test(
		tg::directory! {
		"tangram.ts" => tg::file!(""),
		"hello" => tg::directory!(
			"test.ts" => tg::file!("export default 5;")
		)
			}
		.into(),
		Some(PathBuf::from("hello/test.ts")),
		|_, output| async move {
			assert_json_snapshot!(output, @r#"
   {
     "exports": {
       "default": {
         "variable": [
           {
             "location": {
               "module": {
                 "kind": "ts",
                 "referent": {
                   "item": "dir_01m5hagkb7jk1c1wcq5sva7h4kx0tnd2zyhcw6mgsm811f3et6yscg",
                   "subpath": "hello/test.ts"
                 }
               },
               "range": {
                 "start": {
                   "line": 0,
                   "character": 0
                 },
                 "end": {
                   "line": 0,
                   "character": 17
                 }
               }
             },
             "type": {
               "kind": "literal",
               "value": {
                 "kind": "number",
                 "value": 5
               }
             },
             "comment": {
               "text": "",
               "tags": []
             }
           }
         ]
       }
     }
   }
   "#);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(
	artifact: tg::Artifact,
	subpath: Option<PathBuf>,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, serde_json::Value) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let server_temp = Temp::new();
	let server_config = Config::with_path(server_temp.path().to_owned());
	let server = Server::start(server_config).await?;
	let result = AssertUnwindSafe(async {
		let arg = tg::package::document::Arg {
			package: artifact.id(&server).await?.try_unwrap_directory().unwrap(),
			subpath,
			remote: None,
		};
		let output = server.document_package(arg).await?;
		(assertions)(server.clone(), output).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
