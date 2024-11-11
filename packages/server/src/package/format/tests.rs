use crate::{Config, Server};
use futures::{Future, FutureExt as _};
use insta::assert_json_snapshot;
use std::panic::AssertUnwindSafe;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn test_all() -> tg::Result<()> {
	test(
		temp::directory! {
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
		},
		|_, artifact| async move {
			assert_json_snapshot!(artifact, @r#"
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
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test<F, Fut>(artifact: temp::Artifact, assertions: F) -> tg::Result<()>
where
	F: FnOnce(Server, temp::Artifact) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
		let arg = tg::package::format::Arg {
			path: directory.to_path_buf(),
		};
		server.format_package(arg).await?;
		let artifact = temp::Artifact::with_path(directory.as_ref()).await?;
		(assertions)(server.clone(), artifact).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
