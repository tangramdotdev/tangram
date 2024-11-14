use crate::{
	compiler::Compiler, config::Build, config::BuildHeartbeatMonitor, config::Config, Server,
};
use futures::FutureExt as _;
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn test_uri_for_module_hello() -> tg::Result<()> {
	let uri_suffix = "foo/bar.tg.ts";
	let module_path = "".into();
	let module_subpath = uri_suffix.into();
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"bar.tg.ts" => "",
		},
		"tangram.ts" => r#"import * as foo from "./foo""#,
	};

	test_equivalence(module_path, module_subpath, uri_suffix, artifact).await
}

#[tokio::test]
async fn test_uri_for_module_subpath() -> tg::Result<()> {
	let uri_suffix = "foo/bar/tangram.ts";
	let module_path = "foo/bar".into();
	let module_subpath = "tangram.ts".into();
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"bar" => temp::directory! {
				"tangram.ts" => r#"export default tg.target(() => "Hello, World!");"#,
			},
		},
		"tangram.ts" => r#"import * as foo from "./foo""#,
	};

	test_equivalence(module_path, module_subpath, uri_suffix, artifact).await
}

async fn test_equivalence(
	module_path: PathBuf,
	module_subpath: PathBuf,
	uri_suffix: &str,
	artifact: temp::Artifact,
) -> tg::Result<()> {
	let server_temp = Temp::new();
	let mut options = Config::with_path(server_temp.path().to_owned());
	options.build = Some(Build::default());
	options.build_heartbeat_monitor = Some(BuildHeartbeatMonitor::default());
	let server = Server::start(options).await?;
	let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
	let result = AssertUnwindSafe(async {
		let temp = Temp::new_persistent();
		artifact.to_path(temp.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = temp.path().display(), "failed to write the artifact"),
		)?;

		// Needed to generate tangram.lock
		tg::Artifact::check_in(
			&server,
			tg::artifact::checkin::Arg {
				destructive: false,
				deterministic: false,
				ignore: true,
				locked: false,
				path: temp.path().to_owned(),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;

		let mut item_path = temp.path().to_path_buf();
		item_path.push(module_path);
		let module = tg::Module {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::Item::Path(item_path),
				path: None,
				subpath: Some(module_subpath),
				tag: None,
			},
		};
		let converted_uri = compiler.lsp_uri_for_module(&module).await?;
		let uri = format!("file://{}/{}", temp.path().display(), uri_suffix)
			.parse()
			.unwrap();
		let converted_module = compiler.module_for_lsp_uri(&uri).await?;

		assert_eq!(uri, converted_uri);
		assert_eq!(module, converted_module);

		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	if result.as_ref().is_ok_and(Result::is_ok) {
		server_temp.remove().await.ok();
	}
	result.unwrap()
}
