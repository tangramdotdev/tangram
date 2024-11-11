use crate::{compiler::Compiler, Config, Server};
use futures::{Future, FutureExt};
use std::{
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn path_dependency_path() -> tg::Result<()> {
	test_path(
		temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
			"foo.tg.ts" => "",
		},
		tg::module::Kind::Ts,
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
		|_, module| async move {
			assert_eq!(module.kind, tg::module::Kind::Ts);
			let tg::module::Item::Path(_path) = &module.referent.item else {
				return Err(tg::error!("expected a path item"));
			};
			let Some(subpath) = &module.referent.subpath else {
				return Err(tg::error!("expected a subpath"));
			};
			let expected: &Path = "./foo.tg.ts".as_ref();
			assert_eq!(subpath, expected);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency_object() -> tg::Result<()> {
	test_object(
		temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
			"foo.tg.ts" => "",
		},
		tg::module::Kind::Ts,
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
		|_, module| async move {
			assert_eq!(module.kind, tg::module::Kind::Ts);
			let tg::module::Item::Object(_object) = &module.referent.item else {
				return Err(tg::error!("expected a path item"));
			};
			let Some(subpath) = &module.referent.subpath else {
				return Err(tg::error!("expected a subpath"));
			};
			let expected: &Path = "./foo.tg.ts".as_ref();
			assert_eq!(subpath, expected);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_path_dependency_path() -> tg::Result<()> {
	test_path(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			}
		},
		tg::module::Kind::Ts,
		"foo/tangram.ts",
		tg::Import::with_specifier_and_attributes("../bar", None).unwrap(),
		|_, module| async move {
			assert_eq!(module.kind, tg::module::Kind::Ts);
			let tg::module::Item::Path(_path) = &module.referent.item else {
				return Err(tg::error!("expected a path item"));
			};
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_path_dependency_object() -> tg::Result<()> {
	test_object(
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			}
		},
		tg::module::Kind::Ts,
		"foo/tangram.ts",
		tg::Import::with_specifier_and_attributes("../bar", None).unwrap(),
		|_, module| async move {
			assert_eq!(module.kind, tg::module::Kind::Ts);
			let tg::module::Item::Object(_object) = &module.referent.item else {
				return Err(tg::error!("expected a path item"));
			};
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test_path<F, Fut>(
	artifact: temp::Artifact,
	kind: tg::module::Kind,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Module) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
	let result = AssertUnwindSafe(async {
		let directory = Temp::new();
		artifact.to_path(directory.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
		)?;
		tg::Artifact::check_in(
			&server,
			tg::artifact::checkin::Arg {
				destructive: false,
				deterministic: false,
				ignore: true,
				locked: false,
				path: directory.path().to_owned().join(subpath),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;
		let referrer = tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Path(directory.path().to_owned()),
				subpath: Some(PathBuf::from(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await?;
		(assertions)(server.clone(), module).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}

async fn test_object<F, Fut>(
	artifact: temp::Artifact,
	kind: tg::module::Kind,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Module) -> Fut,
	Fut: Future<Output = tg::Result<()>>,
{
	let temp = Temp::new();
	let options = Config::with_path(temp.path().to_owned());
	let server = Server::start(options).await?;
	let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
	let result = AssertUnwindSafe(async {
		let temp = Temp::new();
		artifact.to_path(temp.as_ref()).await.map_err(
			|source| tg::error!(!source, %path = temp.path().display(), "failed to write the artifact"),
		)?;
		let artifact = tg::Artifact::check_in(
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
		let referrer = tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Object(artifact.id(&server).await?.into()),
				subpath: Some(PathBuf::from(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await?;
		(assertions)(server.clone(), module).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
