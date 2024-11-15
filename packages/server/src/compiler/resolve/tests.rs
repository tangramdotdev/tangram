use crate::{compiler::Compiler, Config, Server};
use futures::{Future, FutureExt};
use pretty_assertions::assert_eq;
use std::{panic::AssertUnwindSafe, path::PathBuf};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn path_dependency_path() -> tg::Result<()> {
	test_path(
		tg::module::Kind::Ts,
		temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
			"foo.tg.ts" => "",
		},
		"",
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
		|_, path, module| async move {
			let right = tg::Module {
				kind: tg::module::Kind::Ts,
				referent: tg::Referent {
					item: tg::module::Item::Path(path),
					path: Some("".into()),
					subpath: Some("foo.tg.ts".into()),
					tag: None,
				},
			};
			assert_eq!(module, right);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn path_dependency_object() -> tg::Result<()> {
	test_object(
		tg::module::Kind::Ts,
		temp::directory! {
			"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
			"foo.tg.ts" => "",
		},
		"",
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
		|server, artifact, module| async move {
			let right = tg::Module {
				kind: tg::module::Kind::Ts,
				referent: tg::Referent {
					item: tg::module::Item::Object(artifact.id(&server).await?.into()),
					path: Some("".into()),
					subpath: Some("foo.tg.ts".into()),
					tag: None,
				},
			};
			assert_eq!(module, right);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_path_dependency_path() -> tg::Result<()> {
	test_path(
		tg::module::Kind::Ts,
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			}
		},
		"foo",
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("../bar", None).unwrap(),
		|_, path, module| async move {
			let path = tokio::fs::canonicalize(path.join("bar")).await.unwrap();
			let right = tg::Module {
				kind: tg::module::Kind::Ts,
				referent: tg::Referent {
					item: tg::module::Item::Path(path),
					path: Some("bar".into()),
					subpath: Some("tangram.ts".into()),
					tag: None,
				},
			};
			assert_eq!(module, right);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

#[tokio::test]
async fn package_path_dependency_object() -> tg::Result<()> {
	test_object(
		tg::module::Kind::Ts,
		temp::directory! {
			"foo" => temp::directory! {
				"tangram.ts" => r#"import * as bar from "../bar""#,
			},
			"bar" => temp::directory! {
				"tangram.ts" => "",
			}
		},
		"foo",
		"tangram.ts",
		tg::Import::with_specifier_and_attributes("../bar", None).unwrap(),
		|server, artifact, module| async move {
			let artifact = artifact
				.unwrap_directory()
				.get(&server, "bar")
				.await
				.unwrap();
			let object = artifact.id(&server).await?.into();
			let right = tg::Module {
				kind: tg::module::Kind::Ts,
				referent: tg::Referent {
					item: tg::module::Item::Object(object),
					path: Some("bar".into()),
					subpath: Some("tangram.ts".into()),
					tag: None,
				},
			};
			assert_eq!(module, right);
			Ok::<_, tg::Error>(())
		},
	)
	.await
}

async fn test_path<F, Fut>(
	kind: tg::module::Kind,
	artifact: temp::Artifact,
	path: &str,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, PathBuf, tg::Module) -> Fut,
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
		let referrer = tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Path(temp.path().join(path)),
				path: Some(path.into()),
				subpath: Some(PathBuf::from(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await?;
		(assertions)(server.clone(), temp.path().to_owned(), module).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}

async fn test_object<F, Fut>(
	kind: tg::module::Kind,
	artifact: temp::Artifact,
	path: &str,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) -> tg::Result<()>
where
	F: FnOnce(Server, tg::Artifact, tg::Module) -> Fut,
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
		let artifact = tg::Artifact::check_in(
			&server,
			tg::artifact::checkin::Arg {
				destructive: false,
				deterministic: false,
				ignore: true,
				locked: false,
				path: directory.path().to_owned(),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;
		let item = artifact
			.clone()
			.unwrap_directory()
			.get(&server, path)
			.await?
			.id(&server)
			.await?
			.into();
		let referrer = tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Object(item),
				path: Some(path.into()),
				subpath: Some(PathBuf::from(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await?;
		(assertions)(server.clone(), artifact, module).await?;
		Ok::<_, tg::Error>(())
	})
	.catch_unwind()
	.await;
	server.stop();
	server.wait().await;
	result.unwrap()
}
