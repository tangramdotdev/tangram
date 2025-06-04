use crate::{Server, compiler::Compiler, test::test};
use futures::Future;
use pretty_assertions::assert_eq;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn path_dependency_path() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
		"foo.tg.ts" => "",
	};
	let kind = tg::module::Kind::Ts;
	let path = "";
	let subpath = "tangram.ts";
	let import = tg::module::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap();
	let assertions = |_, path: PathBuf, module| async move {
		let right = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::data::Item::Path(path.join("foo.tg.ts")),
				path: None,
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_path(artifact, kind, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn path_dependency_object() {
	let artifact = temp::directory! {
		"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
		"foo.tg.ts" => "",
	};
	let kind = tg::module::Kind::Ts;
	let path = "";
	let subpath = "tangram.ts";
	let import = tg::module::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap();
	let assertions = |server, artifact: tg::Artifact, module| async move {
		let item = artifact
			.unwrap_directory_ref()
			.get(&server, "foo.tg.ts")
			.await
			.unwrap();
		let right = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::data::Item::Object(item.id().into()),
				path: Some("foo.tg.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_object(artifact, kind, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn package_path_dependency_path() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar""#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	};
	let kind = tg::module::Kind::Ts;
	let path = "foo";
	let subpath = "tangram.ts";
	let import = tg::module::Import::with_specifier_and_attributes("../bar", None).unwrap();
	let assertions = |_, path: PathBuf, module| async move {
		let path = tokio::fs::canonicalize(path.join("bar")).await.unwrap();
		let right = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::data::Item::Path(path.join("tangram.ts")),
				path: None,
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_path(artifact, kind, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn package_path_dependency_object() {
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar""#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	};
	let kind = tg::module::Kind::Ts;
	let path = "foo";
	let subpath = "tangram.ts";
	let import = tg::module::Import::with_specifier_and_attributes("../bar", None).unwrap();
	let assertions = |_server, _artifact, module: tg::module::Data| async move {
		let right = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: module.referent.item.clone(),
				path: Some("foo/../bar/tangram.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_object(artifact, kind, path, subpath, import, assertions).await;
}

async fn test_path<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send,
	kind: tg::module::Kind,
	path: &str,
	subpath: &str,
	import: tg::module::Import,
	assertions: F,
) where
	F: FnOnce(Server, PathBuf, tg::module::Data) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send + 'static,
{
	test(async move |context| {
		let server = context.start_server().await;
		let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
		let temp = Temp::new();
		let artifact = artifact.into();
		artifact
			.to_path(temp.as_ref())
			.await
			.map_err(
				|source| tg::error!(!source, %path = temp.path().display(), "failed to write the artifact"),
			)
			.unwrap();
		let arg = tg::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: temp.path().join(path),
			updates: Vec::new(),
		};
		tg::checkin(&server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the artifact"))
			.unwrap();
		let referrer = tg::module::Data {
			kind,
			referent: tg::Referent {
				item: tg::module::data::Item::Path(temp.path().join(path).join(subpath)),
				path: None,
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await.unwrap();
		(assertions)(server, temp.path().to_owned(), module).await;
	})
	.await;
}

async fn test_object<F, Fut>(
	artifact: impl Into<temp::Artifact> + Send,
	kind: tg::module::Kind,
	path: &str,
	subpath: &str,
	import: tg::module::Import,
	assertions: F,
) where
	F: FnOnce(Server, tg::Artifact, tg::module::Data) -> Fut + Send + 'static,
	Fut: Future<Output = ()> + Send + 'static,
{
	test(async move |context| {
		let server = context.start_server().await;
		let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
		let temp = Temp::new();
		let artifact = artifact.into();
		artifact
			.to_path(temp.as_ref())
			.await
			.map_err(
				|source| tg::error!(!source, %path = temp.path().display(), "failed to write the artifact"),
			)
			.unwrap();
		let arg = tg::checkin::Arg {
			destructive: false,
			deterministic: false,
			ignore: true,
			locked: false,
			lockfile: true,
			path: temp.path().join(path),
			updates: Vec::new(),
		};
		let artifact = tg::checkin(&server, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the artifact"))
			.unwrap();
		let item = artifact
			.clone()
			.unwrap_directory()
			.get(&server, "tangram.ts")
			.await
			.unwrap()
			.id()
			.into();
		let referrer = tg::module::Data {
			kind,
			referent: tg::Referent {
				item: tg::module::data::Item::Object(item),
				path: Some(Path::new(path).join(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await.unwrap();
		(assertions)(server, artifact, module).await;
	})
	.await;
}
