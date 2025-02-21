use crate::{Server, compiler::Compiler, test::test};
use futures::Future;
use pretty_assertions::assert_eq;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

#[tokio::test]
async fn path_dependency_path() {
	let kind = tg::module::Kind::Ts;
	let artifact = temp::directory! {
		"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
		"foo.tg.ts" => "",
	};
	let path = "";
	let subpath = "tangram.ts";
	let import = tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap();
	let assertions = |_, path, module| async move {
		let right = tg::Module {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::Item::Path(path),
				path: None,
				subpath: Some("foo.tg.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_path(kind, artifact, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn path_dependency_object() {
	let kind = tg::module::Kind::Ts;
	let artifact = temp::directory! {
		"tangram.ts" => r#"import * as foo from "./foo.tg.ts""#,
		"foo.tg.ts" => "",
	};
	let path = "";
	let subpath = "tangram.ts";
	let import = tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap();
	let assertions = |server, artifact: tg::Artifact, module| async move {
		let right = tg::Module {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::Item::Object(artifact.id(&server).await.unwrap().into()),
				path: None,
				subpath: Some("foo.tg.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_object(kind, artifact, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn package_path_dependency_path() {
	let kind = tg::module::Kind::Ts;
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar""#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	};
	let path = "foo";
	let subpath = "tangram.ts";
	let import = tg::Import::with_specifier_and_attributes("../bar", None).unwrap();
	let assertions = |_, path: PathBuf, module| async move {
		let path = tokio::fs::canonicalize(path.join("bar")).await.unwrap();
		let right = tg::Module {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::Item::Path(path),
				path: None,
				subpath: Some("tangram.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_path(kind, artifact, path, subpath, import, assertions).await;
}

#[tokio::test]
async fn package_path_dependency_object() {
	let kind = tg::module::Kind::Ts;
	let artifact = temp::directory! {
		"foo" => temp::directory! {
			"tangram.ts" => r#"import * as bar from "../bar""#,
		},
		"bar" => temp::directory! {
			"tangram.ts" => "",
		}
	};
	let path = "foo";
	let subpath = "tangram.ts";
	let import = tg::Import::with_specifier_and_attributes("../bar", None).unwrap();
	let assertions = |server, artifact: tg::Artifact, module| async move {
		let artifact = artifact
			.unwrap_directory()
			.get(&server, "bar")
			.await
			.unwrap();
		let object = artifact.id(&server).await.unwrap().into();
		let right = tg::Module {
			kind: tg::module::Kind::Ts,
			referent: tg::Referent {
				item: tg::module::Item::Object(object),
				path: None,
				subpath: Some("tangram.ts".into()),
				tag: None,
			},
		};
		assert_eq!(module, right);
	};
	test_object(kind, artifact, path, subpath, import, assertions).await;
}

async fn test_path<F, Fut>(
	kind: tg::module::Kind,
	artifact: impl Into<temp::Artifact> + Send,
	path: &str,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) where
	F: FnOnce(Server, PathBuf, tg::Module) -> Fut + Send + 'static,
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
		tg::Artifact::check_in(
			&server,
			tg::artifact::checkin::Arg {
				cache: false,
				destructive: false,
				deterministic: false,
				ignore: true,
				locked: false,
				lockfile: true,
				path: temp.path().to_owned(),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the artifact"))
		.unwrap();
		let referrer = tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Path(temp.path().join(path)),
				path: Some(path.into()),
				subpath: Some(PathBuf::from(subpath)),
				tag: None,
			},
		};
		let module = compiler.resolve_module(&referrer, &import).await.unwrap();
		(assertions)(server, temp.path().to_owned(), module).await;
	})
	.await;
}

async fn test_object<F, Fut>(
	kind: tg::module::Kind,
	artifact: impl Into<temp::Artifact> + Send,
	path: &str,
	subpath: &str,
	import: tg::Import,
	assertions: F,
) where
	F: FnOnce(Server, tg::Artifact, tg::Module) -> Fut + Send + 'static,
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
		let artifact = tg::Artifact::check_in(
			&server,
			tg::artifact::checkin::Arg {
				cache: false,
				destructive: false,
				deterministic: false,
				ignore: true,
				locked: false,
				lockfile: true,
				path: temp.path().to_owned(),
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to check in the artifact"))
		.unwrap();
		let item = artifact
			.clone()
			.unwrap_directory()
			.get(&server, path)
			.await
			.unwrap()
			.id(&server)
			.await
			.unwrap()
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
		let module = compiler.resolve_module(&referrer, &import).await.unwrap();
		(assertions)(server, artifact, module).await;
	})
	.await;
}
