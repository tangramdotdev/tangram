use std::path::Path;
use tangram_cli_test::{Server, assert_success};
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn directory() {
	let artifact = temp::directory! {
		"file" => temp::file!("hello!"),
		"link" => temp::symlink!("file"),
	}
	.into();
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn file() {
	let artifact = temp::file!("hello, world!").into();
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn tagged_dependency() {
	let artifact = temp::directory! {
		"tangram.ts" => temp::file!(r#"import * as foo from "foo/*"#),
	}
	.into();
	let foo = temp::directory! {
		"tangram.ts" => temp::file!(""),
	};
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![("foo/1.0.0".into(), foo.into())];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn symlink() {
	let artifact = temp::directory! {
		"file" => temp::file!("hello!"),
		"link" => temp::symlink!("file"),
	}
	.into();
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn artifact_symlink() {
	let artifact = temp::directory! {
		".tangram" => temp::directory! {
			"artifacts" => temp::directory! {
				"dir_01ds3dt46yzjdndgmtdv2ppm4c47tmr20s46ae9qs5qwvf1je3r9wg" => temp::directory!()
			}
		},
		"link" => temp::symlink!(".tangram/artifacts/dir_01ds3dt46yzjdndgmtdv2ppm4c47tmr20s46ae9qs5qwvf1je3r9wg"),
	}.into();
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn cyclic_artifact_symlink() {
	let artifact = temp::directory! {
		".tangram" => temp::directory! {
			"artifacts" => temp::directory! {
				"sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg" => temp::symlink!("sym_01tf70d3w3nm5tx0ghnhmcb6kcvms71967febephw12qmd9zkc1pvg"),
				"sym_01tf70d3w3nm5tx0ghnhmcb6kcvms71967febephw12qmd9zkc1pvg" => temp::symlink!("sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg")
			}
		},
		"link" => temp::symlink!(".tangram/artifacts/sym_01jxvmh7z5daw3yztgjbrr3hmjv9cp0jhg1mjatcqccvyez83ff2eg"),
	}.into();
	let path = Path::new("");
	let deterministic = false;
	let tags = vec![];
	test(artifact, path, deterministic, tags).await;
}

#[tokio::test]
async fn object_dependency() {
	let artifact = temp::directory! {
		"file" => temp::File {
			contents: "".into(),
			executable: false,
			xattrs: [
				("user.tangram.dependencies".into(), r#"["dir_01ds3dt46yzjdndgmtdv2ppm4c47tmr20s46ae9qs5qwvf1je3r9wg"]"#.into())
			].into(),
		}
	}.into();
	let path = Path::new("");
	let deterministic = true;
	let tags = vec![("empty".into(), temp::directory! {}.into())];
	test(artifact, path, deterministic, tags).await;
}

async fn test(
	artifact: temp::Artifact,
	path: &Path,
	deterministic: bool,
	tags: Vec<(String, temp::Artifact)>,
) {
	let server = Server::new(TG).await.unwrap();

	// Tag.
	let tags = tags.into_iter().collect::<Vec<_>>();
	for (tag, artifact) in &tags {
		let temp = Temp::new();
		artifact.to_path(&temp).await.unwrap();
		let output = server
			.tg()
			.arg("tag")
			.arg(tag)
			.arg(temp.path())
			.output()
			.await
			.unwrap();
		assert_success!(output);
	}

	// Write the artifact.
	let temp = Temp::new();
	artifact.to_path(&temp).await.unwrap();

	// Checkin.
	let mut command = server.tg();
	command.arg("checkin").arg(temp.path().join(path));
	if deterministic {
		command.arg("--deterministic");
	}
	let output = command.output().await.unwrap();
	assert_success!(output);
	let a = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	// Checkout.
	let temp = temp::Temp::new();
	let output = server
		.tg()
		.arg("checkout")
		.arg(&a)
		.arg(temp.path())
		.output()
		.await
		.unwrap();
	assert_success!(output);

	// Clean.
	for (tag, _) in &tags {
		server
			.tg()
			.arg("tag")
			.arg("delete")
			.arg(tag)
			.output()
			.await
			.unwrap();
	}
	server.tg().arg("clean").output().await.unwrap();

	// Check in.
	let mut command = server.tg();
	command.arg("checkin").arg(temp.path());
	if deterministic {
		command.arg("--deterministic");
	}
	let output = command.output().await.unwrap();
	assert_success!(output);
	let b = std::str::from_utf8(&output.stdout)
		.unwrap()
		.trim()
		.to_owned();

	assert_eq!(a, b);
}
